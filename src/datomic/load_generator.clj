(ns datomic.load-generator
  (:require
   [datomic.api :as d]
   [datomic.common :refer [retry-fn with-nano-time log-retry await-derefs]]
   [clojure.data.generators :as dgen]
   [datomic.schema :as is])
  (:import
   (java.util UUID)))


; Transact the schema
#_(defn transact-schema [uri]
 (doseq [tx is/datomic-schema]
   @(d/transact-async (d/connect uri) [tx])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Activity Simulator
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-random-uuid
  []
  (UUID/randomUUID))

(defn get-squiid
  []
  (d/squuid))

(defmulti activity->tx-data (fn [db activity] (:activity/operation activity)))


(defn pix-transfer-out-request-base
  []
  {:pix-transfer-out-request/created-at #inst "2023-09-06T14:43:29.823"
   :pix-transfer-out-request/requested-at #inst "2023-09-06T14:43:29.785"
   :pix-transfer-out-request/post-date #inst "2023-09-06"
   :pix-transfer-out-request/id (get-squiid)
   :pix-transfer-out-request/beneficiary {
                                          :beneficiary/id (get-squiid)
                                          :beneficiary/bank-account
                                          {:beneficiary-bank-account/account-branch 1
                                           :beneficiary-bank-account/institution-number "0260"
                                           :beneficiary-bank-account/institution-ispb "18236120"
                                           :beneficiary-bank-account/institution-name "NU PAGAMENTOS - IP"
                                           :beneficiary-bank-account/name "Mango's Joe"
                                           :beneficiary-bank-account/account-check-digit 0
                                           :beneficiary-bank-account/id (get-squiid)
                                           :beneficiary-bank-account/tax-type :beneficiary-bank-account.tax-type/person
                                           :beneficiary-bank-account/institution-short-name "NU PAGAMENTOS - IP"
                                           :beneficiary-bank-account/account-number 1122334455
                                           :beneficiary-bank-account/tax-id "519.013.920-40"
                                           :beneficiary-bank-account/account-type :beneficiary-bank-account.account-type/checking-account}
                                          :beneficiary/contact
                                          {:beneficiary-contact/beneficiary-contact-id (get-squiid)
                                           :beneficiary-contact/id (get-squiid)}
                                          :beneficiary/pix-alias
                                          {:beneficiary-pix-alias/id (get-squiid)
                                           :beneficiary-pix-alias/type :beneficiary-pix-alias.type/cpf
                                           :beneficiary-pix-alias/value "519.013.920-40"}}
   :pix-transfer-out-request/request-hash (get-random-uuid)
   :pix-transfer-out-request/beneficiary-alias-id (get-random-uuid)
   :pix-transfer-out-request/beneficiary-bank-account-id (get-squiid)
   ;; :pix-transfer-out-request/e2e-id "E18236120202309061443s15b59ca497" ;; TO:DO needs to be unique
   :pix-transfer-out-request/source {:http-request/id (get-random-uuid)} ;; weight the distribution
   :pix-transfer-out-request/client-certificate {:client-certificate/serial-number "133989849514101249469764420820740144920"}
   :pix-transfer-out-request/initiation-type :pix-transfer-out-request.initiation-type/pix-alias
   :pix-transfer-out-request/amount 19.96M
   :pix-transfer-out-request/message "Rent money"})


(defmethod activity->tx-data :activity.operation/savings-account
  [db activity]
  [activity])

(defn gen-savings-account
  []
  {:savings-account/id (get-squiid) :savings-account/customer-id (get-random-uuid)})

 ;TO:DO Sample gen-savings-account from the pool

(defmethod activity->tx-data :activity.operation/pix-request ;TO:DO this guy is not complete
  [_ {savings-account :activity.operation.pix-request/savings-account}]
  (let [] ; I need to get the savings-account from the pool
    [(-> (pix-transfer-out-request-base)
         (merge
          {:pix-transfer-out-request/savings-account savings-account}))]))

; I'll need to sample beneficiaries (apply the same pattern)


(defn pipeline
  [{:keys [conn in-flight tps fell-behind-fn recording-fn op-count spin-sleep? done?]
    :or   {done?       (atom false)
           spin-sleep? false
           op-count    Long/MAX_VALUE
           fell-behind-fn #(println
                            (let [ns (- (:now %) (:deadline %))
                                  us (long (/ ns 1000))
                                  ms (long (/ us 1000))]
                              (format "Fell Behind by %s ns, %s us %s ms" ns us ms)))
           recording-fn (fn [& _] (do (print ".") (flush)))}} txes]
  (let [q (java.util.concurrent.LinkedBlockingQueue. (int in-flight))
        target-ops-per-ms        (double (/ tps 1000))
        target-ops-tick-ns       (long (/ 1000000 target-ops-per-ms))
        target-ops-pos?          (> target-ops-per-ms 0)
        sleep-until              (fn sleep-until [^long deadline]
                                   (let [now (System/nanoTime)]
                                     (when-not (< now deadline)
                                       (fell-behind-fn {:now now :deadline deadline})))
                                   (while (< (System/nanoTime) deadline)
                                     (when-not spin-sleep?
                                       (java.util.concurrent.locks.LockSupport/parkNanos (- deadline (System/nanoTime))))))
        throttle-nanos           (fn throttle-nanos [^long start-time-nanos ^long ops-done]
                                   (let [deadline (+ start-time-nanos (* ops-done target-ops-tick-ns))]
                                     (when target-ops-pos? (sleep-until deadline))
                                     deadline))]
    {:result-future (future
                      (loop [{:keys [tx fut] :as current} (.take q)]
                        (when-not (= current :done)
                          (if fut
                            (if-let [res (try
                                           (deref fut 10000 nil)
                                           (catch Throwable t nil))]
                              (let [completed (System/nanoTime)]
                                (recording-fn (assoc current
                                                     :res res
                                                     :completed completed)))
                              (println "failure after submission. insert retry logic here for" tx))
                            (println "failure prior to submission. insert retry logic here for" tx))
                          (recur (.take q))))
                      :done)
     :submit-future (future
                      (let [start-time-nanos (System/nanoTime)]
                        (loop [txes     txes
                               ops-done (long 0)]
                          (when (and
                                 (seq txes)
                                 (not @done?)
                                 (and op-count (< ops-done op-count)))
                            ;; Do Work
                            (.put q {:started   (System/nanoTime)
                                     :scheduled (+ start-time-nanos
                                                   (* ops-done target-ops-tick-ns))
                                     :tx        (first txes)
                                     :fut       (try
                                                  (d/transact-async conn (first txes))
                                                  (catch Throwable t nil))})
                            (throttle-nanos start-time-nanos (inc ops-done))
                            (recur (next txes) (inc ops-done))))
                        (.put q :done))
                      :done)}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Activity Generators
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn rand-nth
  "Replacement of core/rand-nth (and generators/rand-nth) that allows
  control of the randomization basis (through binding *rnd*) and the
  distribution (via dist).

  dist defaults to (partial uniform 0 (count coll))."
  ([coll]
   (rand-nth (partial dgen/uniform 0 (count coll)) coll))
  ([dist coll]
   (nth coll (dist))))


(defn gen-pix-request-event
  [db dist savings-account-id-pool & _]
  {:activity/operation :activity.operation/pix-request
   :activity.operation.pix-request/savings-account (rand-nth dist savings-account-id-pool)})

(defn load-txes
  [conn batch conc txes]
  (->> txes
       (partition-all batch)
       (pipeline {:conn conn :in-flight conc :tps conc})
       (vals)
       (await-derefs)))

(defn run
  [{:keys [uri seed
           accounts
           pixes balance-conc balance-batch tps
           in-flight]
    :or {seed 42 balance-batch 1000 balance-conc 10 tps 2 in-flight 100}}]
  (d/create-database uri)
  ;(transact-schema uri)
  (binding [dgen/*rnd* (java.util.Random. seed)]
    (let [conn (d/connect uri)
          _ (->> #(gen-savings-account)
                 (repeatedly accounts)
                 (seque 100)
                 (load-txes conn balance-batch balance-conc))
          savings-account-id-pool (into [] (map :e)
                                        (seq (d/datoms (d/db conn) :avet :savings-account/id)))
          dist (partial dgen/uniform 0 (count savings-account-id-pool))]
      (->> #(gen-pix-request-event (d/db conn) dist savings-account-id-pool)
           (repeatedly (long pixes))
           (map #(activity->tx-data (d/db conn) %))
           (pipeline {:conn conn
                      :in-flight in-flight
                      :tps tps})
           (vals)
           (await-derefs)))))

(comment
  (run {:uri "datomic:dev://localhost:4334/warriv-2"
        :pixes 1000
        :accounts 10
        :tps 10}))
