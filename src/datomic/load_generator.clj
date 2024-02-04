(ns datomic.load-generator
  (:require
   [datomic.api :as d]
   [datomic.common :refer [retry-fn with-nano-time log-retry await-derefs]]
   [clojure.data.generators :as dgen]
   [datomic.schema :as is])
  (:import
   (java.util UUID)))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Activity Simulator
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-random-uuid
  []
  (UUID/randomUUID))

(defn get-squiid
  []
  (d/squuid))








#_(defn pipeline
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
#_(defn rand-nth
  "Replacement of core/rand-nth (and generators/rand-nth) that allows
  control of the randomization basis (through binding *rnd*) and the
  distribution (via dist).

  dist defaults to (partial uniform 0 (count coll))."
  ([coll]
   (rand-nth (partial dgen/uniform 0 (count coll)) coll))
  ([dist coll]
   (nth coll (dist))))



#_(defn load-txes
  [conn batch conc txes]
  (->> txes
       (partition-all batch)
       (pipeline {:conn conn :in-flight conc :tps conc})
       (vals)
       (await-derefs)))

#_(defn run
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

#_(comment
  (run {:uri "datomic:dev://localhost:4334/day-of-datomic"
        :stocks 1000
        :accounts 10
        :tps 10}))
