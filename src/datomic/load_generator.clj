(ns datomic.load-generator
  (:require
   [datomic.api :as d]
   [datomic.common :refer [await-derefs]]
   [datomic.afinity :as aff]))

(defn generate-customer-portifolio
  "Returns a map of a customer + stocks"
  [customer-id stock-code total stock]
  {:customer-portifolio/customer-id customer-id
   :customer-portifolio/stock stock
   :customer-portifolio/total total
   :customer-portifolio/stock-code stock-code})

(defn generate-customer-portifolio-partitioned
  "Returns a map of a customer + stocks but partitioned"
  [customer-id stock-code partition-id total stock]
  {:customer-portifolio-partitioned/customer-id customer-id
   :customer-portifolio-partitioned/stock stock
   :customer-portifolio-partitioned/stock-code stock-code
   :customer-portifolio-partitioned/total total
   :db/id (d/tempid (d/implicit-part partition-id))})

(defn generate-customer-portifolio-indexed
  "Returns a map of a customer + stocks but partitioned"
  [customer-id stock-code total stock]
  {:customer-portifolio-index/customer-id customer-id
   :customer-portifolio-index/stock stock
   :customer-portifolio-index/stock-code stock-code
   :customer-portifolio-index/total total})

(defn create-all-customers
  "Creates n squuids and returns it"
  [n-customers]
  (take n-customers (repeatedly d/squuid)))

(def all-customers (create-all-customers 100))

(defn get-all-stocks
  "Return all stocks available in the database"
  [db]
  (let [query-map {:query '[:find (pull ?e [:stock/code :db/id])
                            :in $
                            :where [?e :stock/code ?]]
                   :args [db]}
        ret (d/query query-map)]
    ret))

(def db-uri "datomic:dev://localhost:4334/day-of-datomic")
(def conn (d/connect "datomic:dev://localhost:4334/day-of-datomic"))
(def all-stocks (get-all-stocks (d/db (d/connect db-uri))))

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
                                                  (d/transact-async conn [(first txes)])
                                        ;(d/transact-async conn [(:not-partitioned (first txes))])
                                        ;(d/transact-async conn [(:partitioned (first txes))])
                                                  (catch Throwable t (println t)))})
                            (throttle-nanos start-time-nanos (inc ops-done))
                            (recur (next txes) (inc ops-done))))
                        (.put q :done))
                      :done)}))

(defn generate-txs-portifolio
  []
  (let [c-id (rand-nth all-customers)
        one-stock (first (rand-nth all-stocks))
        stock-code (:stock/code one-stock)
        partition-key (aff/hash-uuid c-id)
        total-stocks (bigint (+ 1 (rand-int 2000)))]
    {:partitioned (generate-customer-portifolio-partitioned c-id stock-code partition-key total-stocks one-stock)
     :not-partitioned (generate-customer-portifolio c-id stock-code total-stocks one-stock)}))

(defn generate-txs-indexed
  []
  (let [c-id (rand-nth all-customers)
        one-stock (first (rand-nth all-stocks))
        stock-code (:stock/code one-stock)
        total-stocks (bigint (+ 1 (rand-int 2000)))]
    (generate-customer-portifolio-indexed c-id stock-code total-stocks one-stock)))

(run {:uri db-uri :stocks 300000 :tps 100 :in-flight 50})

(defn run
  [{:keys [uri stocks tps in-flight]}]
  (let [conn (d/connect uri)]
    (->> #(generate-txs-indexed)
         (repeatedly stocks)
         (pipeline {:conn conn
                    :in-flight in-flight
                    :tps tps})
         (vals)
         (await-derefs))))

#_(let [db (d/db (d/connect db-uri))
        query-map {:query '[:find (pull ?e [*])
                            :in $
                            :where [?e :customer-portifolio-partitioned/customer-id ?]]
                   :args [db]
                   :io-context :dod/presentation}
        {:keys [ret io-stats]} (d/query query-map)]
    (println "Query Result:" ret))

#_(let [db (d/db (d/connect db-uri))
        query-map {:query '[:find (pull ?e [*])
                            :in $
                            :where [?e :customer-portifolio/customer-id ?]]
                   :args [db]
                   :io-context :dod/presentation}
        {:keys [ret _]} (d/query query-map)]
    (println "Query Result:" ret))

(clojure.pprint/pprint (d/db-stats (d/db conn)))

;(let [db (d/db (d/connect db-uri))])

;(run {:uri db-uri :stocks 300000 :tps 100 :in-flight 50})

(run {:uri db-uri :stocks 100000 :tps 50 :in-flight 20})



(let [db (d/db (d/connect db-uri))
        query-map {:query '[:find (pull ?e [*])
                            :in $
                            :where [?e :customer-portifolio-index/customer-id ?]]
                   :args [db]
                   :io-context :dod/presentation}
        {:keys [ret io-stats]} (d/query query-map)]
    (println "Query Result:" ret))


