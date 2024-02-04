(ns datomic.load-generator
  (:require
   [datomic.api :as d]
   [datomic.schema :as dschema]
   [datomic.afinity :as aff])
  (:import
   (java.util UUID)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Activity Simulator
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn generate-customer-portifolio
  "Returns a map of a customer + stocks"
  [customer-id stock stock-code]
  {:customer-portifolio/customer-id customer-id
   :customer-portifolio/stock stock
   :customer-portifolio/stock-code stock-code})

(defn generate-customer-portifolio-partitioned
  "Returns a map of a customer + stocks but partitioned"
  [customer-id stock stock-code partition-id]
  {:customer-portifolio/customer-id customer-id
   :customer-portifolio/stock stock
   :customer-portifolio/stock-code stock-code
   :db/id (d/tempid (d/implicit-part partition-id))})

(defn create-all-customers
  "Creates n squuids and returns it"
  [n-customers]
  (take n-customers (repeatedly d/squuid)))

(def all-customers (create-all-customers 100))

(println all-customers)

#_(let [db (d/db (d/connect db-uri))
        query-map {:query '[:find ?e
                            :in $
                            :where
                            [?e :stock/code]]
                   :args [db]
                   :io-context :dod/presentation}
        {:keys [_ io-stats]} (d/query query-map)]
  ;(println "Query Result:" ret)
    (println "I/O Stats:" io-stats))

(defn get-all-stocks
  "Return all stocks availables to purchase
   in the database"
  [db]
  (let [query-map {:query '[:find (pull ?e [:stock/code :db/id])
                            :in $
                            :where [?e :stock/code ?]]
                   :args [db]}
        ret (d/query query-map)]
    ret))

(def conn (d/connect "datomic:dev://localhost:4334/day-of-datomic"))

(def all-stocks (get-all-stocks (d/db (d/connect "datomic:dev://localhost:4334/day-of-datomic"))))

;(println (:stock/code (ffirst all-stocks)))

(def one-stock (first (rand-nth all-stocks)))

(println (generate-customer-portifolio (rand-nth all-customers) (:stock/code one-stock) (:db/id one-stock)))

(defn transact-one-example
  [txes
   db]
  (d/transact db txes))

(do
  (println "---------------------------------------------------")
  (doseq [_ (range 2)]
    (let [c-id (rand-nth all-customers)
          one-stock (first (rand-nth all-stocks))
          stock-code (:stock/code one-stock)
          partition-key (aff/hash-uuid (rand-nth all-customers))]
      (clojure.pprint/pprint (generate-customer-portifolio c-id (:db/id one-stock) stock-code))
      (clojure.pprint/pprint (generate-customer-portifolio-partitioned c-id (:db/id one-stock) stock-code partition-key))
      (aff/hash-uuid (rand-nth all-customers))
      (println "############################################")))
  (println "---------------------------------------------------"))





