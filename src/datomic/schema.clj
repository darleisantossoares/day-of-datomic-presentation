(ns datomic.schema
  (:require [datomic.api :as d]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]))

(def customer-portifolio-schema
  [{:db/ident :customer-portifolio/customer-id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one}
   {:db/ident :customer-portifolio/stock-code
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :customer-portifolio/total
    :db/valueType :db.type/bigint
    :db/cardinality :db.cardinality/one}
   {:db/ident :customer-portifolio/stock
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/ref}
   {:db/ident :customer-portifolio/customer-id+stock-code
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/tuple
    :db/tupleAttrs [:customer-portifolio/customer-id :customer-portifolio/stock-code]
    :db/unique :db.unique/identity}])

(def order-schema
  [[{:db/ident :order/customer-id
     :db/valueType :db.type/uuid
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :order/operation
     :db/valueType :db.type/keyword
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :order/timestamp
     :db/valueType :db.type/inst
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :order/stock-code
     :db/valueType :db.type/keyword
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :order/stock-ref
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :order/total
     :db/valueType :db.type/bigint
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :order/value
     :db/valueType :db.type/float
     :db/cardinality :db.cardinality/one}]])

(def stock-schema
  [[{:db/ident :stock/code
     :db/valueType :db.type/keyword
     :db/cardinality :db.cardinality/one
     :db/unique :db.unique/identity
     :db/doc "Stock Code"}]
   [{:db/ident :stock/company
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one}]])


(def customer-portifolio-schema-partitioned
  [{:db/ident :customer-portifolio-partitioned/customer-id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one}
   {:db/ident :customer-portifolio-partitioned/stock-code
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :customer-portifolio-partitioned/total
    :db/valueType :db.type/bigint
    :db/cardinality :db.cardinality/one}
   {:db/ident :customer-portifolio-partitioned/stock
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/ref}
   {:db/ident :customer-portifolio-partitioned/customer-id+stock-code
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/tuple
    :db/tupleAttrs [:customer-portifolio-partitioned/customer-id :customer-portifolio-partitioned/stock-code]
    :db/unique :db.unique/identity}])

;;;;; Transact Schema
(def db-uri "datomic:dev://localhost:4334/day-of-datomic")

(defn create-database [uri]
  (d/create-database uri))

;(create-database db-uri)

(def conn (d/connect db-uri))

(defn transact-schema [connection schema]
  (if (> 1 (count schema))
    (doseq [s stock-schema]
      @(d/transact connection s)))
  @(d/transact connection schema))

; Transact stock schema
;(transact-schema conn stock-schema)

;(transact-schema conn customer-portifolio-schema-partitioned)

;; read csv
#_(defn process-csv-row [row]
    (println row))

(defn read-csv-file [file-path]
  (with-open [reader (io/reader file-path)]
    (doall (csv/read-csv reader :skip-lines 1))))

(def csv-path  "/Users/darlei.soares/dev/nu/day-of-datomic-presentation/src/files/stocks.csv")
(defn transform-csv-row-to-datomic [row]
  [{:db/id (d/tempid :db.part/user)
    :stock/code (keyword (first row))
    :stock/company (second row)}])

(defn insert-csv-data [file-path conn]
  (let [csv-data (read-csv-file file-path)
        tx-data (map transform-csv-row-to-datomic csv-data)]
    (doseq [r tx-data]
      @(d/transact conn [r]))))

;(insert-csv-data csv-path conn)

(pprint (d/db-stats (d/db conn)))

(let [db (d/db (d/connect db-uri))
      query-map {:query '[:find ?e
                          :in $
                          :where
                          [?e :stock/code]]
                 :args [db]
                 :io-context :dod/presentation}
      {:keys [_ io-stats]} (d/query query-map)]
  ;(println "Query Result:" ret)
  (println "I/O Stats:" io-stats))

(let [db (d/db (d/connect db-uri))
      query-map {:query '[:find (pull ?e [:db/id :stock/code :stock/company])
                          :in $
                          :where
                          [?e :stock/code :AMZN]]
                 :args [db]
                 :io-context :dod/presentation}
      {:keys [ret io-stats]} (d/query query-map)]
  (println "Query Result:" ret)
  (println "I/O Stats:" io-stats))

(let [db (d/db (d/connect db-uri))
      query-map {:query '[:find (pull ?e [:db/id :stock/code :stock/company])
                          :in $
                          :where
                          [?e :stock/code :NU]]
                 :args [db]
                 :io-context :dod/presentation}
      {:keys [ret io-stats]} (d/query query-map)]
  (println "Query Result:" ret)
  (println "I/O Stats:" io-stats))

; d/datoms
(let [conn (d/connect db-uri)
      db (d/db conn)
      datoms-lazy-seq (->> (d/datoms db :avet :stock/code))]
  (doseq [datom datoms-lazy-seq]
    (println datom)))



