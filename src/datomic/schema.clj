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
      @(d/transact connection s))
    @(d/transact connection schema)))

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

;(pprint (d/db-stats (d/db conn)))

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

#_(let [db (d/db (d/connect db-uri))
      query-map {:query '[:find (pull ?e [:db/id :stock/code :stock/company])
                          :in $
                          :where
                          [?e :stock/code :AMZN]]
                 :args [db]
                 :io-context :dod/presentation}
      {:keys [ret io-stats]} (d/query query-map)]
  (println "Query Result:" ret)
  (println "I/O Stats:" io-stats))

#_(let [db (d/db (d/connect db-uri))
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
#_(let [conn (d/connect db-uri)
      db (d/db conn)
      datoms-lazy-seq (->> (d/datoms db :avet :stock/code))]
  (doseq [datom datoms-lazy-seq]
    (println datom)))



(let [db (d/db (d/connect db-uri))
      query {:query '[:find (pull ?e [:customer-portifolio/customer-id])
                      :in $
                      :where [?e :customer-portifolio/stock-code :AAPL]]
             :args [db]
             :io-context :dod/coragem}
      {:keys [ret io-stats]} (d/query query)]
  (pprint ret)
  (pprint io-stats))


 ;[#:customer-portifolio-partitioned{:customer-id #uuid "65c01b63-998e-42b4-aa73-b417c164bae2"}]
 ;[#:customer-portifolio-partitioned{:customer-id #uuid "65c01b63-1169-4064-beae-a23e0674da76"}]
 ;[#:customer-portifolio-partitioned{:customer-id #uuid "65c01b63-b9af-402d-baaa-4ee99ce4e8b8"}]
 ;[#:customer-portifolio-partitioned{:customer-id #uuid "65c01be5-7c68-429f-8fe4-2339ca4d4f91"}]
 ;[#:customer-portifolio-partitioned{:customer-id #uuid "65c01b63-5453-4d7d-915a-0585bf5d9c22"}]
 ;[#:customer-portifolio-partitioned{:customer-id #uuid "65c01b63-1c8a-48e9-a267-98ed3d9e397b"}]
 ;[#:customer-portifolio-partitioned{:customer-id #uuid "65c01be5-5877-42ea-ad99-7252961e3b30"}]


(def db (d/db (d/connect db-uri)))
(def query '[:find (pull ?e [*])
             :in $
             :where [?e :customer-portifolio-partitioned/customer-id #uuid "65c01be5-5877-42ea-ad99-7252961e3b30"]])

(def query-map {:query query
                :args [db] 
                :io-context :dod/partitioned-customer-id})

(let [{:keys [ret io-stats]} (d/query query-map)]
  (pprint ret)
  (pprint io-stats))


{:io-context :dod/without-partitions-customer-id,
 :api :query,
 :api-ms 465.78,
 :reads
 {:aevt 126,
  :dev 198,
  :eavt 148,
  :aevt-load 96,
  :eavt-load 146,
  :ocache 274,
  :dev-m {:aevt 126,
  :dev 198,
  :eavt 148,
  :aevt-load 96,
  :eavt-load 146,
  :ocache 274,
  :dev-ms 211.88}}



{:io-context :dod/partitioned-customer-id,
 :api :query,
 :api-ms 78.04,
 :reads
 {:aevt 89,
  :dev 20,
  :eavt 10,
  :aevt-load 27,
  :eavt-load 6,
  :ocache 99,
  :dev-ms 25.31}}


(def query-with-index '[:find ?e
                        :in $ ?uuid ?stock-code
                        :where [?e :customer-portifolio/customer-id+stock-code [?uuid ?stock-code]]])

(def query-map {:query query-with-index
                :args [db #uuid "65c01be5-5877-42ea-ad99-7252961e3b30" :NURE]
                :io-context :dod/query-index})
 

 (let [{:keys [ret io-stats]} (d/query query-map)]
   (pprint ret)
   (pprint io-stats))
 

(def query-with-index-incomplete '[:find ?e
                                   :in $ ?uuid
                                   :where [?e :customer-portifolio/customer-id+stock-code [?uuid nil]]])

 (def query-map {:query query-with-index-incomplete
                 :args [db #uuid  "65c01b63-5453-4d7d-915a-0585bf5d9c22"]
                 :io-context :dbd/query-index-incomplete})
 
 (let [{:keys [ret io-stats]} (d/query query-map)]
   (pprint ret)
   (pprint io-stats))
 
