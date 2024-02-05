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


(def customer-portifolio-index-schema
  [{:db/ident :customer-portifolio-index/customer-id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/index true}
   {:db/ident :customer-portifolio-index/stock-code
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :customer-portifolio-index/total
    :db/valueType :db.type/bigint
    :db/cardinality :db.cardinality/one}
   {:db/ident :customer-portifolio-index/stock
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/ref}
   {:db/ident :customer-portifolio-index/customer-id+stock-code
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/tuple
    :db/tupleAttrs [:customer-portifolio-index/customer-id :customer-portifolio-index/stock-code]
    :db/unique :db.unique/identity}])

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



(def stock-schema
  [[{:db/ident :stock/code
     :db/valueType :db.type/keyword
     :db/cardinality :db.cardinality/one
     :db/unique :db.unique/identity
     :db/doc "Stock Code"}]
   [{:db/ident :stock/company
     :db/valueType :db.type/string
     :db/cardinality :db.cardinality/one}]])


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

(transact-schema conn customer-portifolio-index-schema)

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

#_(let [db (d/db (d/connect db-uri))
        query {:query '[:find (pull ?e [:customer-portifolio/customer-id])
                        :in $
                        :where [?e :customer-portifolio/stock-code :AAPL]]
               :args [db]
               :io-context :dod/coragem}
        {:keys [ret io-stats]} (d/query query)]
    (pprint ret)
    (pprint io-stats))

(def db (d/db (d/connect db-uri)))
#_(def query '[:find (pull ?e [*])
               :in $
               :where [?e :customer-portifolio-partitioned/customer-id #uuid "65c01be5-5877-42ea-ad99-7252961e3b30"]])

#_(def query-map {:query query
                  :args [db]
                  :io-context :dod/partitioned-customer-id})
#_(let [{:keys [ret io-stats]} (d/query query-map)]
    (pprint ret)
    (pprint io-stats))

#_(def query-with-index '[:find ?e
                          :in $ ?uuid ?stock-code
                          :where [?e :customer-portifolio/customer-id+stock-code [?uuid ?stock-code]]])

#_(def query-map {:query query-with-index
                  :args [db #uuid "65c01be5-5877-42ea-ad99-7252961e3b30" :NURE]
                  :io-context :dod/query-index})

#_(let [{:keys [ret io-stats]} (d/query query-map)]
    (pprint ret)
    (pprint io-stats))

#_(def query-with-index-incomplete '[:find ?e
                                     :in $ ?uuid
                                     :where [?e :customer-portifolio/customer-id+stock-code [?uuid nil]]])

#_(def query-map {:query query-with-index-incomplete
                  :args [db #uuid  "65c01b63-5453-4d7d-915a-0585bf5d9c22"]
                  :io-context :dbd/query-index-incomplete})

#_(let [{:keys [ret io-stats]} (d/query query-map)]
    (pprint ret)
    (pprint io-stats))

#_(pprint (d/db-stats db))




