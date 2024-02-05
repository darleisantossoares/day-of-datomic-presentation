(ns datomic.presentation
  (:require [datomic.api :as d]
            [dev.nu.morse :as morse]))

(def db-uri "datomic:dev://localhost:4334/day-of-datomic")
(def conn (d/connect db-uri))
(def db (d/db conn))


(clojure.pprint/pprint (d/db-stats db))


;; query all the stocks available in the database
#_(let [query-map {:query '[]
                 :args [db]
                 :io-context :day-of-datomic/get-all-stocks}
      {:keys [ret io-stats]} (d/query query-map)]
  (println "Query result" ret)
  (println "I/O Stats" io-stats))


;; query customers that owns stock X


;; query stocks owned by customer X without partitions


;; query stocks owned by customer X with partitions


;; query stocks owned by customer X without partitions but using index


;; query stocks owned by customer X with partitions using index


;; query stock Y owned by customer X without partitions and without index


;; query stock Y owned by customer X with partitions and without index


;; query stock Y owned by customer X without partitions with full index


;; query stock Y owned by custoemr X with partitions with full index


;; using d/datoms to query data, io-stats doesn't support


;; using index-pull to query data, io-stats doesn't support















