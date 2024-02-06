(ns datomic.presentation
  (:require [datomic.api :as d]
            [clojure.pprint :refer [pprint]]
            [datomic.afinity :as aff]))

(def db-uri "datomic:dev://localhost:4334/day-of-datomic")
(def conn (d/connect db-uri))
(def db (d/db conn))

(clojure.pprint/pprint (d/db-stats db))


(let [query-map {:query '[:find ?e
                          :in $
                          :where [?e :stock/code :NU]]
                 :args [db]
                 :io-context :day-of-datomic/stocks}
      {:keys [ret  io-stats]} (d/query query-map)]
  (print ret)
  (println "=================================")
  (pprint  io-stats)
  (println "=================================")
  )



{:io-context :day-of-datomic/stocks,
 :api :query,
 :api-ms 91.04,
 :reads {:aevt 8, :eavt 15, :ocache 23}}



{:io-context :day-of-datomic/stocks,
 :api :query,
 :api-ms 110.14,
 :reads
 {:aevt 22400, :dev 5, :aevt-load 9, :ocache 22400, :dev-ms 13.29}}


{:io-context :day-of-datomic/stocks,
 :api :query,
 :api-ms 13.95,
 :reads
 {:avet 2,
  :dev 2,
  
  
  :ocache 2,
  :dev-ms 4.39,
  :avet-load 2}}



;; query customers
(defn customers-sample
  [db sample-size]
  (let [query-customers {:query '[:find (pull ?e [:customer-portifolio/customer-id])
                                  :in $
                                  :where [?e :customer-portifolio/customer-id ?]]
                         :args [db]}
        query-partitioned {:query '[:find (pull ?e [:customer-portifolio-partitioned/customer-id])
                                    :in $
                                    :where [?e :customer-portifolio-partitioned/customer-id ?]]
                           :args [db]}
        query-indexed {:query '[:find (pull ?e [:customer-portifolio-index/customer-id])
                                :in $
                                :where [?e :customer-portifolio-index/customer-id ?]]
                       :args [db]}]
    {:customers (take sample-size (shuffle (d/query query-customers)))
     :customers-partitioned (take sample-size (shuffle (d/query query-partitioned)))
     :customer-indexed (take sample-size (shuffle (d/query query-indexed)))}))


(def customers-sample-testing (customers-sample db 2000))

(def c-1 (:customer-portifolio/customer-id (first (rand-nth (:customers customers-sample-testing)))))


(println c-1)



(let [query-map {:query '[:find (pull ?e [*])
                          :in $ ?customer-id 
                          :where [?e :customer-portifolio/customer-id ?customer-id]]
                 :args [db c-1]
                 :io-context :day-of-datomic/customer-stock}
      {:keys [ret  io-stats]} (d/query query-map)]
  (println (count ret))
  (pprint io-stats))


(def c-2 (:customer-portifolio-index/customer-id (first (rand-nth (:customer-indexed customers-sample-testing)))))


(println c-2)



(def c-3 (:customer-portifolio-partitioned/customer-id (first (rand-nth (:customers-partitioned customers-sample-testing)))))
(println c-3)

(let [query-map {:query '[:find (pull ?e [:customer-portifolio-index/customer-id
                                          :customer-portifolio-index/stock-code
                                          :customer-portifolio-index/stock])
                          :in $ ?customer-id 
                          :where [?e :customer-portifolio-index/customer-id ?customer-id]]
                 :args [db c-3]
                 :io-context :day-of-datomic/customer-stock-partitioned}
      {:keys [ret  io-stats]} (d/query query-map)]
  (doseq [x ret]
    (print x))
  (pprint io-stats))

{:io-context :day-of-datomic/customer-stock-indexed,
 :api :query,
 :api-ms 176.68,
 :reads
 {:avet 1,
  :dev 48,
  :eavt 18368,
  :eavt-load 47,
  :ocache 18369,
  :dev-ms 61.53,
  :avet-load 1}}


{:io-context :day-of-datomic/customer-stock-indexed,
 :api :query,
 :api-ms 91.21,
 :reads
 {:avet 1,
  :aevt 692,
  :dev 23,
  :aevt-load 22,
  :ocache 693,
  :dev-ms 27.71,
  :avet-load 1}}


{:io-context :day-of-datomic/customer-stock-partitioned,
 :api :query,
 :api-ms 8.81,
 :reads {:avet 1, :dev 1, :ocache 1, :dev-ms 3.65, :avet-load 1}}


(let [query-map {:query '[:find (pull ?e [*])
                          :in $ ?customer-id
                          :where [?e :customer-portifolio/customer-id+stock-code [?customer-id :NU]]]
                 :args [db c-1]
                 :io-context :day-of-datomic/customer-stock}
      {:keys [ret  io-stats]} (d/query query-map)]
  (println ret)
  (pprint io-stats))


{:io-context :day-of-datomic/customer-stock,
 :api :query,
 :api-ms 107.16,
 :reads {:aevt 137, :eavt 20253, :ocache 20390}}


{:io-context :day-of-datomic/customer-stock,
 :api :query,
 :api-ms 10.99,
 :reads {:avet 1, :dev 1, :ocache 1, :dev-ms 4.57, :avet-load 1}}























;; query customers that owns stock X
#_(let [query-map {:query '[:find (pull ?e [*])
                            :in $
                            :where customer-portifolio/stock-code ?stock]
                   :args [db :NU]
                   :io-context :day-of-datomic/customer-stock}
        {:keys [ret  io-stats]} (d/query query-map)]
    (println ret)
    (pprint io-stats))

#_(let [query-map {:query '[:find (pull ?e [*])
                            :in $
                            :where customer-portifolio-partitioned/stock-code ?stock]
                   :args [db :NU]
                   :io-context :day-of-datomic/customer-stock}
        {:keys [ret  io-stats]} (d/query query-map)]
    (println ret)
    (pprint io-stats))

#_(let [query-map {:query '[:find (pull ?e [*])
                            :in $
                            :where [?e :customer-portifolio-index/stock-code ?stock]]
                   :args [db :NU]
                   :io-context :day-of-datomic/customer-stock}
        {:keys [ret  io-stats]} (d/query query-map)]
    (println ret)
    (pprint io-stats))

;; query stocks owned by customer X without partitions

;; query stocks owned by customer X with partitions

;; query stocks owned by customer X without partitions but using index

;; query stocks owned by customer X with partitions using index

;; query stock Y owned by customer X without partitions and without index

;; query stock Y owned by customer X with partitions and without index

;; query stock Y owned by customer X without partitions with full index

;; query stock Y owned by customer X with partitions with full index

;; using d/datoms to query data, io-stats doesn't support
#_(->> (d/index-pull db
                   {:index :avet
                    :selector '[:customer-portifolio-index/customer-id
                                :customer-portifolio-index/stock-code]
                    :start :customer-portifolio-index/customer-id})
     (take-while (fn [{:keys [stock-code]}] (.startsWith (str stock-code) "A")))
     )

















