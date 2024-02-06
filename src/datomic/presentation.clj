(ns datomic.presentation
  (:require [datomic.api :as d]
            [clojure.pprint :refer [pprint]]))

(def db-uri "datomic:dev://localhost:4334/day-of-datomic")
(def conn (d/connect db-uri))
(def db (d/db conn))

(clojure.pprint/pprint (d/db-stats db))


(let [query-map {:query '[:find (pull ?e [*])
                          :in $
                          :where [?e :stock/code ?]]
                 :args [db]
                 :io-context :day-of-datomic/stocks
                 :query-stats true}
      {:keys [ret  io-stats query-stats]} (d/query query-map)]
  ;(print ret)
  (println "=================================")
  (pprint  io-stats)
  (println "=================================")
  (pprint query-stats))




;; query all the stocks available in the database
(let [query-map {:query '[:find (pull ?e [*])
                          :in $
                          :where [?e :stock/code ?]]
                 :args [db]
                 :io-context :day-of-datomic/stocks}
      {:keys [ret  io-stats]} (d/query query-map)]
  (print ret)
  (println "=================================")
  (pprint  io-stats))

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

(println (:customer-portifolio/customer-id (first (rand-nth (:customers customers-sample-testing)))))


(doseq [datom (d/datoms db :avet :stocks/code)]
  (println datom))































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

















