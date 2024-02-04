(ns datomic.schema
  (:require [datomic.api :as d]))

(def customer-portifolio-schema
  [[{:db/ident :customer-portifolio/customer-id
     :db/valueType :db.type/uuid
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :customer-portifolio/stock-code
     :db/valueType :db.type/keyword
     :db.cardinality :db.cardinality/one}]
   [{:db/ident :customer-portifolio/amount
     :db/valueType :db.type/bigint
     :db/cardinality :db.cardinality/one}]])

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
     :db/cardinality :db.cardinality/on}]
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def customer-portifolio-schema-partitioned
  [[{:db/ident :customer-portifolio-partitioned/customer-id
     :db/valueType :db.type/uuid
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :customer-portifolio-partitioned/stock-code
     :db/valueType :db.type/keyword
     :db.cardinality :db.cardinality-partitioned/one}]
   [{:db/ident :customer-portifolio/amount
     :db/valueType :db.type/bigint
     :db/cardinality :db.cardinality/one}]])

(def order-schema-partitioned
  [[{:db/ident :order-partitioned/customer-id
     :db/valueType :db.type/uuid
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :order-partitioned/operation
     :db/valueType :db.type/keyword
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :order-partitioned/timestamp
     :db/valueType :db.type/inst
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :order-partitioned/stock-code
     :db/valueType :db.type/keyword
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :order-partitioned/stock-ref
     :db/valueType :db.type/ref
     :db/cardinality :db.cardinality/on}]
   [{:db/ident :order-partitioned/total
     :db/valueType :db.type/bigint
     :db/cardinality :db.cardinality/one}]
   [{:db/ident :order-partitioned/value
     :db/valueType :db.type/float
     :db/cardinality :db.cardinality/one}]])


;;;;; Transact Schema
(def db-uri "datomic:dev://localhost:4334/day-of-datomic")

(defn create-database [uri]
  (d/create-database uri))

;(create-database db-uri)

(def conn (d/connect db-uri))

(defn transact-schema [connection schema]
  @(d/transact connection schema))

; Transaciona o Stock Schema
;(transact-schema conn stock-schema)

