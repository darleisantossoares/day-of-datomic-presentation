(ns datomic.affinity-impl
  (:require [datomic.api :as d]))

(def ^:const MAX_IMPLICIT_PARTITIONS (bit-shift-left 1 19))

(defn partition-decisions
  "Invokes applicable partition fns on emap (an entity map within tx-data).
   Return a map of affinity assignments for entities.
   Does not filter assignments by validity."
  [db emap attr->partfn]
  (reduce-kv
   (fn [r k v]
     (if-let [f (get attr->partfn k)]
       (try
         (let [decision (f db emap k v)]
           (cond-> r (map? decision) (conj decision)))
         (catch Exception e
           (println :part-fn-threw k :exception e)
           r))
       r))
   {}
   emap))

(defn tempid? [x]
  (or (instance? datomic.db.DbId x)
      (string? x)))

(defn valid-directives
  "Given a map returned from user partition fns, drops invalid requests,
   returning a map of valid Datomic partition requests (:db/force-partition
   or :db/match-partition), or nil when no valid directives"
  [decisions]
  (loop [decisions (seq decisions)
         force nil
         match nil]
    (if decisions
      (let [[[k v] & rem] decisions]
        (if (tempid? k)
          (cond (instance? Long v)
                (if (< (long v) MAX_IMPLICIT_PARTITIONS)
                  (recur rem (assoc force k (d/implicit-part v)) match)
                  (recur rem (assoc force k (d/part v)) match))
                (tempid? v) (recur rem force (assoc match k v))
                (keyword? v) (recur rem (assoc force k v) match) ;; named partition
                ;; drop non-ids or keywords
                :else (recur rem force match))
          ;; drop non tempid k's
          (recur rem force match)))
      (cond-> nil
        force (assoc :db/force-partition force)
        match (assoc :db/match-partition match)))))

(defn process
  "returns transaction data augmented with partition assignments
   given a db, tx-data, and a map of attr kws to partition fns

   Partition fns are given a db, an entity map, an attr (keyword) and the value of the attr,
   they return a map whose keys are tempids, vals are partitions (as keyword or integer) or other tempids

   Entity maps without a user-defined :db/id will have a tempid generated"
  [db tx-data attr->partfn]
  (mapv (fn [form]
          (if (map? form)
            (let [emap (if (:db/id form)
                         form
                         (assoc form :db/id (d/tempid :db.part/user)))
                  ds (valid-directives (partition-decisions db emap attr->partfn))]
              (if ds
                (merge emap ds)
                form))
            form))
        tx-data))

(defn partition-fns
  "returns a map of attr keywords to partition fn or nil if nothing registered"
  [db]
  (when (d/entid db :nu.db/partition-fn)
    (reduce (fn [m [attr fn]]
              (assoc m attr (requiring-resolve fn)))
            nil
            (d/q '{:find [?attr ?fn]
                   :where [[?a :nu.db/partition-fn ?fn]
                           [?a :db/ident ?attr]]}
                 db))))

(def schema
  [{:db/ident :nu.db/partition-fn
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/symbol
    :db/doc
    "A schema annotation. A fully-qualified symbol naming a var that will act as a partition-fn, receiving
     a db, the entity map, the annotated attribute, and its value as arguments.
     Partition fns should return a map with tempid keys, and partition (keywords or integers) or other tempids as values"}])

(comment
  (valid-directives {(d/tempid 42) :part
                     "t1" :part
                     "t3" "t2"
                     nil 42
                     ::no :exist
                     "r3" 32})

  (valid-directives {(d/tempid 42) :part})
  (valid-directives {(d/tempid 42) "42"}))
