(ns datomic.afinity
  (:require [datomic.affinity-impl :as impl])
  (:import [java.util UUID]))

(set! *warn-on-reflection* true)

(def ^:const MAX_IMPLICIT_PARTITIONS_MASK 0x7FFFF)

(defn- implicit-part-clamp
  "clamps arg to a valid implicit partition (0 <= x < 2^19)"
  ^long [^long n]
  (bit-and n MAX_IMPLICIT_PARTITIONS_MASK))


(defn hash-uuid
  "hashes uuid (random or SQUUID), returning the id of an implicit partition, where
   0<=id<524288.

   WARNING
   Do not change the algorithms, they must be stable _across time_
   in order for partition assignment to be affine"
  ^long [^UUID u]
  ;; grab the 19 least significant bits of uuid, as those are random for SQUUIDs as well
  (implicit-part-clamp (.getLeastSignificantBits u)))


(defn altmod
  "reduction of an integer x into a space [0,max), but faster than (mod x max)
   Both args must be less than 2^32.

   Make sure x is sufficiently random before calling this, or use (-> x mix64 (altmod max))
   https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/"
  ^long [^long x ^long max]
  (-> (bit-and x 0xffffffff)
      (unchecked-multiply max)
      (unsigned-bit-shift-right 32)))

(def ^:const m1 (unchecked-long 0xbf58476d1ce4e5b9))
(def ^:const m2 (unchecked-long 0x94d049bb133111eb))


(defn mix64
  "bitmixer, take an intermediate hash value that may not be thoroughly
  mixed and increase its entropy to obtain both better distribution
  and fewer collisions among hashes. Small differences in input
  values, as you might get when hashing nearly identical data, should
  result in large differences in output values after mixing."
  ^long [^long x]
  (let [r1 (-> (bit-xor x (unsigned-bit-shift-right x 30))
               (unchecked-multiply m1))
        r2 (-> (bit-xor r1 (unsigned-bit-shift-right r1 27))
               (unchecked-multiply m2))]
    (bit-xor r2 (unsigned-bit-shift-right r2 31))))

(defn hash-eid
  "hashes eid, returning the id of an implicit partition, where 0<=id<524288.

   WARNING
   Do not change the algorithms, they must be stable _across time_
   in order for partition assignment to be affine."
  ^long [^long eid]
  (implicit-part-clamp (mix64 eid)))

(defn simulate
  "Dev helper. Processes given tx-data according to any attributes that have been annotated
   with partition functions. Returns tx-data potentially augmented with :db/force-partition and
   :db/match-partition annotations.

   Can optionally take a map of attr kws -> part-fns. If not given, will be fetched from the database."
  ([db tx-data]
   (simulate db tx-data (impl/partition-fns db)))
  ([db tx-data attr->part-fn]
   (impl/process db tx-data attr->part-fn)))

