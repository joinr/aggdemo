(ns aggdemo.core
  (:require [tablecloth.api :as tc]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as fun]
            [criterium.core :as c]))

(def clean (tc/dataset "clean.tsv" {:separator "\t" :key-fn keyword}))

(defn mean [ds opts]
  (tc/aggregate-columns ds :type/numerical  fun/mean opts))


(defn agg [df]
  (-> df
      (tc/group-by [:field-1 :field-19 :field-2])
      (mean {:separate? false})))

(defn numerical? [col]
  (tech.v3.datatype.casting/is-host-numeric-datatype?
   (tech.v3.datatype/get-datatype col)))


(defn faster-agg [df]
  (->>  (for [[k grp] (ds/group-by df (juxt :field-1 :field-19 :field-2))]
          (->> grp
               (reduce-kv (fn [acc cname col]
                            (if (numerical? col)
                              (assoc acc cname (fun/mean col))
                              acc)) {:$group-key [k]})))
        (apply vector)
        ds/->dataset))


(defn naive-mean [xs]
  (let [n (count xs)]
    (/  (reduce + 0.0 xs)
        n)))


(defn fastest-agg [df]
  (->>  (for [[k grp] (ds/group-by df (juxt :field-1 :field-19 :field-2))]
          (->> grp
               (reduce-kv (fn [acc cname col]
                            (if (numerical? col)
                              (assoc acc cname (naive-mean col))
                              acc)) {:$group-key [k]})))
        (apply vector)
        ds/->dataset))

#_
(c/quick-bench (agg clean))

;; Evaluation count : 6 in 6 samples of 1 calls.
;; Execution time mean : 1.591757 sec
;; Execution time std-deviation : 36.901102 ms
;; Execution time lower quantile : 1.559710 sec ( 2.5%)
;; Execution time upper quantile : 1.653688 sec (97.5%)
;; Overhead used : 1.858909 ns

;; Found 2 outliers in 6 samples (33.3333 %)
;; low-severe	 1 (16.6667 %)
;; low-mild	 1 (16.6667 %)
;; Variance from outliers : 13.8889 % Variance is moderately inflated by outliers

#_
(c/quick-bench (faster-agg clean))

;; Evaluation count : 6 in 6 samples of 1 calls.
;; Execution time mean : 410.350732 ms
;; Execution time std-deviation : 7.332131 ms
;; Execution time lower quantile : 402.336599 ms ( 2.5%)
;; Execution time upper quantile : 420.504524 ms (97.5%)
;; Overhead used : 1.858909 ns

#_
(c/quick-bench (fastest-agg clean))

;; Evaluation count : 6 in 6 samples of 1 calls.
;; Execution time mean : 288.989482 ms
;; Execution time std-deviation : 19.631281 ms
;; Execution time lower quantile : 271.648599 ms ( 2.5%)
;; Execution time upper quantile : 319.472599 ms (97.5%)
;; Overhead used : 1.858909 ns

