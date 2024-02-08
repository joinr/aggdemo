(ns aggdemo.core
  (:require [tablecloth.api :as tc]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as fun]
            [tech.v3.dataset.reductions :as reds]
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
                              acc)) {:$group-name {:field-1 (k 0) :field-19 (k 1) :field-2 (k 2)}})))
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
                              acc)) {:$group-name {:field-1 (k 0) :field-19 (k 1) :field-2 (k 2)}})))
        (apply vector)
        ds/->dataset))


(defn reducing-agg [df]
  (let [cnames (->> df keys (filter #(numerical? (df %))))
        fields  (->> cnames (map (fn [k] [k (reds/mean k)])) (into {}))]
    (reds/group-by-column-agg
     [:field-1 :field-19 :field-2]
     fields
     df)))
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
;; Execution time mean : 445.641532 ms
;; Execution time std-deviation : 9.023157 mss
;; Execution time lower quantile : 435.675399 ms ( 2.5%)
;; Execution time upper quantile : 458.108112 ms (97.5%)
;; Overhead used : 1.858909 ns

#_
(c/quick-bench (fastest-agg clean))

;; Evaluation count : 6 in 6 samples of 1 calls.
;; Execution time mean : 276.388349 ms
;; Execution time std-deviation : 3.778802 ms
;; Execution time lower quantile : 272.463799 ms ( 2.5%)
;; Execution time upper quantile : 282.667499 ms (97.5%)
;; Overhead used : 1.858909 ns

;; Found 2 outliers in 6 samples (33.3333 %)
;; low-severe	 1 (16.6667 %)
;; low-mild	 1 (16.6667 %)
;; Variance from outliers : 13.8889 % Variance is moderately inflated by outliers


#_
(c/quick-bench (reducing-agg clean))

;; Evaluation count : 30 in 6 samples of 5 calls.
;; Execution time mean : 27.356328 ms
;; Execution time std-deviation : 2.659641 ms
;; Execution time lower quantile : 25.054558 ms ( 2.5%)
;; Execution time upper quantile : 30.521558 ms (97.5%)
;; Overhead used : 1.891062 ns

;;helper for verification.
(defn sort-by-group [ds]
  (ds/sort-by-column ds :$group-name (fn [l r] (compare (vec l) (vec r)))))
