(ns aggdemo.equality
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.categorical :as ds-cat]
            [tech.v3.dataset.column :as ds-col]))

;;going to monkey patch stuff for ease of demonstration....

(in-ns 'tech.v3.dataset.categorical)
;;they have dataset-level transforms.  uncertain how much this needs to apply
;;directly to columns, but done for example sake.
(defn transform-column
  "Apply a categorical mapping transformation fit with fit-categorical-map."
  ([column fit-data]
   (let [result-datatype (or (:result-datatype fit-data) :float64)
         lookup-table (:lookup-table fit-data)
         missing (ds-proto/missing column)
         col-meta (meta column)
         missing-value (col-base/datatype->missing-value result-datatype)]
     (col-impl/new-column
      (:name col-meta)
      (dtype/emap (fn [col-val]
                    (if-not (nil? col-val)
                      (let [numeric (get lookup-table col-val)]
                        (errors/when-not-errorf
                         numeric
                         "Failed to find label entry for column value %s"
                         col-val)
                        numeric)
                      missing-value))
                  result-datatype
                  column)
      (assoc col-meta :categorical-map fit-data)
      missing)))
  ;;could decide to throw if no categorical mapping exists.....for now it's idempotent.
  ([column] (if-let [fd (some-> column meta :categorical-map)]
              (transform-column column fd)
              column)))

;;uncertain how much this needs to apply directly to columns, but done for
;;example sake.
(defn invert-column
  "Invert a categorical map returning the column to the original set of values."
  ([column {:keys [src-column lookup-table] :as opts}]
   (let [res-dtype (reduce casting/widest-datatype
                           (map dtype/datatype (keys lookup-table)))
         inv-map (set/map-invert lookup-table)
         missing-val (col-base/datatype->missing-value res-dtype)]
     (col-impl/new-column
      (or src-column (ds-proto/column-name column))
      (dtype/emap (fn [col-val]
                    (if-not (nil? col-val)
                      (let [src-val (get inv-map (long col-val))]
                        (errors/when-not-errorf
                         src-val
                         "Unable to find src value for numeric value %s"
                         col-val)
                        src-val)
                      missing-val))
                  res-dtype
                  column)
      (-> (meta column)
          (dissoc :categorical-map)
          (assoc :categorical? true))
      (ds-proto/missing column))))
  ([column]
   (if-let [opts (some-> column meta :categorical-map)]
     (invert-column column opts)
     column)))


(defn catmap-dataset [ds]
  (reduce-kv (fn [acc k col]
               (let [m (meta col)]
                 (cond (m :categorical-map) acc
                       (m :categorical?)
                         (ds-proto/transform (fit-categorical-map ds k) acc)
                       :else acc)))
             ds ds))

(in-ns 'aggdemo.equality)


;;we want a way to project columns in a dataset trivially.  categorical namespace already has this.
;;one option is to derive a new column that is aware of categorical mappings and apply its notion of
;;equality (wrapped type).

;;Let's extend the notion of projection and inversion via protocol to unify
;;column/dataset etc.
(defprotocol IProjection
  (project [this])
  (invert  [this]))

;;use our newfound functions in the categorical ns to implement Projection semantics.
;;by default, our project/inverse are identity if the input is not compatible.  This
;;we we can still tap into = downstream with no problems.
(extend-protocol IProjection
  Object
  (project [this] this)
  (invert  [this] this)
  tech.v3.dataset.protocols.PColumn
  (project [this] (ds-cat/transform-column this))
  (invert  [this]  (ds-cat/invert-column this))
  tech.v3.dataset.protocols.PDataset
  (project [this] (ds-cat/catmap-dataset this))
  (invert  [this]  (ds-cat/reverse-map-categorical-xforms this)))


;;it looks like these manual column-level creations are handled at the dataset level already
;;upon column creation.  categorical ns defines transforms at the dataset level.

(def data
  (ds-col/new-column :a [0 1]
   {:categorical-map (ds-cat/create-categorical-map {"yes" 0 "no" 1} :a :int64)}))

(def prediction
  (ds-col/new-column :a [1 0]
   {:categorical-map (ds-cat/create-categorical-map {"yes" 1 "no" 0} :a :int64)}))

;;some features of categorical columns: they have categorical? in meta, and
;;a categorical-map to project to->from categories.

#_
(= data prediction) ;; => false
#_
(= (ds-cat/invert-column data) (ds-cat/invert-column prediction)) ;;=> true

;;  but we expect that
#_
(loss/classification-accuracy data prediction) = 1.0  ; which implies that both vector are the same


;; aggdemo.equality> (project d)
;; _unnamed [2 1]:

;; |  :v |
;; |----:|
;; | 1.0 |
;; | 0.0 |
;; aggdemo.equality> (invert (project d))
;; _unnamed [2 1]:

;; | :v |
;; |----|
;; | :A |
;; | :B |

;;define our custom notion of equality that expands to include categorical
;;inversion
(defn cat= [l r]
  (= (invert l) (invert r)))

#_
(cat= data prediction)
;;true

;;go whole-hog and replace = with cat=....not advised necessarily.
;;can also (:refer-clojure :exclude [=]) or rename it.

#_
(def = cat=)
