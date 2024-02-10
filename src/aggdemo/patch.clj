(ns aggdemo.patch
  (:require [tech.v3.dataset :as ds]))


(in-ns 'tech.v3.dataset.impl.column)

(deftype Column
    [^RoaringBitmap missing
     data
     ^IPersistentMap metadata
     ^:unsynchronized-mutable ^Buffer buffer]

  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [_this]
    (and (.isEmpty missing)
         (dtype-proto/convertible-to-array-buffer? data)))
  (->array-buffer [_this]
    (dtype-proto/->array-buffer data))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [_this]
    (and (.isEmpty missing)
         (dtype-proto/convertible-to-native-buffer? data)))
  (->native-buffer [_this]
    (dtype-proto/->native-buffer data))
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [_this] (dtype-proto/elemwise-datatype data))
  dtype-proto/POperationalElemwiseDatatype
  (operational-elemwise-datatype [this]
    (if (.isEmpty missing)
      (dtype-proto/elemwise-datatype this)
      (let [ewise-dt (dtype-proto/elemwise-datatype data)]
        (cond
          (packing/packed-datatype? ewise-dt)
          (packing/unpack-datatype ewise-dt)
          (casting/numeric-type? ewise-dt)
          (casting/widest-datatype ewise-dt :float64)
          (identical? :boolean ewise-dt)
          :object
          :else
          ewise-dt))))
  dtype-proto/PElemwiseCast
  (elemwise-cast [_this new-dtype]
    (let [new-data (dtype-proto/elemwise-cast data new-dtype)]
      (Column. missing
               new-data
               metadata
               nil)))
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [this new-dtype]
    (if (= new-dtype (dtype-proto/elemwise-datatype data))
      (cached-buffer!)
      (make-column-buffer missing (dtype-proto/elemwise-reader-cast data new-dtype)
                          new-dtype new-dtype)))
  dtype-proto/PECount
  (ecount [this] (dtype-proto/ecount data))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [_this] true)
  (->buffer [this]
    (cached-buffer!))
  dtype-proto/PToReader
  (convertible-to-reader? [_this]
    (dtype-proto/convertible-to-reader? data))
  (->reader [this]
    (dtype-proto/->buffer this))
  dtype-proto/PToWriter
  (convertible-to-writer? [_this]
    (dtype-proto/convertible-to-writer? data))
  (->writer [this]
    (dtype-proto/->buffer this))
  dtype-proto/PSubBuffer
  (sub-buffer [this offset len]
    (let [offset (long offset)
          len (long len)
          eidx (+ offset len)]
      (ChunkedList/sublistCheck offset (+ offset len) (dtype/ecount data))
      (if (and (== offset 0) (== len (dtype/ecount this)))
        this
        ;;TODO - use bitmap operations to perform this calculation
        (let [new-missing (RoaringBitmap/and
                           missing
                           (doto (RoaringBitmap.)
                             (.add offset (+ offset len))))
              new-data  (dtype-proto/sub-buffer data offset len)]
          (Column. new-missing
                   new-data
                   metadata
                   nil)))))
  dtype-proto/PClone
  (clone [_col]
    (let [new-data (if (or (dtype/writer? data)
                           (= :string (dtype/get-datatype data))
                           (= :encoded-text (dtype/get-datatype data)))
                     (dtype/clone data)
                     ;;It is important that the result of this operation be writeable.
                     (dtype/make-container :jvm-heap
                                           (dtype/get-datatype data) data))
          cloned-missing (dtype/clone missing)]
      (Column. cloned-missing
               new-data
               metadata
               nil)))
  ds-proto/PRowCount
  (row-count [this] (dtype/ecount data))
  ds-proto/PMissing
  (missing [this] missing)
  ds-proto/PSelectRows
  (select-rows [this rowidxs]
    (let [rowidxs     (simplify-row-indexes (dtype/ecount this) rowidxs)
          new-missing (if (== 0 (set/cardinality missing))
                        (bitmap/->bitmap)
                        (argops/argfilter (set/contains-fn missing)
                                          {:storage-type :bitmap}
                                          rowidxs))
          new-data (dtype/indexed-buffer rowidxs data)]
      (Column. new-missing
               new-data
               metadata
               nil)))
  ds-proto/PColumn
  (is-column? [_this] true)
  (column-buffer [_this] data)
  ds-proto/PColumnName
  (column-name [this] (get metadata :name))
  IMutList
  (size [this] (.size (cached-buffer!)))
  (get [this idx] (.get (cached-buffer!) idx))
  (set [this idx v] (.set (cached-buffer!) idx v))
  (getLong [this idx] (.getLong (cached-buffer!) idx))
  (setLong [this idx v] (.setLong (cached-buffer!) idx v))
  (getDouble [this idx] (.getDouble (cached-buffer!) idx))
  (setDouble [this idx v] (.setDouble (cached-buffer!) idx v))
  (valAt [this idx] (.valAt (cached-buffer!) idx))
  (valAt [this idx def-val] (.valAt (cached-buffer!) idx def-val))
  (invoke [this idx] (.invoke (cached-buffer!) idx))
  (invoke [this idx def-val] (.invoke (cached-buffer!) idx def-val))
  (meta [this] (assoc metadata
                      :datatype (dtype-proto/elemwise-datatype this)
                      :n-elems (dtype-proto/ecount this)))
  (withMeta [_this new-meta] (Column. missing
                                     data
                                     new-meta
                                     buffer))
  (nth [this idx] (nth (cached-buffer!) idx))
  (nth [this idx def-val] (nth (cached-buffer!) idx def-val))
  ;;should be the same as using hash-unorded-coll, effectively.
  (hasheq [this]   (.hasheq (cached-buffer!)))
  (equiv [this o] (if (identical? this o) true (.equiv (cached-buffer!) o)))
  (empty [this]
    (Column. (->bitmap)
             (column-base/make-container (dtype-proto/elemwise-datatype this) 0)
             {}
             nil))
  (reduce [this rfn init] (.reduce (cached-buffer!) rfn init))
  (parallelReduction [this init-val-fn rfn merge-fn options]
    (.parallelReduction (cached-buffer!) init-val-fn rfn merge-fn options))
  Object
  (toString [item]
    (let [n-elems (dtype/ecount data)
          format-str (if (> n-elems 20)
                       "#tech.v3.dataset.column<%s>%s\n%s\n[%s...]"
                       "#tech.v3.dataset.column<%s>%s\n%s\n[%s]")]
      (format format-str
              (name (dtype/elemwise-datatype item))
              [n-elems]
              (ds-proto/column-name item)
              (-> (dtype-proto/sub-buffer item 0 (min 20 n-elems))
                  (dtype-pp/print-reader-data)))))

  ;;Delegates to ListPersistentVector, which caches results for us.
  (hashCode [this] (.hasheq (cached-buffer!)))
  (equals [this o] (.equiv this o)))


(dtype-pp/implement-tostring-print Column)


(defn construct-column
  "Low level column construction.  No analysis is done of the data to detect
  datatype or missing values - the onus is on you.  The column name should
  be provided as the `:name` member of the metadata.  Data must have a
  conversion to a buffer - [tech.v3.datatype.protocols/PToBuffer](https://github.com/cnuernber/dtype-next/blob/master/src/tech/v3/datatype/protocols.clj#L131)."
  ^Column [missing data metadata]
  (Column. (bitmap/->bitmap missing) data metadata nil))


(defn new-column
  "Given a map of (something convertible to a long reader) missing indexes,
  (something convertible to a reader) data
  and a (string or keyword) name, return an implementation of enough of the
  column and datatype protocols to allow efficient columnwise operations of
  the rest of tech.ml.dataset"
  ([name data metadata missing]
   (new-column #:tech.v3.dataset{:name name
                                 :data data
                                 :metadata metadata
                                 :missing missing}))
  ([name data metadata]
   (new-column name data metadata nil))
  ([name data]
   (new-column name data {} nil))
  ([data-or-column-data-map]
   (let [coldata (column-data-process/prepare-column-data data-or-column-data-map)
         data (coldata :tech.v3.dataset/data)
         metadata (coldata :tech.v3.dataset/metadata)
         name (coldata :tech.v3.dataset/name)
         ;;Unless overidden, we now set the categorical? flag
         metadata (if (and (not (contains? metadata :categorical?))
                           (column-base/column-datatype-categorical?
                            (dtype/elemwise-datatype data)))
                    (assoc metadata :categorical? true)
                    metadata)
         missing (bitmap/->bitmap (coldata :tech.v3.dataset/missing))]
     ;;compress bitmaps
     (.runOptimize missing)
     (let [new-meta (assoc metadata :name name)]
       (Column. missing
                data
                new-meta
                nil)))))

(in-ns 'tech.v3.dataset.impl.dataset)

(deftype Dataset [^List columns
                  colmap
                  ^IPersistentMap metadata
                  ^{:unsynchronized-mutable true :tag 'int}  _hash
                  ^{:unsynchronized-mutable true :tag 'int}  _hasheq]
  java.util.Map
  (size [this]    (.count this))
  (isEmpty [this] (not (pos? (.count this))))
  (containsValue [_this v] (some #(= % v) columns))
  (get [this k] (.valAt this k))
  (put [_this _k _v]  (throw (UnsupportedOperationException.)))
  (remove [_this _k] (throw (UnsupportedOperationException.)))
  (putAll [_this _m] (throw (UnsupportedOperationException.)))
  (clear [_this]    (throw (UnsupportedOperationException.)))
  (keySet [_this] (.keySet ^Map colmap))
  (values [_this] columns)
  (entrySet [_this]
    (let [retval (LinkedHashSet.)]
      (.addAll retval (map #(clojure.lang.MapEntry. (:name (meta %)) %)
                           columns))
      retval))

  clojure.lang.ILookup
  (valAt [_this k]
    (when-let [idx (colmap k)]
      (.get columns idx)))
  (valAt [this k not-found]
    (if-let [res (.valAt this k)]
      res
      not-found))

  clojure.lang.MapEquivalence

  clojure.lang.IPersistentMap
  (assoc [this k v]
    (let [n-cols (ds-proto/column-count this)
          c (coldata->column n-cols
                             (ds-proto/row-count this)
                             k v)
          cidx (get colmap k n-cols)]
      (Dataset. (assoc (or columns []) cidx c) (assoc colmap k cidx) metadata 0 0)))
  (assocEx [this k v]
    (if-not (colmap k)
      (.assoc this k v)
      (throw (ex-info "Key already present" {:k k}))))
  ;;without implements (dissoc pm k) behavior
  (without [this k]
    (if-let [cidx (get colmap k)]
      (let [cols (hamf/concatv (hamf/subvec columns 0 cidx) (hamf/subvec columns (inc cidx)))]
        (Dataset. cols
                  (into {} (map-indexed #(vector (:name (meta %2)) %1)) cols)
         metadata 0 0))
      this))
  (entryAt [this k]
    (when-let [v (.valAt this k)]
      (clojure.lang.MapEntry. k v)))
  ;;No idea if this is correct behavior....
  (empty [_this] (empty-dataset))
  ;;ported from clojure java impl.
  (cons [this e]
    (cond (instance? java.util.Map$Entry e)
            (.assoc this (key e) (val e))
          (vector? e)
            (let [^clojure.lang.PersistentVector e e]
              (when-not (== (.count e) 2)
                (throw (ex-info "Vector arg to map conj must be a pair" {})))
              (.assoc this (.nth e 0) (.nth e 1)))
          :else
            (reduce (fn [^clojure.lang.Associative acc entry]
                      (.assoc acc (key entry) (val entry))) this e)))

  (containsKey [_this k] (.containsKey ^Map colmap k))

  ;;MAJOR DEVIATION
  ;;This conforms to clojure's idiom and projects the dataset onto a
  ;;seq of [column-name column] entries.  Legacy implementation defaulted
  ;;to using iterable, which was a seq of column.
  (seq [_this]
    ;;Do not reorder column data if possible.
    (when (pos? (count columns))
      (map #(clojure.lang.MapEntry. (:name (meta %)) %)  columns)))

  ;;Equality is likely a rat's nest, although we should be able to do it
  ;;if we wanted to!
  (hashCode [this]
    (when (== _hash 0)
      (set! _hash (clojure.lang.APersistentMap/mapHash  this)))
    _hash)

  clojure.lang.IHashEq
  ;;intentionally using seq instead of iterator for now.
  (hasheq [this]
    (when (== _hasheq 0)
      (set! _hasheq (hash-unordered-coll (or (.seq this)
                                             []))))
    _hasheq)

  ;;DOUBLE CHECK equals/equiv semantics...
  (equals [this o] (or (identical? this o)
                       (clojure.lang.APersistentMap/mapEquals this o)))

  (equiv [this o] (or (identical? this o)
                      (map-equiv this o)))


  ds-proto/PRowCount
  (row-count [this]
    (if (== 0 (.size columns))
      0
      (dtype/ecount (.get columns 0))))


  ds-proto/PColumnCount
  (column-count [this]
    (.size columns))


  ds-proto/PMissing
  (missing [this]
    (RoaringBitmap/or (.iterator ^Iterable (lznc/map ds-proto/missing (vals this)))))

  ds-proto/PSelectRows
  (select-rows [dataset rowidxs]
    (let [rowidxs (col-impl/simplify-row-indexes (ds-proto/row-count dataset) rowidxs)] ;;indicate simplified.
      (when (== 0 (long (ds-proto/row-count dataset)))
        (when-not (== 0 (dtype/ecount rowidxs))
          (throw (IndexOutOfBoundsException. "Cannot select rows of empty dataset"))))
      (->> columns
           ;;select may be slower if we have to recalculate missing values.
           (lznc/map #(ds-proto/select-rows % rowidxs))
           (new-dataset (ds-proto/dataset-name dataset)
                        (dissoc metadata :print-index-range)))))


  ds-proto/PSelectColumns
  (select-columns [dataset colnames]
    ;;Conversion to a reader is expensive in some cases so do it here
    ;;to avoid each column doing it.
    (let [map-selector? (instance? Map colnames)]
      (->> (cond
             (identical? :all colnames)
             columns
             map-selector?
             (->> colnames
                  (lznc/map (fn [[old-name new-name]]
                              (if-let [col-idx (get colmap old-name)]
                                (vary-meta (.get columns (unchecked-int col-idx))
                                           assoc :name new-name)
                                (throw (Exception.
                                        (format "Failed to find column %s" old-name)))))))
             :else
             (let [idx-set (HashSet.)]
               (->> colnames
                    (lznc/map (fn [colname]
                                (if-let [col-idx (get colmap colname)]
                                  (when-not (.contains idx-set col-idx)
                                    (.add idx-set col-idx)
                                    (.get columns (unchecked-int col-idx)))
                                  (throw (Exception.
                                          (format "Failed to find column %s" colname))))))
                    (lznc/remove nil?))))
           (new-dataset (ds-proto/dataset-name dataset) metadata))))

  ds-proto/PDataset
  (is-dataset? [item] true)
  (column [ds cname]
    (if-let [retval (.get ds cname)]
      retval
      (throw (RuntimeException. (str "Column not found: " cname)))))

  (rowvecs [ds options]
    (let [readers (hamf/object-array-list
                   (lznc/map dtype/->reader columns))
          n-cols (count readers)
          n-rows (long (ds-proto/row-count ds))
          copying? (get options :copying? true)
          ^IFn$LO row-fn
          (if copying?
            (case n-cols
              0 (hamf-fn/long->obj row-idx [])
              1 (row-vec-copying 1)
              2 (row-vec-copying 2)
              3 (row-vec-copying 3)
              4 (row-vec-copying 4)
              5 (row-vec-copying 5)
              6 (row-vec-copying 6)
              7 (row-vec-copying 7)
              8 (row-vec-copying 8)
              (let [crange (hamf/range n-cols)]
                (hamf-fn/long->obj
                 row-idx
                 (->> crange
                      (lznc/map (hamf-fn/long->obj
                                 col-idx (.readObject ^Buffer (.get readers col-idx) row-idx)))
                      (hamf/vec)))))
            ;;Non-copying in-place reader
            (let [crange (hamf/range n-cols)]
              (hamf-fn/long->obj
               row-idx
               (reify ObjectReader
                 (lsize [this] n-cols)
                 (readObject [this col-idx]
                   (.readObject ^Buffer (.get readers col-idx) row-idx))
                 (reduce [this rfn acc]
                   (reduce (hamf-rf/long-accumulator
                            acc col-idx (rfn acc ((.get readers col-idx) row-idx)))
                           acc
                           crange))))))]
      (reify ObjectReader
        (lsize [rdr] n-rows)
        (readObject [rdr row-idx] (.invokePrim row-fn row-idx))
        (subBuffer [rdr sidx eidx]
          (-> (ds-proto/select-rows ds (hamf/range sidx eidx))
              (ds-proto/rowvecs options)))
        (reduce [rdr rfn acc]
          (reduce (hamf-rf/long-accumulator
                   acc row-idx
                   (rfn acc (.invokePrim row-fn row-idx)))
                  acc (hamf/range n-rows))))))


  (rows [ds options]
    (let [^Buffer rvecs (.rowvecs ds options)
          colnamemap (LinkedHashMap.)
          _ (reduce (hamf-rf/indexed-accum
                     acc idx cname
                     (.put colnamemap cname idx)
                     colnamemap)
                    colnamemap
                    (lznc/map #(get (.-metadata ^Column %) :name) columns))
          n-rows (dtype/ecount rvecs)
          nil-missing? (get options :nil-missing?)
          ^RoaringBitmap ds-missing (when (not nil-missing?) (ds-proto/missing ds))
          constant-cmap? (or nil-missing? (.isEmpty ds-missing))
          ^IFn$LO cmap-fn (if constant-cmap?
                            (fn [^long v] colnamemap)
                            (let [col-fns
                                  (->> (vals ds)
                                       (lznc/map (fn [col]
                                                   (let [^RoaringBitmap col-missing (ds-proto/missing col)]
                                                     (when-not (.isEmpty col-missing)
                                                       (let [ckey (ds-proto/column-name col)]
                                                         (fn [^long row-idx ^Map colnamemap]
                                                           (when (.contains col-missing (unchecked-int row-idx))
                                                             (.remove colnamemap ckey))
                                                           colnamemap))))))
                                       (lznc/remove nil?)
                                       (hamf/object-array))
                                  n-fns (alength col-fns)]
                              (fn [^long row-idx]
                                (if (.contains ds-missing (unchecked-int row-idx))
                                  (let [colnamemap (.clone colnamemap)]
                                    (dotimes [idx n-fns]
                                      (.invokePrim ^IFn$LOO (aget col-fns idx) row-idx colnamemap))
                                    colnamemap)
                                  colnamemap))))]
      (reify ObjectReader
        (lsize [rdr] n-rows)
        (readObject [rdr idx]
          (FastStruct. (.invokePrim cmap-fn idx) (.readObject rvecs idx)))
        (subBuffer [rdr sidx eidx]
          (-> (ds-proto/select-rows ds (hamf/range sidx eidx))
              (ds-proto/rows options)))
        (reduce [rdr rfn acc]
          (reduce (if constant-cmap?
                    (fn [acc vv]
                      (rfn acc (FastStruct. colnamemap vv)))
                    (hamf-rf/indexed-accum
                     acc row-idx vv
                     (rfn acc (FastStruct. (.invokePrim cmap-fn row-idx) vv))))
                  acc
                  rvecs)))))


  dtype-proto/PShape
  (shape [ds]
    [(count columns) (ds-proto/row-count ds)])

  dtype-proto/PCopyRawData
  (copy-raw->item! [_raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! columns ary-target target-offset options))
  dtype-proto/PClone
  (clone [item]
    (new-dataset (ds-proto/dataset-name item)
                 metadata
                 (mapv dtype/clone columns)))

  Counted
  (count [_this] (count columns))

  IFn
  ;;NON-OBVIOUS SEMANTICS
  ;;Legacy implementation of invoke differs from clojure idioms for maps,
  ;;and instead of performing a lookup, we have effectively an assoc.
  ;;Is this necessary, or can we better conform to clojure idioms?
  ;; (invoke [item col-name new-col]
  ;;   (ds-proto/add-column item (ds-col-proto/set-name new-col col-name)))

  (invoke [this k]
    (.valAt this k))
  (invoke [this k not-found]
    (.valAt this k not-found))
  (applyTo [this arg-seq]
    (case (count arg-seq)
      1 (.invoke this (first arg-seq))
      2 (.invoke this (first arg-seq) (second arg-seq))))

  IObj
  (meta [_this] metadata)
  (withMeta [_this metadata] (Dataset. columns colmap metadata _hash _hasheq))

  Iterable
  (iterator [_item]
    (.iterator (map-entries columns)))

  Object
  (toString [item]
    (ds-print/dataset->str item)))


(defn construct-dataset
  "Low-level construct a new dataset.

  * 'columns' - java.util.List of [[tech.v3.dataset.impl.column.Column]] objects.
  * 'colmap' - java.util.Map of column name to index in columns.
  * 'metadata' - extra metadata for the dataset.  Dataset name is store as `:name`."
  (^Dataset [columns colmap metadata]
   ;;quick sanity check
   (assert (== (count colmap) (count columns)))
   (Dataset. columns colmap metadata 0 0))
  (^Dataset [columns metadata]
   (Dataset. columns (persistent!
                      (reduce (hamf-rf/indexed-accum
                               acc idx col
                               (.put ^Map acc (or (:name (meta col)) idx) idx)
                               acc)
                              (hamf/mut-map)
                              columns))
             metadata 0 0)))


(defn new-dataset
  "Create a new dataset from a sequence of columns.  Data will be converted
  into columns using ds-col-proto/ensure-column-seq.  If the column seq is simply a
  collection of vectors, for instance, columns will be named ordinally.
  options map -
    :dataset-name - Name of the dataset.  Defaults to \"_unnamed\".
    :key-fn - Key function used on all column names before insertion into dataset.

  The return value fulfills the dataset protocols."
  ([options ds-metadata column-seq]
   (let [;;Options was dataset-name so have to keep that pathway going.
         dataset-name (or (if (map? options)
                            (:dataset-name options)
                            options)
                          (:name ds-metadata)
                          "_unnamed")
         column-seq (hamf/vec column-seq)]
     (if-not (seq column-seq)
       (Dataset. [] {}
                 (assoc (col-impl/->persistent-map ds-metadata)
                        :name
                        dataset-name) 0 0)
       (let [column-seq (hamf/vec column-seq)
             ;;sizes is only used to determine if we have no columns (empty set) -> 0,
             ;;or the maximum size of a column.
             ;;we can avoid the overhead by just reducing over the columns and
             ;;retaining the maximum size.
             sizes (->> column-seq
                        (reduce (fn [acc data]
                                  (let [data (if (map? data)
                                               (get data :tech.v3.dataset/data data)
                                               data)
                                        argtype (argtypes/arg-type data)]
                                    ;;nil return expected
                                    (max acc
                                         (if (or (identical? argtype :scalar)
                                                 (identical? argtype :iterable))
                                           1
                                           (dtype/ecount data))))) 0))
             #_ #_
             sizes (->> column-seq
                        (lznc/map (fn [data]
                                    (let [data (if (map? data)
                                                 (get data :tech.v3.dataset/data data)
                                                 data)
                                          argtype (argtypes/arg-type data)]
                                      ;;nil return expected
                                      (if (or (identical? argtype :scalar)
                                              (identical? argtype :iterable))
                                        1
                                        (dtype/ecount data)))))
                        (lznc/remove nil?)
                        (hamf/immut-set))

             n-rows (long sizes) #_(long (if (== 0 (count sizes))
                                     0
                                     (apply max sizes)))
             n-cols (count column-seq)
             key-fn (or (when (map? options)
                          (get options :key-fn identity))
                        identity)
             column-seq (->> column-seq
                             (lznc/map-indexed
                              (fn [idx column]
                                (let [cname (ds-proto/column-name column)
                                      cname (if (or (nil? cname)
                                                    (and (string? cname)
                                                         (== 0 (.length ^String cname))))
                                              (key-fn idx)
                                              (key-fn cname))]
                                  (coldata->column n-cols n-rows cname column))))
                             (hamf/vec))]
         (Dataset. column-seq
                   (do
                     (let [hs (hamf/mut-map)]
                       (reduce (hamf-rf/indexed-accum
                                acc idx col
                                (.put hs (ds-proto/column-name col) idx))
                               nil
                               column-seq)
                       (persistent! hs)))
                   (assoc (col-impl/->persistent-map ds-metadata)
                          :name
                          dataset-name)
                   0 0)))))
  ([options column-seq]
   (new-dataset options {} column-seq))
  ([column-seq]
   (new-dataset {} {} column-seq)))


;;eliminate RestFn from vary-meta.
(defn- coldata->column
  [^long n-cols ^long n-rows col-name new-col-data]
  (let [argtype (argtypes/arg-type new-col-data)
        n-rows (if (== 0 n-cols)
                 (cond
                   (dtype/reader? new-col-data)
                   (dtype/ecount new-col-data)
                   (map? new-col-data)
                   (if (contains? new-col-data :tech.v3.dataset/data)
                     (dtype/ecount (new-col-data :tech.v3.dataset/data))
                     1)
                   :else
                   (count (take Integer/MAX_VALUE new-col-data)))
                 n-rows)]
    (cond
      (ds-proto/is-column? new-col-data)
      (with-meta new-col-data (assoc (meta new-col-data) :name col-name))
      ;;maps are scalars in dtype-land but you can pass in column data maps
      ;;so this next check is a bit hairy
      (or (identical? argtype :scalar)
           ;;This isn't a column data map.  Maps are
          (and (instance? Map new-col-data)
               (not (.containsKey ^Map new-col-data :tech.v3.dataset/data))))
      (col-impl/new-column col-name (dtype/const-reader new-col-data n-rows))
      :else
      (let [map-data
            (if (or (= argtype :iterable)
                    (map? new-col-data))
              (column-data-process/prepare-column-data
               (if (map? new-col-data)
                 new-col-data
                 (take n-rows new-col-data)))
              ;;Else has to be reader or tensor.
              (let [data-ecount (dtype/ecount new-col-data)
                    new-col-data (if (> data-ecount n-rows)
                                   (dtype/sub-buffer new-col-data 0 n-rows)
                                   new-col-data)]
                (column-data-process/prepare-column-data new-col-data)))
            new-c (col-impl/new-column (assoc map-data :tech.v3.dataset/name col-name))
            c-len (dtype/ecount new-c)]
        (cond
          (< c-len n-rows)
          (col-impl/extend-column-with-empty new-c (- n-rows c-len))
          (> c-len n-rows)
          (dtype/sub-buffer new-c 0 n-rows)
          :else
          new-c)))))





(in-ns 'aggdemo.patch)


