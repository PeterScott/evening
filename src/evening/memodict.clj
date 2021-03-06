(ns evening.memodict
  (:use [evening atom weft])
  (:import [java.util TreeMap SortedMap NoSuchElementException])
  (:import [org.msgpack Packer Unpacker]))

;;; An id-to-weft memoization dict maps the ids of atoms whose
;;; predecessors are in another yarn to their awareness wefts. As long
;;; as this structure is maintained, pulling becomes an O(1)
;;; operation. The map must support predecessor queries.
;;;
;;; Internally, this uses a TreeMap, which maps 64-bit atom ids to
;;; wefts. When doing lookup, it's important to check to make sure
;;; that a predecessor query hasn't gone back into another yarn.

(defn- get-ancestor-id
  "Return a number or the key of its nearest predecessor in a
  TreeMap. Returns nil if there is no such predecessor."
  [^TreeMap treemap x]
  (let [^SortedMap headmap (.headMap treemap (+ x 1))]
    (try (.lastKey headmap)
         (catch NoSuchElementException _
           nil))))

(defn new-memodict [& [oldmap]] (TreeMap. (or oldmap {})))

(defn memodict-add!
  "Add an (id, weft) pair to a memoization dict, modifying it in
  place. If there is already an entry there, it will be replaced."
  [^TreeMap memodict ^long id weft]
  (.put memodict id weft))

(defn memodict-get
  "Look up an id in a memoization dict. Returns either an empty weft,
   or the weft in the given yarn with the highest offset less than or
   equal to the given offset."
  [^TreeMap memodict ^long id]
  (if-let [ancestor-id (get-ancestor-id memodict id)]
    (cond
     ;; Exact id found in memodict. Return it.
     (== id ancestor-id) (.get memodict id)
     ;; Same yarn. Extend weft.
     (== (id-user id) (id-user ancestor-id))
     (weft-extend (.get memodict ancestor-id) (id-user id) (id-offset id))
     ;; Nothing found. Return singleton weft.
     :else (sorted-map (id-user id) (id-offset id)))
    (sorted-map (id-user id) (id-offset id))))

(defn pull
  "Pull the awareness weft of a given atom id, assuming a properly
   filled-out memoization dict. If the predecessor id is 0, it will be
   ignored."
  [^TreeMap memodict ^long id ^long pred]
  (let [weft (weft-extend (memodict-get memodict id)
                          (id-user id) (id-offset id))]
    (if (== pred 0)
      weft
      (weft-merge weft (memodict-get memodict pred)))))

(defn pack-memodict [^TreeMap memodict ^Packer packer]
  (.packMap packer (.size memodict))
  (doseq [entry memodict] ; ^java.util.Map.Entry
    (let [id (long (.getKey entry))
          yarn (id-user id) offset (id-offset id)]
      (.pack packer (int yarn))
      (.pack packer (int offset)))
    (pack-weft (.getValue entry) packer)))

(defn unpack-memodict [^Unpacker unpacker]
  (let [len (.unpackMap unpacker)
        memodict (TreeMap.)]
    (dotimes [_ len]
      (let [yarn (.unpackInt unpacker) offset (.unpackInt unpacker)]
        (.put memodict (pack-id yarn offset)
              (unpack-weft unpacker))))
    memodict))

;;; Example memodict from Grishchenko's paper
; (def foomemo
;   (TreeMap. {(pack-id 1 1) (quickweft "a1")
;              (pack-id 1 5) (quickweft "a5b2")
;              (pack-id 2 1) (quickweft "a3b1")
;              (pack-id 2 2) (quickweft "a3b2")}))
