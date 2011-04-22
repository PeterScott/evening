(ns evening.atom)

;;; An atom is represented as two 32-bit numbers: the user id and the
;;; offset within that user's yarn. For compactness, this can be
;;; packed into a single 64-bit long.

(defn ^:static pack-id ^long [^long user ^long offset]
  (bit-or (bit-shift-left user 32) offset))

(defn ^:static ^long id-offset [^long id]
  (bit-and id 0xFFFFFFFF))

(defn ^:static ^long id-user [^long id]
  (bit-and (bit-shift-right id 32) 0xFFFFFFFF))

(defn unpack-id [^long id]
  [(bit-and (bit-shift-right id 32) 0xFFFFFFFF)
   (bit-and id 0xFFFFFFFF)])

;;; Id range sigs are approximate signatures for sets of ids. They
;;; tell, for each user id, which offsets are present in the set of
;;; ids. Membership testing has false positives, but no false
;;; negatives. They're represented as a map of user ids to max- and
;;; min-offsets, represented as int[] arrays. Min is inclusive, and
;;; max is exclusive.

;; Example: {42 [3 7] 66 [2 8]}

(def empty-id-range-sig {0 (int-array 0 2)})

(defn add-to-id-range-sig [sig id]
  (let [user (id-user id)
        offset (id-offset id)]
    (update-in sig [user]
               (fn [entry]
                 (if entry
                   (let [min-offset (nth entry 0)
                         max-offset (nth entry 1)]
                     (cond (< offset min-offset) (int-array [offset max-offset])
                           (>= offset max-offset) (int-array [min-offset (+ offset 1)])
                           :else entry))
                   (int-array [offset (+ offset 1)]))))))

;;; Serialization goes to and from int[] arrays. FIXME: make it
;;; serialize to and from byte arrays? Or is that not necessary?

(defn serialize-id-range-sig [sig]
  (int-array (apply concat
                    (for [[user offset-range] (map vector (keys sig) (vals sig))]
                      [user (nth offset-range 0) (nth offset-range 1)]))))

(defn deserialize-id-range-sig [serialized]
  (loop [sig {} i 0]
    (if (>= i (count serialized))
      sig
      (recur (assoc sig (nth serialized i)
                    (int-array [(nth serialized (+ i 1)) (nth serialized (+ i 2))]))
             (+ i 3)))))
