(ns evening.atom
  (:use evening.serdes)
  (:import [org.msgpack Packer Unpacker]))

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

(defn in-id-range-sig?
  "Is an id in an id range signature? May have false positives, but no
  false negatives."
  [sig id]
  (if-let [entry (sig (id-user id))]
    (let [min-offset (nth entry 0)
          max-offset (nth entry 1)
          offset (id-offset id)]
      (and (>= offset min-offset)
           (< offset max-offset)))))

;;; Serialization goes to and from byte arrays.

(defn pack-id-range-sig
  "Pack an id range signature with a Packer."
  [sig ^Packer packer]
  (.pack packer (int (count sig)))
  (doseq [[user [high low]] (seq sig)]
    (.pack packer (int user))
    (.pack packer (int high))
    (.pack packer (int low))))

(defn unpack-id-range-sig
  "Unpack an id range signature from an Unpacker."
  [^Unpacker unpacker]
  (let [len (.unpackInt unpacker)
        sig (transient {})]
    (dotimes [_ len]
      (let [user (.unpackInt unpacker) high (.unpackInt unpacker) low (.unpackInt unpacker)]
        (assoc! sig user [high low])))
    (persistent! sig)))


;;; Atom sequences come in two types: split and single. A split atom
;;; sequence consists of two arrays: a long[] array of ids, and an
;;; int[] array of (pred-yarn, pred-offset, char-code) triples. A
;;; single atom sequence is a simple int[] array of (yarn, offset,
;;; pred-yarn, pred-offset, char-code) 5-tuples. The former are
;;; optimized for linear scanning for a given id; the latter are
;;; optimized for being simple to work with, and easy to serialize.
;;;
;;; Here we provide accessor functions for the nth id, predecessor,
;;; and character of each type of sequence.

(defn split-atom-seq-nth-id [^longs idseq ^long n]
  (nth idseq n))

(defn split-atom-seq-nth-pred [^ints aseq ^long n]
  (let [base (* 3 n)]
    (pack-id (nth aseq base) (nth aseq (+ base 1)))))

(defn split-atom-seq-nth-char [^ints aseq ^long n]
  (char (nth aseq (+ (* n 3) 2))))

(defn split-atom-seq
  "Return a lazy seq of [yarn offset pred-yarn pred-offset char] atom
  entries from a split atom seq."
  [^longs idseq ^ints aseq]
  (letfn [(iter [i j] (if (< i (alength idseq))
                        (let [id (nth idseq i)
                              yarn (id-user id)
                              offset (id-offset id)]
                          (cons [yarn offset 
                                 (nth aseq j) (nth aseq (+ j 1))
                                 (char (nth aseq (+ j 2)))]
                                (iter (+ i 1) (+ j 3))))))]
    (iter 0 0)))


(defn single-atom-seq-nth-id [^ints aseq ^long n]
  (let [base (* 5 n)]
    (pack-id (nth aseq base) (nth aseq (+ base 1)))))

(defn single-atom-seq-nth-pred [^ints aseq ^long n]
  (let [base (* 5 n)]
    (pack-id (nth aseq (+ base 2)) (nth aseq (+ base 3)))))

(defn single-atom-seq-nth-char [^ints aseq ^long n]
  (char (nth aseq (+ (* n 5) 4))))

(defn single-atom-seq
  "Return a lazy seq of [yarn offset pred-yarn pred-offset char] atom
  entries from a single atom seq."
  [^ints aseq]
  (letfn [(iter [i] (if (< i (alength aseq))
                      (cons [(nth aseq i) (nth aseq (+ i 1))
                             (nth aseq (+ i 2)) (nth aseq (+ i 3))
                             (char (nth aseq (+ i 4)))]
                            (iter (+ i 5)))))]
    (iter 0)))
