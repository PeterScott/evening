(ns evening.weft
  (:use evening.atom))

;;; A weft is a map of user ids to the tops of their yarns. For
;;; example, the weft a5b2 could be written {1 5, 2 2}, where a's id is
;;; 1 and b's id is 2. These must be sorted maps, to get the
;;; comparison to work properly.

(def empty-weft (sorted-map))

(defn weft-extend [weft yarn offset]
  (update-in weft [yarn] #(max offset (or % 0))))

(defn weft-get [weft yarn]
  (or (weft yarn) 0))

(defn weft-covers [weft id]
  (<= (id-offset id) (weft-get weft (id-user id))))

(defn weft-merge
  "Merge zero or more wefts, returning a superweft of the set."
  [& wefts]
  (reduce #(merge-with max %1 %2) empty-weft wefts))

;;; Weft comparison operates in lexical order by weftI. The code for
;;; this is ugly, but fast and correct.

(defn weft-gt [a b]
  (loop [aseq (seq a) bseq (seq b)]
    (if (or (empty? aseq) (empty? bseq))
      (> (count a) (count b))
      (let [[my-yarn my-offset] (first aseq)
            [other-yarn other-offset] (first bseq)]
        (cond (< my-yarn other-yarn) true
              (> my-yarn other-yarn) false
              (> my-offset other-offset) true
              (< my-offset other-offset) false
              :else (recur (rest aseq) (rest bseq)))))))

;;; Some quick weft debugging stuff

(defn quickweft [^String spec]
  (apply weft-merge
         (for [i (range 0 (.length spec) 2)]
           (sorted-map (- (.codePointAt spec i) 96)
                       (- (.codePointAt spec (+ i 1)) 48)))))

(defn quickweft-print [weft]
  (apply str
         (mapcat (fn [[yarn offset]] [(char (+ yarn 96)) offset]) (seq weft))))
