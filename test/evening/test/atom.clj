(ns evening.test.atom
  (:use [evening atom serdes] :reload)
  (:use [clojure.test]))

;;; Utility functions

(defn- map-values                       ; Borrowed from Datalog
  "Like map, but works over the values of a hash map"
  [f hash]
  (let [key-vals (map (fn [[key val]] [key (f val)]) hash)]
    (if (seq key-vals)
      (apply conj (empty hash) key-vals)
      hash)))

(defn- sig= [sig1 sig2]
  (let [a (map-values vec sig1)
        b (map-values vec sig2)]
    (= a b)))

;;; Tests

(deftest id-representation
  (let [aid (pack-id 42 666)
        bid (pack-id 1234 5678910)]
    (is (= (unpack-id aid) [42 666]))
    (is (= (unpack-id bid) [1234 5678910]))
    (is (= (id-user aid) 42))
    (is (= (id-offset aid) 666))
    (is (= (id-user bid) 1234))
    (is (= (id-offset bid) 5678910))))

(deftest id-range-sigs
  (let [sig1 {42 [3 7] 66 [2 8]}
        sig2 (add-to-id-range-sig (add-to-id-range-sig sig1 (pack-id 108 55))
                                  (pack-id 108 78))]
    (testing "id range signature membership"
      (is (in-id-range-sig? sig1 (pack-id 42 3)))
      (is (in-id-range-sig? sig1 (pack-id 42 5)))
      (is (not (in-id-range-sig? sig1 (pack-id 42 7))))
      (is (in-id-range-sig? sig2 (pack-id 42 4)))
      (is (in-id-range-sig? sig2 (pack-id 66 7)))
      (is (in-id-range-sig? sig2 (pack-id 108 58)))
      (is (not (in-id-range-sig? sig2 (pack-id 108 50)))))
    (testing "id range signature serdes"
      (is (sig= sig1
                (with-unpack-from-bytes [up (with-pack-to-bytes p (pack-id-range-sig sig1 p))]
                  (unpack-id-range-sig up))))
      (is (sig= sig2
                (with-unpack-from-bytes [up (with-pack-to-bytes p (pack-id-range-sig sig2 p))]
                  (unpack-id-range-sig up)))))))

(deftest atom-seqs
  (testing "split atom seqs"
    (let [ids (long-array [(pack-id 1 2) (pack-id 2 7)])
          aseq (int-array [3 4 97 1 8 100])]
      (is (= (unpack-id (split-atom-seq-nth-id ids 0)) [1 2]))
      (is (= (unpack-id (split-atom-seq-nth-id ids 1)) [2 7]))
      (is (= (unpack-id (split-atom-seq-nth-pred aseq 0)) [3 4]))
      (is (= (unpack-id (split-atom-seq-nth-pred aseq 1)) [1 8]))
      (is (= (split-atom-seq-nth-char aseq 0) \a))
      (is (= (split-atom-seq-nth-char aseq 1) \d))
      (is (= (split-atom-seq ids aseq) '([1 2 3 4 \a] [2 7 1 8 \d])))
      (is (nil? (split-atom-seq (long-array []) (int-array []))))))
  (testing "single atom seqs"
    (let [testseq (int-array [1 2 3 4 97 2 7 1 8 100])]
      (is (= (unpack-id (single-atom-seq-nth-id testseq 0)) [1 2]))
      (is (= (unpack-id (single-atom-seq-nth-id testseq 1)) [2 7]))
      (is (= (unpack-id (single-atom-seq-nth-pred testseq 0)) [3 4]))
      (is (= (unpack-id (single-atom-seq-nth-pred testseq 1)) [1 8]))
      (is (= (single-atom-seq-nth-char testseq 0) \a))
      (is (= (single-atom-seq-nth-char testseq 1) \d))
      (is (= (single-atom-seq testseq) '([1 2 3 4 \a] [2 7 1 8 \d])))
      (is (nil? (single-atom-seq (int-array [])))))))
