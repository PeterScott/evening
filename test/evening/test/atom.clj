(ns evening.test.atom
  (:use [evening.atom] :reload)
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
      (is (sig= sig1 (deserialize-id-range-sig (serialize-id-range-sig sig1))))
      (is (sig= sig2 (deserialize-id-range-sig (serialize-id-range-sig sig2))))
      (is (= 24 (count (serialize-id-range-sig sig1))))
      (is (= 36 (count (serialize-id-range-sig sig2))))
      (is (= (type (serialize-id-range-sig sig1))
             (type (byte-array 0)))))))
