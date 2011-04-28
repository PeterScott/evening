(ns evening.test.memodict
  (:use [clojure.test])
  (:use [evening memodict weft atom serdes]))

;;; Example memodict from Grishchenko's paper
(def foomemo
  (new-memodict {(pack-id 1 1) (quickweft "a1")
                 (pack-id 1 5) (quickweft "a5b2")
                 (pack-id 2 1) (quickweft "a3b1")
                 (pack-id 2 2) (quickweft "a3b2")}))

(deftest test-memodict-get
  (is (= (memodict-get foomemo (pack-id 1 1)) {1 1}))
  (is (= (memodict-get foomemo (pack-id 1 4)) {1 4}))
  (is (= (memodict-get foomemo (pack-id 1 5)) {1 5, 2 2}))
  (is (= (memodict-get foomemo (pack-id 1 8)) {1 8, 2 2}))
  (is (= (memodict-get foomemo (pack-id 2 1)) {1 3, 2 1}))
  (is (= (memodict-get foomemo (pack-id 2 2)) {1 3, 2 2}))
  (is (= (memodict-get foomemo (pack-id 2 10)) {1 3, 2 10}))
  (is (= (memodict-get foomemo (pack-id 10 42)) {10 42})))

(deftest test-memodict-add!
  (let [md (new-memodict {(pack-id 1 1) (quickweft "a1")})]
    (is (= md {(pack-id 1 1) (quickweft "a1")}))
    (memodict-add! md (pack-id 2 1) (quickweft "a3b1"))
    (memodict-add! md (pack-id 1 5) (quickweft "a5b2"))
    (is (= md {(pack-id 1 1) (quickweft "a1")
               (pack-id 1 5) (quickweft "a5b2")
               (pack-id 2 1) (quickweft "a3b1")}))
    (memodict-add! md (pack-id 2 2) (quickweft "a3b2"))
    (is (= md foomemo))))

(deftest test-pull
  (is (= (pull foomemo (pack-id 1 3) 0) {1 3}))
  (is (= (pull foomemo (pack-id 1 6) 0) {1 6, 2 2}))
  (is (= (pull foomemo (pack-id 1 6) (pack-id 5 6)) {1 6, 2 2, 5 6}))
  (is (= (pull foomemo (pack-id 1 2) (pack-id 2 1)) {1 3, 2 1})))

(deftest test-serdes
  (is (= foomemo (pack-then-unpack pack-memodict unpack-memodict foomemo)))
  (is (= (new-memodict) (pack-then-unpack pack-memodict unpack-memodict (new-memodict)))))
