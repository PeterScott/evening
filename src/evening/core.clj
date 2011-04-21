(ns evening.core
  (:use evening.btree))

;;; Example data
(def foo-leaf (leaf-node "foo-leaf" (.getBytes "Foo is the word here") true 20))
(def bar-leaf (leaf-node "bar-leaf" (.getBytes "We are the bar flies") false 20))
(def fubar-branch (branch-node "fubar-branch" [foo-leaf bar-leaf] true 40))
