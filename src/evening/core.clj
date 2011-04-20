(ns evening.core
  (:use [clojure.contrib.json :only (json-str read-json)])
  (:require [clj-riak.client :as riak]))

;;;;;;;;;;;;;;;;;;;;;;; Riak server connection ;;;;;;;;;;;;;;;;;;;;;;;

;;; Riak connection parameters. Change these to fit your installation.
(def *riak-connection* (riak/init {:host "127.0.0.1" :port 8081}))
(def *default-bucket* "evening")

(defn- kv-get [key]
  (if-let [response (riak/get *riak-connection* *default-bucket* key)]
    (response :value)))

(defn- kv-set! [key value]
  (riak/put *riak-connection* *default-bucket* key
            {:value value :content-type "text/plain"}))

(defn- kv-get-str [key]
  (if-let [value (kv-get key)]
    (String. value)))

(defn- kv-set-str! [key value]
  (kv-set! key (.getBytes value)))

;;;;;;;;;;;;;;; Storing documents as B+ trees in Riak ;;;;;;;;;;;;;;;;

(defprotocol RiakChunk
  "A chunk that can be stored in Riak"
  (store-chunk! [chunk] "Store chunk in Riak, returning non-dirty chunk"))

;;; A leaf node has a name (its Riak id) and an array of data.
(defrecord LeafNode [^String name ^bytes data ^boolean dirty?]
  RiakChunk (store-chunk! [this] (store-leaf! this)))
;;; A branch node has a name and a clojure vector of child nodes.
(defrecord BranchNode [^String name children ^boolean dirty?]
  RiakChunk (store-chunk! [this] (store-branch! this)))

;;; Some example nodes
(def foo-leaf (LeafNode. "foo-leaf" (.getBytes "Foo is the word here") true))
(def bar-leaf (LeafNode. "bar-leaf" (.getBytes "We are the bar flies") false))
(def fubar-branch (BranchNode. "fubar-branch" [foo-leaf bar-leaf] true))

(defn- store-leaf!
  "Store a leaf node in Riak, overwriting any existing value."
  [leaf]
  (if (:dirty? leaf)
    (do (kv-set! (:name leaf) (:data leaf))
        (assoc leaf :dirty? false))
    leaf))

(defn- get-leaf
  "Get a leaf node from Riak."
  [name]
  (if-let [data (kv-get name)]
    (LeafNode. name data false)))

(defn- store-branch!
  "Store a branch node in Riak, along with any dirty child
  nodes. Return the non-dirty node."
  [branch]
  (if (:dirty? branch)
    (let [clean (assoc branch
                  :dirty? false
                  :children (to-array (map store-chunk! (:children branch))))]
      (kv-set! (:name branch)
               (.getBytes (json-str (map #(str (if (= (type %) LeafNode) \L \B) (:name %))
                                     (:children branch)))))
      clean)
    branch))

(defn- get-branch
  "Recursively get a branch node from Riak, along with its children."
  [name]
  (when-let [data (kv-get name)]
    (letfn [(is-leaf? [x] (= (first x) \L))
            (get-name [x] (subs x 1))]
      (let [child-names (read-json (String. data))
            children (map #(if (is-leaf? %)
                             (get-leaf (get-name %))
                             (get-branch (get-name %)))
                          child-names)]
        (BranchNode. name (vec children) false)))))
