(ns evening.core
  (:use [clojure.contrib.json :only (json-str read-json)])
  (:import (com.trifork.riak RiakClient RiakObject RiakLink
                             RequestMeta BucketProperties))
  (:require [clojure.contrib.string :as string])
  (:require [clj-riak.client :as riak]))

;;;;;;;;;;;;;;;;;;;;;;; Riak server connection ;;;;;;;;;;;;;;;;;;;;;;;

;;; Riak connection parameters. Change these to fit your installation.
(def *riak-connection* (riak/init {:host "127.0.0.1" :port 8081}))
(def *default-bucket* "evening")

(defn- kv-get [key]
  (if-let [response (riak/get *riak-connection* *default-bucket* key)]
    response))

(defn- kv-set! [key value]
  (let [#^RiakObject ro (RiakObject. *default-bucket* key value)
        #^RequestMeta rm (RequestMeta.)]
    (.store *riak-connection* ro rm)))

(defn- kv-get-str [key]
  (if-let [value (:value (kv-get key))]
    (String. value)))

(defn- kv-set-str! [key value]
  (kv-set! key (.getBytes value)))

;;;;;;;;;;;;;;; Storing documents as B+ trees in Riak ;;;;;;;;;;;;;;;;

(defprotocol RiakChunk
  "A chunk that can be stored in Riak"
  (store-chunk! [chunk] "Store chunk in Riak, returning non-dirty chunk"))

;;; Leaf nodes are relatively easy: we just store a byte array, along
;;; with the name and its width, in atoms. The width is prepended to
;;; the byte array, in the format "width:bytes", where width is an
;;; integer in ASCII format.

(defrecord LeafNode
    [^String name                       ; Name of the node
     ^bytes data                        ; Atom data for this leaf
     ^boolean dirty?                    ; Is this leaf dirty?
     ^long width])                      ; Width of leaf, in atoms

(defn- serialize-leaf [leaf]
  (.getBytes (str (:width leaf) ":" (String. (:data leaf)))))

(defn- unserialize-leaf [bytes]
  (let [str (String. bytes)
        [numstr content] (string/split #":" 2 str)]
    [(Integer/parseInt numstr) (.getBytes content)]))

(defn- store-leaf!
  "Store a leaf node in Riak, overwriting any existing value."
  [leaf]
  (if (:dirty? leaf)
    (do (kv-set! (:name leaf) (serialize-leaf leaf))
        (assoc leaf :dirty? false))
    leaf))

(extend-type LeafNode
  RiakChunk (store-chunk! [this] (store-leaf! this)))

(defn- get-leaf
  "Get a leaf node from Riak."
  [name]
  (if-let [response (kv-get name)]
    (let [[width data] (unserialize-leaf (:value response))]
     (LeafNode. name data false width))))

;;; Branch nodes are trickier. We need to store the names of the child
;;; nodes, whether those child nodes are branches or leaves, and the
;;; widths of the subsequences of to each child, for O(lg n) random
;;; lookup. We should also store the sum of those widths.
;;;
;;; The children of a branch node are a vector of child nodes. They
;;; are serialized to a colon-separated series of "[BL]name" strings
;;; for database storage. The width is read from the child nodes once
;;; they're loaded. Since a document is either entirely loaded into
;;; memory or entirely not loaded (i.e. no partial tree reads), this
;;; is okay.

(defrecord BranchNode
    [^String name                       ; Name of the node
     children                           ; Vector of children
     ^boolean dirty?                    ; Is this subtree dirty?
     ^long width])                      ; Width of subtree, in atoms

(defn- parse-branch-serialization
  "Given a child spec string (e.g. 'Bfoo:Lbar') parse that into a seq
  of [name is-branch?] chunks (e.g. (['foo' true] ['bar' false]))."
  [str]
  (map #(vector (subs % 1) (= (first %) \B))
       (filter #(< 0 (.length %)) (string/split #":" str))))

(defn- unparse-branch-serialization
  "Generate child spec string from a vector of child chunks"
  [children]
  (apply str (interpose ":" (map #(str (if (= (type %) BranchNode) "B" "L")
                                       (:name %))
                                 children))))

(defn- store-branch!
  "Store a branch node in Riak, along with any dirty child nodes.
  Return the non-dirty node."
  [branch]
  (if (:dirty? branch)
    (let [clean (assoc branch
                  :dirty? false
                  :children (vec (map store-chunk! (:children branch))))]
      (kv-set! (:name branch)
               (.getBytes (unparse-branch-serialization (:children branch))))
      clean)
    branch))

(extend-type BranchNode
  RiakChunk (store-chunk! [this] (store-branch! this)))

(defn- get-branch
  "Recursively get a branch node from Riak, along with its children."
  [name]
  (when-let [data (kv-get name)]
    (let [child-spec (parse-branch-serialization (String. (:value data)))
          children (for [[child-name is-branch?] child-spec]
                     (if is-branch? (get-branch child-name) (get-leaf child-name)))
          width (reduce + (map :width children))]
      (BranchNode. name (vec children) false width))))


;;; Debugging

;;; Some example nodes
(def foo-leaf (LeafNode. "foo-leaf" (.getBytes "Foo is the word here") true 20))
(def bar-leaf (LeafNode. "bar-leaf" (.getBytes "We are the bar flies") false 20))
(def fubar-branch (BranchNode. "fubar-branch"
                               [foo-leaf bar-leaf]
                               true 40))

;;; For getting rid of database debugging cruft
(defn- delete-all-keys []
  (doseq [key (riak/list-keys *riak-connection* *default-bucket*)]
    (riak/delete *riak-connection* *default-bucket* key)))

(defn- show-all-kv-pairs []
  (for [key (riak/list-keys *riak-connection* *default-bucket*)]
    [key (->> (riak/get *riak-connection* *default-bucket* key)
              :value String.)]))
