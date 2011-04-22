(ns evening.metadata
  (:require [redis.core :as redis])
  (:use [clojure.contrib.json :only (json-str read-json)])
  (:use [clojure.contrib.datalog.util :only (map-values)]))

;;;;;;;;;;;;;;;;;;;;; Attaching metadata to keys ;;;;;;;;;;;;;;;;;;;;;

;;; Various things may need metadata, and the access to this metadata
;;; should be fast. For this reason, rather than storing it in Riak,
;;; we use Redis for its speed.
;;;
;;; One thing: in the Redis configuration, you need to set the
;;; connection timeout to 0, or there will be broken connections in
;;; the connection pool.

(def *redis-server* {:host "127.0.0.1" :port 6379})

(defn ping "Is the metadata server up and responding?" []
  (redis/with-server *redis-server*
    (boolean (redis/ping))))

(defn- decode-value [val]
  (if val (read-json val)))

(defn set-metadata!
  "Set some fields of the metadata for a given key. The values must
  come in the form of a map. The keys of the map will be turned into
  strings with clojure.core/name, and the values will be JSON
  encoded."  
  [key metadata-map]
  (redis/with-server *redis-server*
    (apply redis/hmset key
           (interleave (map name (keys metadata-map))
                       (map json-str (vals metadata-map))))))

(defn get-metadata
  "Get the metadata for a given key. If one or more metadata fields
  are given, then a seq of their values will be returned; otherwise
  this function will return a map of all the metadata."
  [key & fields]
  (redis/with-server *redis-server*
    (if (nil? fields)
      (map-values decode-value (redis/hgetall key))
      (map decode-value (apply redis/hmget key (map name fields))))))

(defn get-metadata-single
  "Get a single metadata value for a given key-field pair."
  [key field]
  (redis/with-server *redis-server*
    (decode-value (redis/hget key (name field)))))

(defn set-metadata-single
  "Set a single metadata value for a given key-field pair."
  [key field val]
  (redis/with-server *redis-server*
    (redis/hset key (name field) (json-str val))))
