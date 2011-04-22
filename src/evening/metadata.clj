(ns evening.metadata
  (:import [redis.clients.jedis Jedis JedisPool])
  (:use [clojure.data.json :only (json-str read-json)]))

;;;;;;;;;;;;;;;;;;;;; Attaching metadata to keys ;;;;;;;;;;;;;;;;;;;;;

;;; Various things may need metadata, and the access to this metadata
;;; should be fast. For this reason, rather than storing it in Riak,
;;; we use Redis for its speed.
;;;
;;; One thing: in the Redis configuration, you need to set the
;;; connection timeout to 0, or there will be broken connections in
;;; the connection pool.
;;;
;;; TODO: When Jedis gets auto-reconnection support, definitely
;;; upgrade. Until then, set things up so that having a thread fail is
;;; not a show-stopper.

(defn new-redis-server "Set the Redis server address" [host port]
  (JedisPool. host port))

(def *redis-server* (new-redis-server "127.0.0.1" 6379))

(defmacro with-redis [redis & body]
  `(let [~redis (.getResource *redis-server*)]
     (try (do ~@body)
          (finally (.returnResource *redis-server* ~redis)))))

(defn ping "Is the metadata server up and responding?" []
  (with-redis redis
    (boolean (.ping redis))))

(defn- decode-value [val]
  (if val (read-json val)))

(defn- map-keys-vals [fkeys fvals m]
  (zipmap (map fkeys (keys m))
          (map fvals (vals m))))

(defn set-metadata!
  "Set some fields of the metadata for a given key. The values must
  come in the form of a map. The keys of the map will be turned into
  strings with clojure.core/name, and the values will be JSON
  encoded."  
  [key metadata-map]
  (with-redis redis
    (.hmset redis key (map-keys-vals name json-str metadata-map))))

(defn get-metadata
  "Get the metadata for a given key. If one or more metadata fields
  are given, then a seq of their values will be returned; otherwise
  this function will return a map of all the metadata."
  [key & fields]
  (with-redis redis
    (if (nil? fields)
        (let [hashmap (.hgetAll redis key)]
          (zipmap (keys hashmap) (map decode-value (vals hashmap))))
      (map decode-value (.hmget redis key (into-array String (map name fields)))))))

(defn get-metadata-single
  "Get a single metadata value for a given key-field pair."
  [key field]
  (with-redis redis
    (decode-value (.hget redis key (name field)))))

(defn set-metadata-single
  "Set a single metadata value for a given key-field pair."
  [key field val]
  (with-redis redis
    (.hset redis key (name field) (json-str val))))
