(ns evening.serdes
  (:import [org.msgpack Packer Unpacker]))

;;; Serialization and deserialization are accomplished using
;;; MessagePack's Packer and Unpacker classes. This module contains
;;; convenience utilities for working with them.

(defmacro with-pack-to-bytes
  "Bind a Packer backed by a byte[] array, and return the byte array."
  [packer & body]
  `(let [out-stream# (java.io.ByteArrayOutputStream.)
         ~packer (Packer. out-stream#)]
     (do ~@body)
     (.toByteArray out-stream#)))

(defmacro with-unpack-from-bytes
  "Provide an unpacker backed by a byte[] array."
  [[unpacker bytes] & body]
  `(let [~unpacker (Unpacker. (java.io.ByteArrayInputStream. ~bytes))]
     ~@body))

(defn pack-then-unpack
  "Pack x with pack-fun, then unpack with unpack-fun. For testing."
  [pack-fun unpack-fun x]
  (let [packed (with-pack-to-bytes p (pack-fun x p))]
    (with-unpack-from-bytes [up packed]
      (unpack-fun up))))
