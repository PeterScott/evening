(defproject evening "0.0.1-SNAPSHOT"
  :description "Realtime synchronized text document server"
  :url "https://github.com/PeterScott/evening"
  :license {:name "GNU General Public License, version 3"
            :url "http://www.gnu.org/licenses/gpl-3.0.html"}
  :repositories {"msgpack.org" "http://msgpack.org/maven2/"}
  :dependencies [[org.clojure/clojure "1.3.0-master-SNAPSHOT"]
                 [org.clojure/data.json "0.1.0"]
                 [org.clojars.ossareh/clj-riak "0.1.0-SNAPSHOT"]
                 [redis.clients/jedis "1.5.3-SNAPSHOT"]
                 [org.msgpack/msgpack "0.5.2-devel"]])
