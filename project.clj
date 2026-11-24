(defproject pgcustard-collector "0.1.2"
  :license {:name "Apache-2.0" :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :main ^:skip-aot pgcustard.runner

  :dependencies [[org.clojure/clojure "1.12.7"]
                 [org.clojure/data.json "2.5.2"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [org.postgresql/postgresql "42.7.15" :exclusions [org.checkerframework/checker-qual]]
                 [com.taoensso/timbre "6.8.0"]])
