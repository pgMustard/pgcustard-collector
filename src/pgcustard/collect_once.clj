(ns pgcustard.collect-once
  (:require [clojure.java.jdbc :as jdbc]
            [pgcustard.collect :as collect]
            [pgcustard.errors :as errors]
            [pgcustard.history :as history]
            [pgcustard.payload :as payload]
            [pgcustard.postgres :as postgres]
            [taoensso.timbre :as log])
  (:import [java.sql Timestamp]
           [java.time Instant]))

(defn collect-first-poll [read-db]
  (collect/configure-session! read-db)
  (collect/collection read-db (history/lookups history/empty-state (Instant/now))))

(defn- collect-from [connection]
  (jdbc/with-db-connection [read-db (postgres/data-source connection)]
    (collect-first-poll read-db)))

(defn print-payload! [connection]
  (let [stdout *out*]
    (binding [*out* *err*]
      (try
        (let [collected-at (Timestamp. (System/currentTimeMillis))
              collected    (collect-from connection)]
          (binding [*out* stdout]
            (println (payload/encode collected-at collected)))
          (if (:failure collected) 1 0))
        (catch Exception e
          (log/error "couldn't collect, so there's nothing to print -" (errors/error-message e))
          1)))))
