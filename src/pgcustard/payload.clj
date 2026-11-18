(ns pgcustard.payload
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [pgcustard.errors :as errors])
  (:import [java.io ByteArrayOutputStream]
           [java.nio.charset StandardCharsets]
           [java.time Duration]
           [java.util Properties]
           [java.util.zip GZIPOutputStream]))

(def version "2026-11-12")

(def ^Duration max-age (Duration/ofDays 7))

(defn- packaged-version []
  (when-let [properties (io/resource "META-INF/maven/pgcustard-collector/pgcustard-collector/pom.properties")]
    (with-open [reader (io/reader properties)]
      (.getProperty (doto (Properties.) (.load reader)) "version"))))

(def collector-version
  (or (System/getProperty "pgcustard-collector.version") (packaged-version)))

(def ^:private leading-keys [:payload_version :collector_version :collected_at :failure :reachable])

(def queryid-keys #{:queryid :query_id})

(defn- encode-value [k v]
  (if (and (queryid-keys k) (some? v))
    (str v)
    v))

(defn- failure-report [{:keys [section error]}]
  {:section  section
   :message  (errors/error-message error)
   :sqlstate (errors/sql-state error)})

(defn encode [collected-at collected]
  (let [document (assoc collected
                        :payload_version   version
                        :collector_version collector-version
                        :collected_at      collected-at
                        :failure           (some-> (:failure collected) failure-report)
                        :reachable         (not (false? (:reachable collected))))]
    (json/write-str
      (apply array-map (mapcat (juxt identity document)
                               (distinct (concat leading-keys (keys collected)))))
      :value-fn encode-value
      :escape-unicode false
      :escape-slash false)))

(defn gzip ^bytes [^String json-text]
  (let [out (ByteArrayOutputStream.)]
    (with-open [zipped (GZIPOutputStream. out)]
      (.write zipped (.getBytes json-text StandardCharsets/UTF_8)))
    (.toByteArray out)))
