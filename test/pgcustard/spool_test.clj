(ns pgcustard.spool-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [pgcustard.collector-fixtures :refer [with-temp-dir]]
            [pgcustard.spool :as spool])
  (:import [java.io File]
           [java.time Duration Instant]))

(def ^:private start (Instant/parse "2026-11-13T12:00:00.123Z"))

(defn- seconds-later [secs]
  (.plus start (Duration/ofSeconds secs)))

(defn- names [dir]
  (map #(.getName ^File %) (spool/pending dir)))

(deftest payloads-are-listed-oldest-first
  (with-temp-dir
    (fn [dir]
      (doseq [secs [60 0 30]]
        (spool/add! dir (seconds-later secs) (.getBytes (str secs))))
      (spit (io/file dir "notes.txt") "not a payload")
      (is (= ["20261113T120000.123Z.json.gz" "20261113T120030.123Z.json.gz" "20261113T120100.123Z.json.gz"]
             (names dir)))
      (is (= start (spool/collected-at (first (spool/pending dir)))))
      (is (= "60" (slurp (last (spool/pending dir))))))))

(deftest a-half-written-payload-is-never-listed
  (with-temp-dir
    (fn [dir]
      (spit (io/file dir ".123.tmp") "partial")
      (spool/add! dir start (.getBytes "whole"))
      (is (= ["20261113T120000.123Z.json.gz"] (names dir)))
      (testing "and is cleared up at the next start"
        (spool/prepare! dir)
        (is (= ["20261113T120000.123Z.json.gz"] (map #(.getName ^File %) (.listFiles ^File dir))))))))

(deftest payloads-past-the-servers-window-are-dropped
  (with-temp-dir
    (fn [dir]
      (spool/add! dir start (.getBytes "old"))
      (spool/add! dir (seconds-later 1) (.getBytes "newer"))
      (spool/enforce-caps! dir (seconds-later (inc (* 7 24 60 60))) Long/MAX_VALUE)
      (is (= ["20261113T120001.123Z.json.gz"] (names dir))))))

(deftest the-oldest-payloads-go-first-when-the-spool-is-full
  (with-temp-dir
    (fn [dir]
      (doseq [secs [0 30 60]]
        (spool/add! dir (seconds-later secs) (byte-array 100)))
      (spool/enforce-caps! dir (seconds-later 60) 250)
      (is (= ["20261113T120030.123Z.json.gz" "20261113T120100.123Z.json.gz"] (names dir))))))

(deftest only-one-collector-uses-a-directory
  (with-temp-dir
    (fn [dir]
      (let [file (io/file dir "lock")
            held (spool/lock! file)]
        (is (some? held))
        (is (nil? (spool/lock! file)))
        (.close (.channel held))
        (let [again (spool/lock! file)]
          (is (some? again))
          (.close (.channel again)))))))
