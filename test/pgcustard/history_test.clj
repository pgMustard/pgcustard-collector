(ns pgcustard.history-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [pgcustard.collector-fixtures :refer [with-temp-dir]]
            [pgcustard.history :as history])
  (:import [java.sql Timestamp]
           [java.time Duration Instant]))

(def ^:private start (Instant/parse "2026-11-01T00:00:00Z"))

(defn- days-later [days]
  (.plus start (Duration/ofDays days)))

(def ^:private reset-at (Timestamp/from (Instant/parse "2026-10-01T00:00:00.123456Z")))

(defn- row [queryid calls]
  {:queryid queryid :calls calls :stats_since reset-at})

(defn- frozen [state at rows]
  ((:frozen-queryids (history/lookups state at)) rows))

(defn- missing [state at queryids]
  ((:missing-query-texts (history/lookups state at)) queryids))

(defn- unresolved [state at]
  ((:unresolved-xmin-queryids (history/lookups state at))))

(defn- holding [queryid]
  {:database {:optional {:query_id queryid}}})

(deftest with-no-history-every-collection-is-a-first-poll
  (is (= #{} (frozen history/empty-state start [(row 1 5)])))
  (is (= [1 2] (missing history/empty-state start [1 2])))
  (is (empty? (unresolved history/empty-state start))))

(deftest a-query-freezes-after-a-week-without-new-calls
  (let [state (history/record history/empty-state start {:queries [(row 1 5) (row 2 5)]})]
    (is (= #{} (frozen state (days-later 6) [(row 1 5)])))
    (is (= #{1} (frozen state (days-later 7) [(row 1 5) (row 2 6)])))
    (testing "a new call starts the week again"
      (let [called (history/record state (days-later 7) {:queries [(row 1 6)]})]
        (is (= #{} (frozen called (days-later 13) [(row 1 6)])))
        (is (= #{1} (frozen called (days-later 14) [(row 1 6)])))))
    (testing "and so does a statistics reset, even at the same count"
      (is (= #{} (frozen state (days-later 8) [(assoc (row 1 5) :stats_since (Timestamp/from start))]))))
    (testing "only rows that were tracked count, since only those were stored"
      (is (= #{} (frozen state (days-later 8) [(row 3 5)]))))))

(deftest a-sent-text-is-not-sent-again-for-a-day
  (let [state (history/record history/empty-state start {:query_texts [{:queryid 1 :query "select 1"}]})]
    (is (= [2] (missing state (.plus start (Duration/ofHours 23)) [1 2])))
    (is (= [1 2] (missing state (days-later 1) [1 2])))))

(deftest an-xmin-holder-is-looked-up-until-its-text-is-sent
  (let [held (history/record history/empty-state start (holding 7))]
    (is (= [7] (unresolved held (days-later 1))))
    (testing "its text arriving resolves it"
      (is (empty? (unresolved (history/record held (days-later 1) {:query_texts [{:queryid 7 :query "select 7"}]})
                              (days-later 1)))))
    (testing "a week after it was last seen it's forgotten"
      (is (= [7] (unresolved (history/record held (days-later 6) {}) (days-later 6))))
      (is (empty? (unresolved (history/record held (days-later 7) {}) (days-later 7)))))
    (testing "being seen again keeps it"
      (let [seen-again (history/record held (days-later 5) (holding 7))]
        (is (= [7] (unresolved (history/record seen-again (days-later 10) {}) (days-later 10))))))))

(deftest counters-are-forgotten-as-the-server-forgets-them
  (let [state (history/record history/empty-state start {:queries [(row 1 5)]})]
    (is (contains? (:counters (history/record state (days-later 99) {})) 1))
    (is (not (contains? (:counters (history/record state (days-later 100) {})) 1)))))

(deftest history-survives-a-restart
  (with-temp-dir
    (fn [dir]
      (let [file  (io/file dir "history.json")
            state (history/record history/empty-state start
                                  {:queries     [(row 9007199254740993 5) {:queryid -3 :calls 1 :stats_since nil}]
                                   :query_texts [{:queryid -1158905337641546873 :query "select $1"}]
                                   :database    {:optional {:query_id 6264675695848577039}}})]
        (history/save! file state)
        (is (= state (history/load-state file)))))))

(deftest unreadable-history-is-set-aside
  (with-temp-dir
    (fn [dir]
      (let [file (io/file dir "history.json")]
        (testing "a missing file is a first poll"
          (is (= history/empty-state (history/load-state file))))
        (doseq [contents ["{\"history_version\": 99, \"texts\": {}}" "not json"]]
          (spit file contents)
          (is (= history/empty-state (history/load-state file)))
          (is (not (.exists file)))
          (is (= contents (slurp (io/file dir "history.json.old")))))))))
