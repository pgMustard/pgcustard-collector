(ns pgcustard.payload-test
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [pgcustard.payload :as payload])
  (:import [java.sql SQLException Timestamp]
           [java.time Instant]))

(def ^:private collected-at (Timestamp/from (Instant/parse "2026-11-12T16:49:16.5Z")))

(defn- sent [collected]
  (json/read-str (payload/encode collected-at collected) :key-fn keyword))

(deftest timestamps-keep-their-microseconds
  (let [analyzed (Timestamp/from (Instant/parse "2026-11-12T16:49:16.123456Z"))]
    (is (= "2026-11-12T16:49:16.123456Z" (get-in (sent {:tables {:sizes [{:last_analyze analyzed}]}})
                                                 [:tables :sizes 0 :last_analyze])))
    (is (= "2026-11-12T16:49:16.500Z" (:collected_at (sent {}))))))

(deftest queryids-travel-as-strings
  (testing "since JSON readers round integers past 2^53"
    (let [wire (sent {:query_texts [{:queryid -1158905337641546873 :query "select $1"}]
                      :queries     [{:queryid 9007199254740993 :calls 3}]
                      :database    {:optional {:query_id 6264675695848577039}}})]
      (is (= "9007199254740993" (get-in wire [:queries 0 :queryid])))
      (is (= "-1158905337641546873" (get-in wire [:query_texts 0 :queryid])))
      (is (= "6264675695848577039" (get-in wire [:database :optional :query_id]))))))

(deftest text-travels-unescaped
  (let [text "select * from café where path = '/tmp'"]
    (is (str/includes? (payload/encode collected-at {:query_texts [{:queryid 1 :query text}]}) text))
    (is (= text (get-in (sent {:query_texts [{:queryid 1 :query text}]}) [:query_texts 0 :query])))))

(deftest a-failure-travels-as-its-section-message-and-sqlstate
  (is (= {:section "tables" :message "canceling statement" :sqlstate "57014"}
         (:failure (sent {:failure {:section "tables"
                                    :error   (SQLException. "canceling statement" "57014")}}))))
  (is (nil? (:failure (sent {})))))

(deftest a-payload-says-whether-the-database-could-be-reached
  (is (true? (:reachable (sent {}))))
  (is (false? (:reachable (sent {:reachable false})))))

(deftest a-payload-names-its-version
  (is (= payload/version (:payload_version (sent {})))))

(deftest the-envelope-leads-the-document
  (is (= ["payload_version" "collector_version" "collected_at" "failure" "reachable" "query_texts" "database" "tables" "indexes"
          "queries"]
         (map second (re-seq #"\"(\w+)\":" (payload/encode collected-at {:query_texts [] :database {} :tables {}
                                                                          :indexes [] :queries []}))))))
