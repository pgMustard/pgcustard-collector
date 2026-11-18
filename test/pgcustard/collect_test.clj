(ns pgcustard.collect-test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [pgcustard.collect :as collect]))

(deftest capture-fetches-more-than-it-stores
  (testing "headroom keeps excluded entries visible so they can be readmitted"
    (is (> @#'collect/top-queries-considered @#'collect/top-queries-tracked)))
  (testing "the fetch limit is the considered count, not the tracked count"
    (is (str/includes? (collect/top-queries-query true true)
                       (str "limit " @#'collect/top-queries-considered)))))

(deftest top-queries-skip-statements-hidden-from-the-role
  (testing "without pg_monitor, other roles' statements arrive with a null queryid, which query_metrics refuses"
    (doseq [toplevel? [true false]
            stats-since? [true false]]
      (is (str/includes? (collect/top-queries-query toplevel? stats-since?)
                         "and queryid is not null")))))

(deftest xmin-holder-query-captures-the-queryid-not-the-text
  (testing "the raw statement is never read, so no literal values are stored"
    (doseq [query-id? [true false]]
      (is (not (re-find #"a\.query\b(?!_id)" (collect/xmin-holder-query true query-id?))))))
  (testing "query_id only exists from Postgres 14"
    (is (str/includes? (collect/xmin-holder-query true true) "a.query_id"))
    (is (not (str/includes? (collect/xmin-holder-query true false) "a.query_id")))))

(deftest optional-database-query-reads-only-what-exists
  (testing "pg_stat_statements is left out when it isn't available"
    (is (not (str/includes? (collect/optional-database-query 180000 {:pgss? false}) "pg_stat_statements"))))
  (testing "pg_stat_statements_info is left out where it doesn't exist"
    (is (not (str/includes? (collect/optional-database-query 180000 {:pgss? true :pgss-info? false})
                            "pg_stat_statements_info")))
    (is (str/includes? (collect/optional-database-query 180000 {:pgss? true :pgss-info? true})
                       "pg_stat_statements_info")))
  (testing "the checkpoint view follows the server version"
    (is (str/includes? (collect/optional-database-query 160000 {}) "pg_stat_bgwriter"))
    (is (str/includes? (collect/optional-database-query 170000 {}) "pg_stat_checkpointer"))))

(defn- collection-with [overrides]
  (with-redefs [collect/database         (fn [_] {:snapshot {:idle_sessions 1}})
                collect/xmin-query-texts (fn [_ _] [{:queryid 1 :query "begin"}])
                collect/tables           (fn [_] {:sizes [] :shapes []})
                collect/indexes          (fn [_] [])
                collect/queries          (fn [_ _] {:rows [{:queryid 2}] :texts [{:queryid 2 :query "select $1"}]})]
    (with-redefs-fn overrides
      #(collect/collection :read-db {}))))

(deftest a-collection-gathers-every-section
  (let [collected (collection-with {})]
    (is (= #{:database :tables :indexes :queries :query_texts} (set (keys collected))))
    (testing "texts for xmin holders and top queries travel together"
      (is (= [1 2] (map :queryid (:query_texts collected)))))))

(deftest a-failing-section-ends-the-collection
  (let [failure   (java.sql.SQLException. "canceling statement" "57014")
        collected (collection-with {#'collect/tables (fn [_] (throw failure))})]
    (testing "earlier sections are kept and later ones never run"
      (is (some? (:database collected)))
      (is (not-any? #(contains? collected %) [:tables :indexes :queries])))
    (testing "the failure names its section"
      (is (= {:section "tables" :error failure} (:failure collected))))))

(deftest a-failed-xmin-text-lookup-leaves-the-collection-going
  (let [collected (collection-with {#'collect/xmin-query-texts (fn [_ _] (throw (Exception. "gone")))})]
    (is (nil? (:failure collected)))
    (is (= [2] (map :queryid (:query_texts collected))))))

(deftest query-texts-leave-without-their-literals
  (with-redefs [jdbc/query (fn [_ _] [{:queryid 1 :query "alter role alice password 'hunter2'"}
                                      {:queryid 2 :query nil}])]
    (is (= [{:queryid 1 :query "alter role alice password $1"} {:queryid 2 :query nil}]
           (#'collect/query-texts :read-db true [1 2])))))
