(ns pgcustard.collect-test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [pgcustard.collect :as collect]
            [pgcustard.collector-fixtures :refer [connection-from-url test-postgres-url]]
            [pgcustard.postgres :as postgres]))

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
                collect/xmin-query-texts (fn [_ _ _] [{:queryid 1 :query "begin"}])
                collect/tables           (fn [_ _] {:sizes [] :shapes []})
                collect/indexes          (fn [_ _ _] [])
                collect/queries          (fn [_ _ _] {:rows [{:queryid 2}] :texts [{:queryid 2 :query "select $1"}]})]
    (with-redefs-fn overrides
      #(collect/collection :read-db {}))))

(deftest a-collection-gathers-every-section
  (let [collected (collection-with {})]
    (is (= #{:database :tables :indexes :queries :query_texts} (set (keys collected))))
    (testing "texts for xmin holders and top queries travel together"
      (is (= [1 2] (map :queryid (:query_texts collected)))))))

(deftest a-failing-section-ends-the-collection
  (let [failure   (java.sql.SQLException. "canceling statement" "57014")
        collected (collection-with {#'collect/tables (fn [_ _] (throw failure))})]
    (testing "earlier sections are kept and later ones never run"
      (is (some? (:database collected)))
      (is (not-any? #(contains? collected %) [:tables :indexes :queries])))
    (testing "the failure names its section"
      (is (= {:section "tables" :error failure} (:failure collected))))))

(deftest a-failed-xmin-text-lookup-leaves-the-collection-going
  (let [collected (collection-with {#'collect/xmin-query-texts (fn [_ _ _] (throw (Exception. "gone")))})]
    (is (nil? (:failure collected)))
    (is (= [2] (map :queryid (:query_texts collected))))))

(deftest query-reads-reuse-the-database-snapshots-pg-stat-statements-checks
  (let [snapshot {:pgss_available true :pgss_toplevel true :pgss_stats_since false}
        seen     (atom [])]
    (collection-with {#'collect/database         (fn [_] {:snapshot snapshot})
                      #'collect/xmin-query-texts (fn [_ pgss _] (swap! seen conj pgss) [])
                      #'collect/queries          (fn [_ pgss _] (swap! seen conj pgss) {:rows [] :texts []})})
    (is (= [snapshot snapshot] @seen))))

(deftest query-texts-leave-without-their-literals
  (with-redefs [jdbc/query (fn [_ _] [{:queryid 1 :query "alter role alice password 'hunter2'"}
                                      {:queryid 2 :query nil}])]
    (is (= [{:queryid 1 :query "alter role alice password $1"} {:queryid 2 :query nil}]
           (#'collect/query-texts :read-db true [1 2])))))

(defn- by-table-in [schema rows]
  (into {}
        (comp (filter #(= schema (:schema_name %)))
              (map (juxt :table_name identity)))
        rows))

(deftest a-locked-table-is-skipped-rather-than-waited-for
  (if-not test-postgres-url
    (is (nil? (System/getenv "CI")) "CI needs PGCUSTARD_TEST_POSTGRES_URL")
    (let [db-spec (postgres/data-source (connection-from-url test-postgres-url))
          schema  (str "lock_test_" (System/nanoTime))]
      (try
        (doseq [sql [(str "create schema " schema)
                     (str "create table " schema ".locked (id int primary key)")
                     (str "create table " schema ".free (id int primary key)")
                     (str "create table " schema ".parted (id int) partition by range (id)")
                     (str "create table " schema ".part_locked partition of " schema ".parted for values from (0) to (10)")
                     (str "create table " schema ".part_free partition of " schema ".parted for values from (10) to (20)")]]
          (jdbc/execute! db-spec [sql]))
        (jdbc/with-db-transaction [holder db-spec]
          (jdbc/execute! holder [(str "lock table " schema ".locked, " schema ".part_locked in access exclusive mode")])
          (with-open [connection (jdbc/get-connection db-spec)]
            (let [read-db {:connection connection}
                  _       (collect/configure-session! read-db)
                  tables  (by-table-in schema (:sizes (collect/tables read-db false)))
                  indexes (by-table-in schema (collect/indexes read-db false []))]
              (testing "the locked table is marked, without sizes"
                (is (true? (get-in tables ["locked" :locked])))
                (is (every? nil? (map (tables "locked") [:heap_bytes :index_bytes :toast_bytes]))))
              (testing "its index is marked, without a size or definition"
                (is (true? (get-in indexes ["locked" :locked])))
                (is (nil? (get-in indexes ["locked" :index_bytes])))
                (is (nil? (get-in indexes ["locked" :index_def]))))
              (testing "a partitioned table with a locked partition is marked, without sizes"
                (is (true? (get-in tables ["parted" :locked])))
                (is (nil? (get-in tables ["parted" :heap_bytes]))))
              (testing "other tables are measured as usual"
                (is (false? (get-in tables ["free" :locked])))
                (is (some? (get-in tables ["free" :heap_bytes])))
                (is (some? (get-in indexes ["free" :index_def])))
                (is (false? (get-in tables ["part_free" :locked])))
                (is (some? (get-in tables ["part_free" :heap_bytes])))))))
        (finally
          (jdbc/execute! db-spec [(str "drop schema if exists " schema " cascade")]))))))

(def ^:private timescaledb-catalog
  ["create schema _timescaledb_catalog"
   "create table _timescaledb_catalog.hypertable (id int, schema_name name, table_name name, compression_state int)"
   "create table _timescaledb_catalog.chunk (id int, hypertable_id int)"
   "create table _timescaledb_catalog.compression_settings (relid regclass, compress_relid regclass)"
   "create table _timescaledb_catalog.compression_chunk_size (chunk_id int, numrows_pre_compression bigint)"
   "create table _timescaledb_catalog.continuous_agg (mat_hypertable_id int, user_view_schema name, user_view_name name)"
   "create schema _timescaledb_internal"
   "create table _timescaledb_internal.bgw_job_stat (job_id int primary key)"
   "create table _timescaledb_internal.bgwriter_notes (id int primary key)"])

(defn- hypertable-sql [schema]
  (concat
    [(str "create schema " schema)
     (str "create table " schema ".readings (junk int, device int, at int, value int)")
     (str "create index readings_device_at on " schema ".readings (device, at)")
     (str "create index readings_twin on " schema ".readings (device, at)")
     (str "create index readings_high on " schema ".readings (device) where value > 10")
     (str "create index readings_low on " schema ".readings (device) where value < 5")
     (str "alter table " schema ".readings drop column junk")
     (str "create table " schema ".plain (id int primary key)")
     (str "create table " schema ".readings_empty (at int)")
     (str "create table " schema ".chunk_1_compressed (batch int)")
     (str "insert into " schema ".chunk_1_compressed select generate_series(1, 3)")
     (str "insert into _timescaledb_catalog.hypertable values (1, '" schema "', 'readings', 0), (2, '" schema "', 'readings_empty', 0)")
     (str "insert into _timescaledb_catalog.compression_chunk_size values (1, 4000)")]
    (mapcat (fn [n]
              (let [chunk (str schema ".chunk_" n)]
                [(str "create table " chunk " () inherits (" schema ".readings) with (autovacuum_enabled = off)")
                 (str "create index chunk_" n "_readings_device_at on " chunk " (device, at)")
                 (str "create index chunk_" n "_readings_high on " chunk " (device) where value > '10'")
                 (str "create index chunk_" n "_readings_low on " chunk " (device) where value < '5'")
                 (str "insert into " chunk " select g % 7, g, g % 20 from generate_series(1, " (* 1500 n) ") g")
                 (str "insert into _timescaledb_catalog.chunk values (" n ", 1)")
                 "select pg_stat_force_next_flush()"
                 (str "vacuum analyze " chunk)]))
            (range 1 6))
    [(str "insert into _timescaledb_catalog.compression_settings values ('" schema ".chunk_1', '" schema ".chunk_1_compressed')")
     (str "delete from " schema ".chunk_2 where at <= 1200")
     "select pg_stat_force_next_flush()"]))

(defn- continuous-aggregate-sql [internal views]
  [(str "create schema " internal)
   (str "create table " internal "._materialized_hypertable_3 (bucket int, device int)")
   (str "create index _materialized_hypertable_3_bucket_idx on " internal "._materialized_hypertable_3 (bucket)")
   (str "create table " internal ".chunk_6 () inherits (" internal "._materialized_hypertable_3)")
   (str "create index chunk_6__materialized_hypertable_3_bucket_idx on " internal ".chunk_6 (bucket)")
   (str "insert into " internal ".chunk_6 select g, g % 7 from generate_series(1, 300) g")
   (str "insert into _timescaledb_catalog.hypertable values (3, '" internal "', '_materialized_hypertable_3', 0)")
   "insert into _timescaledb_catalog.chunk values (6, 3)"
   (str "insert into _timescaledb_catalog.continuous_agg values (3, '" views "', 'readings_hourly')")
   (str "vacuum analyze " internal ".chunk_6")])

(defn- statements! [connection statements]
  (with-open [statement (.createStatement ^java.sql.Connection connection)]
    (doseq [^String sql statements] (.execute statement sql))))

(deftest a-hypertable-is-collected-as-the-sum-of-its-chunks
  (if-not test-postgres-url
    (is (nil? (System/getenv "CI")) "CI needs PGCUSTARD_TEST_POSTGRES_URL")
    (let [db-spec (postgres/data-source (connection-from-url test-postgres-url))
          schema  (str "hypertable_test_" (System/nanoTime))
          internal (str schema "_internal")
          views    (str schema "_views")]
      (with-open [connection (jdbc/get-connection db-spec)]
        (try
          (statements! connection (concat timescaledb-catalog (hypertable-sql schema)
                                          (continuous-aggregate-sql internal views)
                                          [(str "update pg_index set indisvalid = false"
                                                " where indexrelid = '" schema ".chunk_4_readings_device_at'::regclass")]))
          (let [read-db    {:connection connection}
                _          (collect/configure-session! read-db)
                available? (get-in (collect/database read-db) [:snapshot :hypertables_available])
                collected  (collect/tables read-db true)
                tables     (by-table-in schema (:sizes collected))
                all-indexes (collect/indexes read-db true (:sizes collected))
                indexes    (into {}
                                 (comp (filter #(= schema (:schema_name %)))
                                       (map (juxt :index_name identity)))
                                 all-indexes)
                sizes-of   (fn [k names] (reduce + (map #(get-in indexes [% k]) names)))
                hypertable (tables "readings")]
            (testing "the catalog's presence turns the roll-up on"
              (is (true? available?)))
            (testing "the chunk with dead rows is tracked, with the largest filling the other slots"
              (is (= #{"readings" "readings_empty" "plain" "chunk_2" "chunk_4" "chunk_5"} (set (keys tables))))
              (is (= #{"readings" "readings_empty" "plain" "chunk_2" "chunk_4" "chunk_5"}
                     (set (keys (by-table-in schema (:shapes collected)))))))
            (testing "a tracked chunk names its hypertable"
              (is (= [schema "readings"] ((juxt :parent_schema :parent_table) (tables "chunk_5"))))
              (is (nil? (:parent_table (tables "plain")))))
            (testing "the hypertable counts every chunk, tracked or not"
              (is (= 5 (:chunk_count hypertable)))
              (is (= (* 1500 15) (:chunk_tup_ins hypertable)))
              (is (= 1200 (:chunk_tup_del hypertable)))
              (is (> (:heap_bytes hypertable) (+ (:heap_bytes (tables "chunk_4")) (:heap_bytes (tables "chunk_5"))))))
            (testing "compressed rows are counted as they were before compression"
              (is (== (+ 4000 (* 1500 15)) (:catalog_rows hypertable))))
            (testing "a hypertable without chunks is still one"
              (is (= 0 (:chunk_count (tables "readings_empty")))))
            (testing "an ordinary table has no chunks"
              (is (nil? (:chunk_count (tables "plain")))))
            (testing "a chunk's index names the hypertable index it belongs to, however its predicate is written"
              (is (= "readings_high" (get-in indexes ["chunk_5_readings_high" :hypertable_index])))
              (is (= "readings_low" (get-in indexes ["chunk_5_readings_low" :hypertable_index])))
              (is (= "readings_device_at" (get-in indexes ["chunk_5_readings_device_at" :hypertable_index]))))
            (testing "a hypertable's index is the size of every chunk's"
              (is (> (get-in indexes ["readings_device_at" :index_bytes])
                     (sizes-of :index_bytes ["chunk_2_readings_device_at" "chunk_4_readings_device_at"
                                             "chunk_5_readings_device_at"])))
              (is (= (sizes-of :idx_scan ["chunk_2_readings_device_at" "chunk_4_readings_device_at"
                                           "chunk_5_readings_device_at"])
                     (get-in indexes ["readings_device_at" :chunk_idx_scan]))))
            (testing "exact duplicates share the one index each chunk has"
              (is (= (get-in indexes ["readings_device_at" :index_bytes])
                     (get-in indexes ["readings_twin" :index_bytes]))))
            (testing "an index left invalid on one chunk doesn't make the hypertable's invalid"
              (is (false? (get-in indexes ["chunk_4_readings_device_at" :is_valid])))
              (is (true? (get-in indexes ["readings_device_at" :is_valid]))))
            (testing "an ordinary table's index isn't a hypertable's"
              (is (nil? (get-in indexes ["plain_pkey" :chunk_idx_scan]))))
            (testing "a continuous aggregate goes by its own name, and its chunk names it as the parent"
              (is (= #{"readings_hourly"} (set (keys (by-table-in views (:sizes collected))))))
              (is (= 1 (get-in (by-table-in views (:sizes collected)) ["readings_hourly" :chunk_count])))
              (is (= #{"readings_hourly"} (set (keys (by-table-in views (:shapes collected))))))
              (is (= [views "readings_hourly"]
                     ((juxt :parent_schema :parent_table) (get (by-table-in internal (:sizes collected)) "chunk_6"))))
              (is (= [views "readings_hourly"]
                     (->> all-indexes
                          (filter #(= "_materialized_hypertable_3_bucket_idx" (:index_name %)))
                          (map (juxt :schema_name :table_name))
                          first)))
              (is (not-any? #(= "_materialized_hypertable_3" (:table_name %)) (:sizes collected))))
            (testing "TimescaleDB's job tables are left out, and nothing else in its internal schema"
              (is (= #{"bgwriter_notes"} (set (keys (by-table-in "_timescaledb_internal" (:sizes collected))))))
              (is (= #{"bgwriter_notes"} (set (keys (by-table-in "_timescaledb_internal" (:shapes collected))))))
              (is (= #{"bgwriter_notes"} (set (keys (by-table-in "_timescaledb_internal" all-indexes)))))))
          (finally
            (statements! connection ["drop schema if exists _timescaledb_catalog cascade"
                                     "drop schema if exists _timescaledb_internal cascade"
                                     (str "drop schema if exists " schema " cascade")
                                     (str "drop schema if exists " internal " cascade")])))))))
