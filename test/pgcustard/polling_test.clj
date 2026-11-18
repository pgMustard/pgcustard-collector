(ns pgcustard.polling-test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing]]
            [pgcustard.collector-fixtures :refer [connection-from-url test-postgres-url]]
            [pgcustard.history :as history]
            [pgcustard.polling :as polling]
            [pgcustard.postgres :as postgres])
  (:import [java.net ConnectException]
           [java.sql Connection]
           [java.time Instant]
           [org.postgresql.util PSQLException PSQLState ServerErrorMessage]))

(def ^:private now (Instant/parse "2026-11-13T12:00:00Z"))

(def ^:private collected {:database {:snapshot {:idle_sessions 1}}})

(defn- answering [replies]
  (let [left (atom replies)]
    (fn [_]
      (let [reply (ffirst (swap-vals! left rest))]
        (if (instance? Exception reply) (throw reply) reply)))))

(defn- outcome
  ([collect!] (outcome collect! {} (atom {}) now))
  ([collect! overrides state at]
   (polling/outcome (merge {:collect!       collect!
                            :probe!         (constantly true)
                            :now!           (constantly (.plusSeconds now 2))
                            :retry-pause-ms 0}
                           overrides)
                    state at)))

(deftest a-database-that-cant-be-reached-is-unreachable-after-one-retry
  (let [attempts (atom 0)
        probes   (atom 0)
        result   (outcome (fn [_] (swap! attempts inc) (throw (ConnectException. "Connection refused")))
                          {:probe! #(swap! probes inc)} (atom {}) now)]
    (is (= 2 @attempts))
    (is (false? (:reachable result)))
    (is (= {:failure {:section "connection"} :reachable false}
           (update (polling/reported result) :failure dissoc :error)))
    (testing "a retry that never connected isn't probed, since connecting is the probe"
      (is (zero? @probes)))))

(deftest a-connection-error-that-clears-on-retry-gives-the-retry
  (let [result (outcome (answering [(ConnectException. "Connection refused") collected]))]
    (is (= [(.plusSeconds now 2) collected nil] ((juxt :at :collected :reachable) result)))))

(def ^:private dropped-mid-collection
  (assoc collected :failure {:section "tables"
                             :error   (PSQLException. "An I/O error occurred while sending to the backend."
                                                      PSQLState/CONNECTION_FAILURE)}))

(deftest a-connection-lost-mid-collection-is-unreachable-only-if-the-probe-fails-too
  (doseq [[probe reachable] [[true true] [false false]]]
    (is (= reachable (:reachable (outcome (constantly dropped-mid-collection) {:probe! (constantly probe)}
                                          (atom {}) now))))))

(deftest any-other-failure-is-reported-at-once
  (let [attempts (atom 0)
        failure  (PSQLException. "permission denied for table t" PSQLState/UNKNOWN_STATE)]
    (outcome (fn [_] (swap! attempts inc) (assoc collected :failure {:section "tables" :error failure})))
    (is (= 1 @attempts))))

(defn- refused-login []
  (PSQLException. (ServerErrorMessage. "SFATAL\u0000C28P01\u0000Mpassword authentication failed for user \"u\"\u0000")))

(deftest a-refused-login-is-tried-again-only-every-5-minutes
  (let [attempts (atom 0)
        state    (atom {})
        results  (doall (for [secs [0 30 300]]
                          (outcome (fn [_] (swap! attempts inc) (throw (refused-login))) {} state (.plusSeconds now secs))))]
    (is (= 2 @attempts))
    (testing "the polls in between repeat the refusal without connecting"
      (is (= [nil true nil] (map :held? results)))
      (is (identical? (:error (first results)) (:error (second results)))))))

(deftest one-connection-is-kept-across-collections
  (if-not test-postgres-url
    (is (nil? (System/getenv "CI")) "CI needs PGCUSTARD_TEST_POSTGRES_URL")
    (let [db-spec  (postgres/data-source (connection-from-url test-postgres-url))
          held     (atom nil)
          collect! (polling/collector held db-spec)
          lookups  (history/lookups history/empty-state now)
          pid      #(:pid (first (jdbc/query {:connection @held} ["select pg_backend_pid() as pid"])))]
      (try
        (collect! lookups)
        (let [first-pid (pid)]
          (collect! lookups)
          (is (= first-pid (pid)))
          (jdbc/query db-spec ["select pg_terminate_backend(?::int, 5000)" first-pid])
          (testing "a lost connection is replaced before the next collection"
            (is (nil? (:failure (collect! lookups))))
            (is (not= first-pid (pid)))))
        (finally
          (some-> ^Connection @held .close))))))
