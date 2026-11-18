(ns pgcustard.runner-test
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [pgcustard.collector-fixtures :refer [serving with-temp-dir]]
            [pgcustard.history :as history]
            [pgcustard.runner :as runner]
            [pgcustard.sender :as sender]
            [pgcustard.spool :as spool]
            [taoensso.timbre :as log])
  (:import [java.io ByteArrayInputStream File]
           [java.net ConnectException]
           [java.nio.file Files]
           [java.time Instant]
           [java.util.zip GZIPInputStream]
           [org.postgresql.util PSQLException ServerErrorMessage]))

(def ^:private now (.truncatedTo (Instant/now) java.time.temporal.ChronoUnit/MILLIS))

(def ^:private collected
  {:query_texts [{:queryid 1 :query "select 1"}]
   :database    {:snapshot {:idle_sessions 1}}})

(defn- context [^File dir mode url collect!]
  (let [payloads (io/file dir "payloads")]
    (spool/prepare! payloads)
    {:mode          mode
     :mode-dir      dir
     :payloads      payloads
     :url           url
     :token         "pgcustard_test"
     :collect!        collect!
     :probe!          (constantly true)
     :now!            (let [retried-at (atom (.plusSeconds now 1))]
                        #(swap! retried-at (fn [^Instant at] (.plusSeconds at 1))))
     :retry-pause-ms  0
     :send-until-ms   Long/MAX_VALUE
     :spacing-ms      0}))

(defn- unzipped [^bytes gzipped]
  (with-open [in (GZIPInputStream. (ByteArrayInputStream. gzipped))]
    (json/read-str (slurp in :encoding "UTF-8") :key-fn keyword)))

(defn- spooled [ctx]
  (map #(unzipped (Files/readAllBytes (.toPath ^File %))) (spool/pending (:payloads ctx))))

(defn- answering [replies]
  (let [left (atom replies)]
    (fn [_]
      (let [reply (ffirst (swap-vals! left rest))]
        (if (instance? Exception reply) (throw reply) reply)))))

(deftest a-collection-is-spooled-remembered-and-sent
  (with-temp-dir
    (fn [dir]
      (serving [[200 "{\"status\":\"stored\"}"]]
        (fn [url received]
          (let [asked  (atom [])
                ctx    (context dir :live url (fn [lookups]
                                                (swap! asked conj ((:missing-query-texts lookups) [1 2]))
                                                collected))
                state  (atom {:history history/empty-state})]
            (runner/tick! ctx state now)
            (is (= 1 (count @received)))
            (is (= (str now) (:collected_at (unzipped (:body (first @received))))))
            (is (empty? (spool/pending (:payloads ctx))))
            (is (= (:history @state) (history/load-state (io/file dir "history.json"))))
            (testing "the next collection knows which texts were sent"
              (runner/tick! ctx state (.plusSeconds now 30))
              (is (= [[1 2] [2]] (map vec @asked))))))))))

(deftest a-dry-run-sends-nothing
  (with-temp-dir
    (fn [dir]
      (serving [[200 "{\"status\":\"stored\"}"]]
        (fn [url received]
          (let [ctx (context dir :dry-run url (constantly collected))]
            (runner/tick! ctx (atom {:history history/empty-state}) now)
            (is (empty? @received))
            (is (= 1 (count (spool/pending (:payloads ctx)))))))))))

(deftest a-database-that-cant-be-reached-is-spooled-as-a-failure
  (with-temp-dir
    (fn [dir]
      (let [ctx   (context dir :dry-run "http://127.0.0.1:1" (fn [_] (throw (ConnectException. "Connection refused"))))
            state (atom {:history history/empty-state})]
        (runner/tick! ctx state now)
        (is (= [["connection" false]] (map (juxt (comp :section :failure) :reachable) (spooled ctx))))
        (is (= history/empty-state (:history @state)))
        (is (some? (:collecting @state)))))))

(defn- refused-login []
  (PSQLException. (ServerErrorMessage. "SFATAL\u0000C28P01\u0000Mpassword authentication failed for user \"u\"\u0000")))

(deftest a-refused-login-is-spooled-every-tick-so-the-collector-never-looks-silent
  (with-temp-dir
    (fn [dir]
      (let [ctx   (context dir :dry-run "http://127.0.0.1:1" (fn [_] (throw (refused-login))))
            state (atom {:history history/empty-state})]
        (doseq [secs [0 30 60]]
          (runner/tick! ctx state (.plusSeconds now secs)))
        (is (= (repeat 3 ["28P01" true]) (map (juxt (comp :sqlstate :failure) :reachable) (spooled ctx))))))))

(defn- summaries [f]
  (let [entries (atom [])]
    (log/with-merged-config {:appenders {:println {:enabled? false}
                                         :capture {:enabled? true
                                                   :fn       #(swap! entries conj [(:level %) (force (:msg_ %))])}}}
      (f))
    (filter #(re-find #"(waiting|dry run)$" (second %)) @entries)))

(deftest each-tick-logs-a-summary-at-info
  (with-temp-dir
    (fn [dir]
      (serving [[503 ""] [200 "{\"status\":\"stored\"}"]]
        (fn [url _]
          (let [ctx   (context dir :live url (constantly collected))
                state (atom {:history history/empty-state})
                lines (summaries #(doseq [secs [0 30]]
                                    (runner/tick! ctx state (.plusSeconds now secs))))]
            (is (= [:info :info] (map first lines)))
            (let [[unsent caught-up] (map second lines)]
              (is (str/includes? unsent "collected in"))
              (is (not (str/includes? unsent "sent")))
              (is (str/includes? unsent "1 waiting"))
              (is (str/includes? caught-up "2 sent"))
              (is (str/includes? caught-up "0 waiting")))))))))

(deftest a-summary-says-when-collection-failed-or-paused
  (with-temp-dir
    (fn [dir]
      (let [ctx   (context dir :dry-run "http://127.0.0.1:1"
                           (answering [(ConnectException. "Connection refused") (ConnectException. "Connection refused")
                                       (refused-login) (refused-login)]))
            state (atom {:history history/empty-state})
            lines (map second (summaries #(doseq [secs [0 30 60]]
                                            (runner/tick! ctx state (.plusSeconds now secs)))))]
        (is (= ["failed" "failed" "paused"] (map #(re-find #"failed|paused" %) lines)))
        (is (str/includes? (last lines) "3 kept"))))))

(deftest a-token-is-needed-to-send-and-only-over-https
  (let [problems #(runner/config-problems (runner/config (merge {"PGHOST" "db.example.com" "PGDATABASE" "app"
                                                                 "PGUSER" "pgcustard" "PGSSLMODE" "require"}
                                                                %1)
                                                         %2))]
    (is (empty? (problems {"PGCUSTARD_TOKEN" "pgcustard_x"} :live)))
    (is (empty? (problems {} :dry-run)))
    (is (= 1 (count (problems {} :live))))
    (is (str/includes? (first (problems {} :live)) "PGCUSTARD_TOKEN"))
    (is (= 1 (count (problems {"PGCUSTARD_TOKEN" "x" "PGCUSTARD_URL" "http://pgcustard.example"} :live))))
    (is (empty? (problems {"PGCUSTARD_TOKEN" "x" "PGCUSTARD_URL" "http://localhost:3000"} :live)))
    (testing "the log level is optional, and any case will do"
      (is (empty? (problems {"PGCUSTARD_LOG_LEVEL" ""} :dry-run)))
      (is (empty? (problems {"PGCUSTARD_LOG_LEVEL" "WARN"} :dry-run)))
      (is (= 1 (count (problems {"PGCUSTARD_LOG_LEVEL" "quiet"} :dry-run)))))))

(deftest database-settings-are-named-as-the-environment-variables-that-set-them
  (let [valid    {"PGHOST" "db.example.com" "PGDATABASE" "app" "PGUSER" "pgcustard" "PGSSLMODE" "require"}
        problems (mapcat #(runner/config-problems (runner/config % :print))
                         [{}
                          (assoc valid "PGHOST" "not a host!" "PGPORT" "x" "PGDATABASE" "a b")
                          (assoc valid "PGSSLMODE" "sometimes")
                          (assoc valid "PGSSLROOTCERT" "")
                          (assoc valid "PGSSLROOTCERT" "/no/such/host/file.crt")
                          (assoc valid "PGSSLMODE" "verify-ca")])]
    (is (= 12 (count problems)))
    (doseq [problem problems
            :let [[described] (str/split problem #": " 2)]]
      (is (re-find #"\bPG[A-Z]+\b" described) problem)
      (is (not (re-find #"\b(host|port|database|user|sslmode|sslrootcert)\b" described)) problem))
    (testing "a value quoted back is left as it was"
      (is (some #(str/ends-with? % ": /no/such/host/file.crt") problems)))))

(defn- reports-while [f]
  (let [reported (atom [])]
    (with-redefs-fn {#'runner/report! #(swap! reported conj %)}
      f)
    @reported))

(deftest a-sending-problem-that-needs-a-person-is-an-error
  (doseq [[result level] [[{:status 401 :code "invalid_token"} :error]
                          [{:status 403 :code "not_accepting"} :error]
                          [{:status 400 :code "unsupported_payload_version"} :error]
                          [{:status 200} :error]
                          [{:status 425 :code "ahead_of_server_clock"} :warn]
                          [{:status 503} :warn]
                          [{:error "Connection refused"} :warn]]]
    (with-temp-dir
      (fn [dir]
        (let [ctx     (context dir :live "https://proxy.example" (constantly collected))
              reports (reports-while
                        (fn []
                          (with-redefs [sender/send-pending! (constantly (assoc result :outcome :retry-later :sent 0))]
                            (runner/tick! ctx (atom {:history history/empty-state}) now))))]
          (is (= [level] (map :level reports)) (str result))
          (when (= 200 (:status result))
            (testing "a reply from something else names where it came from"
              (is (str/includes? (:message (first reports)) "https://proxy.example")))))))))

(deftest a-lasting-network-failure-is-reported-once-however-it-is-worded
  (with-temp-dir
    (fn [dir]
      (let [state   (atom {:history history/empty-state})
            replies (atom [{:outcome :retry-later :error "Network is unreachable"}
                           {:outcome :retry-later :error "nodename nor servname provided, or not known"}
                           {:outcome :delivered}])
            ctx     (context dir :live "http://127.0.0.1:1" (constantly collected))]
        (is (= 1 (count (reports-while
                          (fn []
                            (with-redefs [sender/send-pending! (fn [_] (ffirst (swap-vals! replies rest)))]
                              (dotimes [i 3]
                                (runner/tick! ctx state (.plusSeconds now (* 30 i))))))))))
        (is (nil? (:sending @state)))))))

(deftest an-unreachable-database-is-reported-once-however-it-is-worded
  (with-temp-dir
    (fn [dir]
      (let [ctx   (context dir :dry-run "http://127.0.0.1:1"
                           (answering [(ConnectException. "Connection refused") (ConnectException. "Connection refused")
                                       (ConnectException. "Operation timed out") (ConnectException. "Operation timed out")]))
            state (atom {:history history/empty-state})]
        (is (= 1 (count (reports-while
                          (fn []
                            (dotimes [i 2]
                              (runner/tick! ctx state (.plusSeconds now (* 30 i)))))))))))))

(deftest a-collection-that-stops-partway-names-the-metrics-it-stopped-at
  (with-temp-dir
    (fn [dir]
      (let [stopped (assoc collected :failure {:section "queries"
                                               :error   (java.sql.SQLException. "permission denied for function pg_stat_statements" "42501")})
            ctx     (context dir :dry-run "http://127.0.0.1:1" (constantly stopped))
            reports (atom nil)
            lines   (summaries #(reset! reports (reports-while (fn [] (runner/tick! ctx (atom {:history history/empty-state}) now)))))]
        (is (= [:warn] (map :level @reports)))
        (is (str/includes? (:message (first @reports)) "query metrics"))
        (is (str/includes? (second (first lines)) "query metrics"))))))

(deftest the-start-line-names-the-database
  (is (str/includes? (#'runner/describe-start {:mode :dry-run :connection {:host "db.internal" :database "app"}})
                     "db.internal:5432/app")))

(defn- live-config [env]
  (runner/config (merge {"PGHOST" "db.example.com" "PGDATABASE" "app" "PGUSER" "pgcustard" "PGSSLMODE" "require"} env)
                 :live))

(deftest a-token-can-come-from-a-file
  (with-temp-dir
    (fn [^File dir]
      (let [path  (.getPath (io/file dir "token"))
            empty (.getPath (io/file dir "empty"))]
        (spit path "pgcustard_from_file\n")
        (spit empty "\n")
        (let [config (live-config {"PGCUSTARD_TOKEN_FILE" path})]
          (is (empty? (runner/config-problems config)))
          (testing "the trailing newline a secret file usually ends with is dropped"
            (is (= "pgcustard_from_file" (:token config)))))
        (doseq [env [{"PGCUSTARD_TOKEN" "pgcustard_x" "PGCUSTARD_TOKEN_FILE" path}
                     {"PGCUSTARD_TOKEN_FILE" (.getPath (io/file dir "missing"))}
                     {"PGCUSTARD_TOKEN_FILE" empty}]]
          (let [problems (runner/config-problems (live-config env))]
            (is (= 1 (count problems)) (str env))
            (is (str/includes? (first problems) "PGCUSTARD_TOKEN") (str env))))
        (testing "a dry run needs no token"
          (is (empty? (runner/config-problems (runner/config {"PGHOST" "h" "PGDATABASE" "d" "PGUSER" "u" "PGSSLMODE" "require"
                                                              "PGCUSTARD_TOKEN_FILE" (.getPath (io/file dir "missing"))}
                                                             :dry-run)))))))))

(deftest https-proxy-applies-to-sends-over-https
  (is (= {:host "proxy.corp" :port 3128} (:proxy (live-config {"HTTPS_PROXY" "http://proxy.corp:3128"}))))
  (testing "the lowercase name works too, and the scheme can be left out"
    (is (= {:host "proxy.corp" :port 3128} (:proxy (live-config {"https_proxy" "proxy.corp:3128"})))))
  (testing "the uppercase name wins when both are set"
    (is (= "upper" (:host (:proxy (live-config {"HTTPS_PROXY" "upper:1" "https_proxy" "lower:2"}))))))
  (testing "plain http to localhost doesn't go through it"
    (is (nil? (:proxy (live-config {"HTTPS_PROXY" "proxy.corp:3128" "PGCUSTARD_URL" "http://localhost:3000"})))))
  (testing "the start line names it"
    (is (str/includes? (#'runner/describe-start (live-config {"HTTPS_PROXY" "http://[::1]:3128"})) "[::1]:3128"))))

(deftest a-proxy-setting-the-collector-cant-use-stops-it
  (doseq [[name value] [["HTTPS_PROXY" "https://proxy.corp:3128"]
                        ["https_proxy" "http://proxy.corp"]
                        ["HTTPS_PROXY" "http://user:secret@proxy.corp:3128"]]]
    (let [problems (runner/config-problems (live-config {"PGCUSTARD_TOKEN" "pgcustard_x" name value}))]
      (is (= 1 (count problems)) value)
      (is (str/starts-with? (first problems) name) value)))
  (testing "a dry run sends nothing, so it isn't checked"
    (is (empty? (runner/config-problems (runner/config {"PGHOST" "h" "PGDATABASE" "d" "PGUSER" "u" "PGSSLMODE" "require"
                                                        "HTTPS_PROXY" "nonsense"}
                                                       :dry-run))))))

(deftest libpq-settings-the-collector-ignores-are-named
  (is (= ["PGSSLCERT" "PGSSLKEY"]
         (:ignored-settings (runner/config {"PGSSLCERT" "/c.crt" "PGSSLKEY" "/c.key" "PGPASSFILE" "/p"
                                            "PGDATA" "/var/lib/postgresql/data" "PGAPPNAME" ""}
                                           :dry-run)))))
