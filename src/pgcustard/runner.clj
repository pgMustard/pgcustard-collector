(ns pgcustard.runner
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [pgcustard.collect-once :as collect-once]
            [pgcustard.errors :as errors]
            [pgcustard.history :as history]
            [pgcustard.payload :as payload]
            [pgcustard.polling :as polling]
            [pgcustard.postgres :as postgres]
            [pgcustard.sender :as sender]
            [pgcustard.spool :as spool]
            [taoensso.timbre :as log])
  (:import [java.io File]
           [java.net URI]
           [java.nio.file AccessDeniedException]
           [java.sql Timestamp]
           [java.time Instant]
           [java.time.temporal ChronoUnit]
           [java.util Date]))

(def ^:private interval-ms 30000)

(def ^:private send-spacing-ms 500)

(def ^:private default-url "https://app.pgcustard.com")

(def ^:private local-hosts #{"localhost" "127.0.0.1" "[::1]"})

(def ^:private mode-directories {:live "live" :dry-run "dry-run"})

(def ^:private log-levels #{"info" "warn" "error"})

(defn- log-level [setting]
  (some-> setting str/lower-case log-levels keyword))

(defn- connection-from-env [env]
  {:host        (env "PGHOST")
   :port        (env "PGPORT")
   :database    (env "PGDATABASE")
   :user        (env "PGUSER")
   :password    (env "PGPASSWORD")
   :sslmode     (env "PGSSLMODE")
   :sslrootcert (env "PGSSLROOTCERT")})

(def ^:private ignored-libpq-settings
  ["PGHOSTADDR" "PGSERVICE" "PGSERVICEFILE" "PGOPTIONS" "PGAPPNAME" "PGCONNECT_TIMEOUT" "PGCLIENTENCODING"
   "PGSSLCERT" "PGSSLKEY" "PGSSLCERTMODE" "PGSSLCRL" "PGSSLCRLDIR" "PGSSLSNI" "PGSSLNEGOTIATION"
   "PGSSLMINPROTOCOLVERSION" "PGSSLMAXPROTOCOLVERSION" "PGREQUIRESSL" "PGREQUIREPEER" "PGREQUIREAUTH"
   "PGCHANNELBINDING" "PGGSSENCMODE" "PGGSSLIB" "PGGSSDELEGATION" "PGKRBSRVNAME" "PGTARGETSESSIONATTRS"
   "PGLOADBALANCEHOSTS"])

(defn- file-contents [path]
  (when-not (str/blank? path)
    (try (str/trim (slurp path))
         (catch Exception _ ::unreadable))))

(defn- token-settings [env]
  {:env       (env "PGCUSTARD_TOKEN")
   :file      (env "PGCUSTARD_TOKEN_FILE")
   :from-file (file-contents (env "PGCUSTARD_TOKEN_FILE"))})

(defn- resolved-token [{:keys [env from-file]}]
  (cond (not (str/blank? env)) env
        (string? from-file)    from-file))

(defn- proxy-setting [env]
  (first (for [name  ["HTTPS_PROXY" "https_proxy"]
               :let  [value (env name)]
               :when (not (str/blank? value))]
           {:name name :value (str/trim value)})))

(defn- proxy-for [url setting]
  (when (and setting (str/starts-with? url "https://"))
    (let [parsed (sender/parse-proxy (:value setting))]
      (when-not (:issue parsed) parsed))))

(defn config [env mode]
  (let [mode-dir (some->> (mode-directories mode) (io/file (or (env "PGCUSTARD_DATA_DIR") "pgcustard-collector")))
        tokens   (token-settings env)
        url      (str/replace (or (env "PGCUSTARD_URL") default-url) #"/+$" "")
        proxy    (proxy-setting env)]
    {:mode             mode
     :connection       (connection-from-env env)
     :ignored-settings (filterv #(not (str/blank? (env %))) ignored-libpq-settings)
     :url              url
     :proxy-setting    proxy
     :proxy            (proxy-for url proxy)
     :token-settings   tokens
     :token            (resolved-token tokens)
     :mode-dir         mode-dir
     :payloads         (some-> mode-dir (io/file "payloads"))
     :log-level        (env "PGCUSTARD_LOG_LEVEL")}))

(defn- sends-securely? [url]
  (let [^URI uri (try (URI. url) (catch Exception _ nil))]
    (boolean (and uri
                  (or (= "https" (.getScheme uri))
                      (and (= "http" (.getScheme uri)) (local-hosts (.getHost uri))))))))

(defn- connection-problem [{:keys [setting issue value]}]
  (case [setting issue]
    [:host :not-set]                     "PGHOST isn't set"
    [:host :invalid]                     "PGHOST isn't a hostname or IP address"
    [:port :invalid]                     "PGPORT isn't a number from 1 to 65535"
    [:database :not-set]                 "PGDATABASE isn't set"
    [:database :invalid]                 "PGDATABASE can only contain letters, digits, and _ $ . -"
    [:user :not-set]                     "PGUSER isn't set"
    [:sslmode :not-set]                  "PGSSLMODE isn't set (it must be disable, require, verify-ca, or verify-full)"
    [:sslmode :invalid]                  "PGSSLMODE must be disable, require, verify-ca, or verify-full"
    [:sslmode :verify-ca-needs-root]     "PGSSLMODE verify-ca needs PGSSLROOTCERT set to the certificate authority to trust"
    [:sslrootcert :empty]                "PGSSLROOTCERT is set but empty"
    [:sslrootcert :needs-verifying-mode] "PGSSLROOTCERT needs PGSSLMODE verify-ca or verify-full"
    [:sslrootcert :unreadable]           (str "PGSSLROOTCERT isn't a readable file: " value)))

(defn- token-problem [{:keys [env file from-file]}]
  (let [env?  (not (str/blank? env))
        file? (not (str/blank? file))]
    (cond
      (and env? file?)           "PGCUSTARD_TOKEN and PGCUSTARD_TOKEN_FILE are both set, so the collector can't tell which token to use"
      env?                       nil
      (not file?)                "neither PGCUSTARD_TOKEN nor PGCUSTARD_TOKEN_FILE is set"
      (= ::unreadable from-file) (str "PGCUSTARD_TOKEN_FILE isn't a readable file: " file)
      (str/blank? from-file)     (str "PGCUSTARD_TOKEN_FILE names an empty file: " file))))

(defn- proxy-problem [{:keys [name value]}]
  (case (:issue (sender/parse-proxy value))
    :invalid     (str name " must be host:port or http://host:port")
    :credentials (str name " includes a username or password, which the collector can't use")
    nil))

(defn config-problems [{:keys [mode connection url token-settings proxy-setting] :as config}]
  (cond-> (mapv connection-problem (postgres/connection-issues connection))
    (and (not (str/blank? (:log-level config))) (nil? (log-level (:log-level config))))
    (conj "PGCUSTARD_LOG_LEVEL must be info, warn, or error")

    (and (= :live mode) (token-problem token-settings))
    (conj (token-problem token-settings))

    (and (= :live mode) (not (sends-securely? url)))
    (conj "PGCUSTARD_URL must start with https://, or http:// for localhost")

    (and (= :live mode) proxy-setting (proxy-problem proxy-setting))
    (conj (proxy-problem proxy-setting))))

(defn- report! [{:keys [level message]}]
  (if (= :error level)
    (log/error message)
    (log/warn message)))

(defn- track! [runner k problem recovery]
  (let [previous (get @runner k)]
    (when (not= (:key previous) (:key problem))
      (swap! runner assoc k problem)
      (cond problem  (report! problem)
            previous (log/info recovery)))))

(def ^:private section-names
  {"database" "database metrics" "tables" "table metrics" "indexes" "index metrics" "queries" "query metrics"})

(defn- section-name [section]
  (get section-names section section))

(defn- collection-problem [collected error]
  (cond
    error
    {:key     (if (errors/unreachable? error) :unreachable (errors/error-message error))
     :level   :warn
     :message (str "collection failed - " (errors/error-message error))}

    (:failure collected)
    (let [{:keys [section] failure :error} (:failure collected)]
      {:key     [section (errors/sql-state failure)]
       :level   :warn
       :message (str "collection stopped at " (section-name section) " - " (errors/error-message failure))})))

(defn- sending-problem [url {:keys [outcome status code error]}]
  (when (= :retry-later outcome)
    (let [reply (fn [level message] {:key [status code] :level level :message message})]
      (cond
        error
        {:key     :unreachable
         :level   :warn
         :message (str "can't reach pgCustard, so payloads are being kept to send later - " error)}

        (= "invalid_token" code)
        (reply :error "pgCustard doesn't recognise the token (it may have been revoked), so payloads will be kept until it's replaced")

        (= "not_accepting" code)
        (reply :error "pgCustard isn't accepting metrics for this database (monitoring may be off, or the subscription may have ended), so payloads are being kept in case sending can resume later")

        (= "ahead_of_server_clock" code)
        (reply :warn "this machine's clock is over a minute ahead of pgCustard's, so the payload will be resent soon")

        (= "unsupported_payload_version" code)
        (reply :error (str "pgCustard no longer accepts payload version " payload/version " from collector "
                           payload/collector-version ", so payloads will be kept until the collector is upgraded"))

        (= 200 status)
        (reply :error (str url " replied 200 but doesn't look like pgCustard, so payloads are being kept to send later; check PGCUSTARD_URL and any proxy in between"))

        :else
        (reply :warn (str "pgCustard replied " status (some->> code (str " ")) ", so payloads are being kept to send later"))))))

(defn- history-file [mode-dir]
  (io/file mode-dir "history.json"))

(defn- millis [nanos]
  (str (quot nanos 1000000) "ms"))

(defn- collection-summary [{:keys [collected error held?]} nanos]
  (cond
    held?                "collection paused after a refused login"
    error                (str "collection failed after " (millis nanos))
    (:failure collected) (str "collection stopped at " (section-name (get-in collected [:failure :section])) " after " (millis nanos))
    :else                (str "collected in " (millis nanos))))

(defn- spool-summary [mode sent waiting]
  (if (= :live mode)
    (str (when (pos? sent) (str sent " sent, ")) waiting " waiting")
    (str waiting " kept for the dry run")))

(defn tick! [{:keys [mode mode-dir payloads url] :as ctx} runner ^Instant now]
  (let [history                                 (:history @runner)
        collect!                                (:collect! ctx)
        started                                 (System/nanoTime)
        {:keys [at collected error] :as result} (polling/outcome
                                                  (assoc ctx :collect! #(collect! (history/lookups history %)))
                                                  runner now)
        collecting-nanos                        (- (System/nanoTime) started)]
    (track! runner :collecting (collection-problem collected error) "collecting successfully again")
    (spool/add! payloads at (payload/gzip (payload/encode (Timestamp/from ^Instant at) (polling/reported result))))
    (when collected
      (let [updated (history/record history at collected)]
        (history/save! (history-file mode-dir) updated)
        (swap! runner assoc :history updated)))
    (spool/enforce-caps! payloads now spool/max-total-bytes)
    (let [sending (when (= :live mode)
                    (sender/send-pending! ctx))]
      (when sending
        (track! runner :sending (sending-problem url sending) "sending successfully again"))
      (log/info (str (collection-summary result collecting-nanos) ", "
                     (spool-summary mode (:sent sending 0) (count (spool/pending payloads))))))))

(defn- current-instant []
  (.truncatedTo (Instant/now) ChronoUnit/MILLIS))

(defn- next-due [due now]
  (+ due (* interval-ms (inc (quot (- now due) interval-ms)))))

(defn- exit! [status & problems]
  (doseq [problem problems]
    (log/error problem))
  (System/exit status))

(defn- describe-start [{:keys [mode url connection proxy]}]
  (str "pgCustard collector " (or payload/collector-version "(no version)") " started, collecting from "
       (postgres/endpoint connection) " every " (quot interval-ms 1000) "s"
       (if (= :live mode)
         (str " and sending to " url (some->> proxy sender/proxy-address (str " through the proxy at ")))
         " for a dry run, so nothing will be sent")))

(defn- configured [mode]
  (let [ctx (config #(System/getenv %) mode)]
    (doseq [setting (:ignored-settings ctx)]
      (log/warn (str setting " is set, but the collector doesn't read it, so it has no effect")))
    (when-let [problems (seq (config-problems ctx))]
      (apply exit! 2 problems))
    ctx))

(defn- lock-data-dir! [mode-dir payloads]
  (try (spool/prepare! mode-dir)
       (spool/prepare! payloads)
       (spool/lock! (io/file mode-dir "lock"))
       (catch AccessDeniedException e
         (exit! 2 (str "can't write to " (.getFile e) ", so the collector is stopping")))))

(defn- start! [mode]
  (let [{:keys [mode-dir payloads connection] :as ctx} (configured mode)
        lock    (or (lock-data-dir! mode-dir payloads)
                    (exit! 2 (str "another collector is already using " (.getPath ^File mode-dir) ", so this one is stopping")))
        db-spec (postgres/data-source connection {:socket-timeout-s postgres/monitored-socket-timeout-s})
        runner  (atom {:lock lock :history (history/load-state (history-file mode-dir))})
        ctx     (assoc ctx
                       :collect!       (polling/collector (atom nil) db-spec)
                       :probe!         (polling/prober db-spec)
                       :now!           current-instant
                       :retry-pause-ms polling/retry-pause-ms
                       :spacing-ms     send-spacing-ms)]
    (log/info (describe-start ctx))
    (loop [due (System/currentTimeMillis)]
      (let [following (+ due interval-ms)]
        (try (tick! (assoc ctx :send-until-ms following) runner (current-instant))
             (catch Exception e
               (log/error (str "an unexpected error interrupted this collection, so the collector will try again in "
                               (quot interval-ms 1000) "s - " (errors/error-message e)))))
        (let [now  (System/currentTimeMillis)
              next (next-due due now)]
          (Thread/sleep (long (- next now)))
          (recur (long next)))))))

(defn- log-line [{:keys [level msg_ ?err ^Date instant]}]
  (str (.toInstant instant) " " (str/upper-case (name level)) " " (force msg_)
       (when ?err
         (str " - " (errors/error-message ?err)))))

(defn- configure-logging! [level]
  (log/set-config! (assoc log/default-config
                          :min-level level
                          :output-fn log-line
                          :appenders {:stderr (log/println-appender {:stream :std-err})})))

(defn -main [& [command]]
  (configure-logging! (or (log-level (System/getenv "PGCUSTARD_LOG_LEVEL")) :info))
  (case command
    "run"     (start! :live)
    "dry-run" (start! :dry-run)
    "print"   (System/exit (collect-once/print-payload! (:connection (configured :print))))
    (exit! 2 "expected a command: run (collect and send), dry-run (collect without sending), or print (collect once, print the payload, and don't send it)")))
