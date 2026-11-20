(ns pgcustard.postgres
  (:require [clojure.string :as str]
            [pgcustard.ca-bundles :as ca-bundles]))

(def ^:private connect-timeout-s 10)

(def default-socket-timeout-s 300)

(def monitored-socket-timeout-s 60)

(def ^:private default-port 5432)

(def ^:private ssl-modes ["verify-full" "verify-ca" "require" "disable"])

(def ^:private allowed-ssl-modes (set ssl-modes))

(def ^:private verifying-ssl-modes #{"verify-ca" "verify-full"})

(def ^:private jvm-trust-store-factory "org.postgresql.ssl.DefaultJavaSSLFactory")

(def system-trust "system")

(def built-in-trusts (into #{system-trust} ca-bundles/trusts))

(def ^:private hostname-pattern #"(?i)\A[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?\z")

(def ^:private ipv6-pattern #"(?i)\A[0-9a-f:]+\z")

(def ^:private database-name-pattern #"\A[A-Za-z0-9_$.-]+\z")

(defn- ipv6-literal? [host]
  (str/includes? host ":"))

(defn- valid-host? [host]
  (boolean (if (ipv6-literal? host)
             (re-matches ipv6-pattern host)
             (re-matches hostname-pattern host))))

(defn- readable-file? [path]
  (let [file (java.io.File. ^String path)]
    (and (.isFile file) (.canRead file))))

(defn- port-number [port]
  (cond
    (nil? port)     default-port
    (integer? port) port
    :else           (try (Integer/parseInt (str/trim (str port)))
                         (catch NumberFormatException _ nil))))

(defn- endpoint-issues [{:keys [host port database user]}]
  (let [parsed-port (port-number port)]
    (cond-> []
      (str/blank? host)
      (conj {:setting :host :issue :not-set})

      (and (not (str/blank? host)) (not (valid-host? host)))
      (conj {:setting :host :issue :invalid})

      (not (and parsed-port (<= 1 parsed-port 65535)))
      (conj {:setting :port :issue :invalid})

      (str/blank? database)
      (conj {:setting :database :issue :not-set})

      (and (not (str/blank? database)) (not (re-matches database-name-pattern database)))
      (conj {:setting :database :issue :invalid})

      (str/blank? user)
      (conj {:setting :user :issue :not-set}))))

(defn- ssl-issues [{:keys [sslmode sslrootcert]}]
  (cond-> []
    (nil? sslmode)
    (conj {:setting :sslmode :issue :not-set})

    (and (some? sslmode) (not (allowed-ssl-modes sslmode)))
    (conj {:setting :sslmode :issue :invalid})

    (and (some? sslrootcert) (str/blank? sslrootcert))
    (conj {:setting :sslrootcert :issue :empty})

    (and (not (str/blank? sslrootcert)) (not (verifying-ssl-modes sslmode)))
    (conj {:setting :sslrootcert :issue :needs-verifying-mode})

    (and (not (str/blank? sslrootcert)) (not (built-in-trusts sslrootcert)) (not (readable-file? sslrootcert)))
    (conj {:setting :sslrootcert :issue :unreadable :value sslrootcert})

    (and (= "verify-ca" sslmode) (str/blank? sslrootcert))
    (conj {:setting :sslmode :issue :verify-ca-needs-root})))

(defn connection-issues [connection]
  (cond
    (string? connection) [{:setting :connection :issue :url}]
    (empty? connection)  [{:setting :connection :issue :not-set}]
    :else                (into (endpoint-issues connection) (ssl-issues connection))))

(defn- problem [{:keys [setting issue value]}]
  (case [setting issue]
    [:connection :url]                   "connection must be host, port, database, user, and password, not a URL"
    [:connection :not-set]               "connection is not configured"
    [:host :not-set]                     "host is not set"
    [:host :invalid]                     "host is not a hostname or an IP address"
    [:port :invalid]                     "port is not a number between 1 and 65535"
    [:database :not-set]                 "database is not set"
    [:database :invalid]                 "database name contains characters that are not allowed"
    [:user :not-set]                     "user is not set"
    [:sslmode :not-set]                  "encryption (sslmode) is not set"
    [:sslmode :invalid]                  (str "sslmode must be one of " (str/join ", " ssl-modes))
    [:sslmode :verify-ca-needs-root]     "verify-ca needs an sslrootcert naming the issuer to trust"
    [:sslrootcert :empty]                "sslrootcert is set but empty"
    [:sslrootcert :needs-verifying-mode] (str "sslrootcert needs sslmode " (str/join " or " (filter verifying-ssl-modes ssl-modes)))
    [:sslrootcert :unreadable]           (str "sslrootcert is not a readable file: " value)))

(defn endpoint-problems [connection]
  (mapv problem (endpoint-issues connection)))

(defn connection-problems [connection]
  (mapv problem (connection-issues connection)))

(defn check-connection! [connection]
  (when-let [problems (seq (connection-problems connection))]
    (throw (ex-info (str "invalid connection: " (str/join "; " problems))
                    {:problems problems}))))

(defn- url-host [host]
  (if (ipv6-literal? host) (str "[" host "]") host))

(defn endpoint [{:keys [host port database]}]
  (str (url-host host) ":" (port-number port) "/" database))

(defn url-encode [^String value]
  (java.net.URLEncoder/encode value "UTF-8"))

(defn trusts-jvm-store? [sslrootcert]
  (or (str/blank? sslrootcert) (= system-trust sslrootcert)))

(defn write-certificate-file ^java.io.File [name ^String pem]
  (doto (java.io.File/createTempFile (str "pgcustard-" name "-ca") ".crt")
    (spit pem)))

(defn certificate-file [name pem]
  (.getAbsolutePath (doto (write-certificate-file name pem) (.deleteOnExit))))

(def ^:private bundle-files
  (delay (into {} (for [trust ca-bundles/trusts]
                    [trust (certificate-file trust (ca-bundles/pem trust))]))))

(defn- root-certificate-file [sslrootcert]
  (get @bundle-files sslrootcert sslrootcert))

(defn- ssl-params [{:keys [sslmode sslrootcert]}]
  (cond
    (nil? sslmode)                      []
    (not (verifying-ssl-modes sslmode)) [(str "sslmode=" sslmode)]
    (trusts-jvm-store? sslrootcert)     [(str "sslmode=" sslmode)
                                         (str "sslfactory=" jvm-trust-store-factory)]
    :else                               [(str "sslmode=" sslmode)
                                         (str "sslrootcert=" (url-encode (root-certificate-file sslrootcert)))]))

(defn jdbc-url [{:keys [host port database] :as connection}
                {:keys [socket-timeout-s statement-timeout-ms settings extra-params]
                 :or   {socket-timeout-s default-socket-timeout-s}}]
  (let [settings (cond-> settings
                   statement-timeout-ms (assoc "statement_timeout" statement-timeout-ms))]
    (str "jdbc:postgresql://" (url-host host) ":" (port-number port) "/" database
         "?" (str/join "&" (cond-> (into [(str "connectTimeout=" connect-timeout-s)
                                          (str "socketTimeout=" socket-timeout-s)
                                          "ApplicationName=pgcustard"
                                          "tcpKeepAlive=true"]
                                         (concat (ssl-params connection) extra-params))
                             (seq settings)
                             (conj (str "options=" (url-encode (str/join " " (for [[setting value] settings]
                                                                                 (str "-c " setting "=" value)))))))))))

(defn data-source
  ([connection] (data-source connection {}))
  ([{:keys [user password] :as connection} url-options]
   (check-connection! connection)
   (let [driver      (org.postgresql.Driver.)
         url         (jdbc-url connection url-options)
         credentials (cond-> (doto (java.util.Properties.) (.setProperty "user" user))
                       password (doto (.setProperty "password" password)))]
     {:datasource (reify javax.sql.DataSource
                    (getConnection [_]
                      (.connect driver url credentials)))})))
