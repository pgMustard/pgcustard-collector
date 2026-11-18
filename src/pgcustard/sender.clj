(ns pgcustard.sender
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [pgcustard.errors :as errors]
            [pgcustard.payload :as payload]
            [pgcustard.spool :as spool]
            [taoensso.timbre :as log])
  (:import [java.io File]
           [java.net InetSocketAddress ProxySelector URI]
           [java.net.http HttpClient HttpClient$Builder HttpClient$Version HttpRequest HttpRequest$BodyPublishers
            HttpResponse HttpResponse$BodyHandlers]
           [java.nio.channels UnresolvedAddressException]
           [java.nio.file Files]
           [java.time Duration]))

(def ^:private never-accepted #{"too_old" "unreadable_payload"})

(def ^:private accepted #{"stored" "already_stored"})

(def ^:private user-agent (cond-> "pgcustard-collector" payload/collector-version (str "/" payload/collector-version)))

(defn parse-proxy [^String setting]
  (let [^URI uri (try (URI. (if (str/includes? setting "://") setting (str "http://" setting)))
                      (catch Exception _ nil))]
    (cond
      (nil? uri)                              {:issue :invalid}
      (.getRawUserInfo uri)                   {:issue :credentials}
      (and (= "http" (.getScheme uri))
           (.getHost uri)
           (< 0 (.getPort uri) 65536)
           (contains? #{nil "" "/"} (.getRawPath uri))
           (nil? (.getRawQuery uri))
           (nil? (.getRawFragment uri)))       {:host (str/replace (.getHost uri) #"^\[|\]$" "") :port (.getPort uri)}
      :else                                   {:issue :invalid})))

(def ^:private http-client
  (memoize
    (fn [{:keys [host port]}]
      (let [^HttpClient$Builder builder (-> (HttpClient/newBuilder)
                                            (.version HttpClient$Version/HTTP_1_1)
                                            (.connectTimeout (Duration/ofSeconds 10)))]
        (when host
          (.proxy builder (ProxySelector/of (InetSocketAddress/createUnresolved ^String host (int port)))))
        (.build builder)))))

(defn- post [^HttpClient client ^URI uri token ^bytes gzipped]
  (let [request  (-> (HttpRequest/newBuilder uri)
                     (.timeout (Duration/ofSeconds 60))
                     (.header "Authorization" (str "Bearer " token))
                     (.header "Content-Encoding" "gzip")
                     (.header "Content-Type" "application/json")
                     (.header "User-Agent" user-agent)
                     (.POST (HttpRequest$BodyPublishers/ofByteArray gzipped))
                     (.build))
        ^HttpResponse response (.send client request (HttpResponse$BodyHandlers/ofString))]
    {:status (.statusCode response) :body (.body response)}))

(defn- reply-code [body]
  (try (let [{:keys [error status]} (json/read-str body :key-fn keyword)]
         (or error status))
       (catch Exception _ nil)))

(defn- reply [{:keys [status body]}]
  (let [code (reply-code body)]
    {:status  status
     :code    code
     :outcome (cond (and (= 200 status) (accepted code))       :delivered
                    (= 413 status)                              :refused
                    (and (= 400 status) (never-accepted code)) :refused
                    :else                                       :retry-later)}))

(defn proxy-address [{:keys [^String host port]}]
  (str (if (str/includes? host ":") (str "[" host "]") host) ":" port))

(defn- failure-description [^URI uri proxy ^Throwable e]
  (let [chain       (errors/cause-chain e)
        unresolved? (some #(instance? UnresolvedAddressException %) chain)]
    (cond (some #(not (str/blank? (.getMessage ^Throwable %))) chain) (errors/error-message e)
          (and unresolved? proxy)                                      (str "unknown proxy host " (:host proxy))
          unresolved?                                                  (str "unknown host " (.getHost uri))
          proxy                                                        (str "couldn't connect to the proxy at " (proxy-address proxy))
          :else                                                        (str "connection to " (.getAuthority uri) " failed"))))

(defn- attempt [client proxy ^URI uri token ^File file]
  (try (reply (post client uri token (Files/readAllBytes (.toPath file))))
       (catch Exception e
         {:outcome :retry-later :error (failure-description uri proxy e)})))

(defn- report-refusal! [{:keys [status code]} ^File file]
  (let [at (str (spool/collected-at file))]
    (cond (= 413 status)     (log/error (str "pgCustard refused the payload collected at " at " as too large, so it's been deleted"))
          (= "too_old" code) (log/warn (str "pgCustard refused the payload collected at " at " as too old, so it's been deleted"))
          :else              (log/error (str "pgCustard couldn't read the payload collected at " at ", so it's been deleted")))))

(defn send-pending! [{:keys [payloads url token send-until-ms spacing-ms proxy]}]
  (let [uri    (URI. (str url "/api/ingest"))
        client (http-client proxy)]
    (loop [[^File file & more] (spool/pending payloads)
           sent                0
           last-result         nil]
      (if (and file (< (System/currentTimeMillis) send-until-ms))
        (let [started (System/currentTimeMillis)
              result  (attempt client proxy uri token file)]
          (if (= :retry-later (:outcome result))
            (assoc result :sent sent)
            (do (when (= :refused (:outcome result))
                  (report-refusal! result file))
                (Files/deleteIfExists (.toPath file))
                (Thread/sleep (long (max 0 (- (+ started spacing-ms) (System/currentTimeMillis)))))
                (recur more (cond-> sent (= :delivered (:outcome result)) inc) result))))
        (some-> last-result (assoc :sent sent))))))
