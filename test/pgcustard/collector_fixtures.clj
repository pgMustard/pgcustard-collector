(ns pgcustard.collector-fixtures
  (:require [clojure.string :as str])
  (:import [com.sun.net.httpserver HttpExchange HttpHandler HttpServer]
           [java.io File]
           [java.net InetSocketAddress URI]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn with-temp-dir [f]
  (let [dir (.toFile (Files/createTempDirectory "pgcustard-collector-test" (into-array FileAttribute [])))]
    (try (f dir)
         (finally
           (doseq [^File file (reverse (file-seq dir))]
             (.delete file))))))

(defn- respond! [^HttpExchange exchange status ^String body]
  (let [bytes (.getBytes body "UTF-8")]
    (.sendResponseHeaders exchange status (alength bytes))
    (with-open [out (.getResponseBody exchange)]
      (.write out bytes))))

(defn serving [replies f]
  (let [received (atom [])
        server   (HttpServer/create (InetSocketAddress. "127.0.0.1" 0) 0)]
    (.createContext server "/api/ingest"
      (reify HttpHandler
        (handle [_ exchange]
          (let [body           (.readAllBytes (.getRequestBody exchange))
                [status reply] (nth replies (min (count @received) (dec (count replies))))]
            (swap! received conj {:authorization (.getFirst (.getRequestHeaders exchange) "Authorization")
                                  :user-agent    (.getFirst (.getRequestHeaders exchange) "User-Agent")
                                  :body          body})
            (respond! exchange status reply)))))
    (.start server)
    (try (f (str "http://127.0.0.1:" (.getPort (.getAddress server))) received)
         (finally (.stop server 0)))))

(defn connection-from-url [url]
  (let [uri             (URI. (str/replace-first url #"^jdbc:" ""))
        params          (into {} (for [pair  (str/split (or (.getQuery uri) "") #"&")
                                       :let  [[k v] (str/split pair #"=" 2)]
                                       :when (seq k)]
                                   [k v]))
        [user password] (some-> (.getUserInfo uri) (str/split #":" 2))]
    {:host     (.getHost uri)
     :port     (if (neg? (.getPort uri)) 5432 (.getPort uri))
     :database (subs (.getPath uri) 1)
     :user     (or user (params "user"))
     :password (or password (params "password"))
     :sslmode  (or (params "sslmode") "disable")}))

(def test-postgres-url (System/getenv "PGCUSTARD_TEST_POSTGRES_URL"))
