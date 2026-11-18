(ns pgcustard.sender-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [pgcustard.collector-fixtures :refer [serving with-temp-dir]]
            [pgcustard.sender :as sender]
            [pgcustard.spool :as spool]
            [taoensso.timbre :as log])
  (:import [java.net ServerSocket URI]
           [java.time Duration Instant]))

(def ^:private start (Instant/parse "2026-11-13T12:00:00Z"))

(defn- spooled! [dir n]
  (doseq [i (range n)]
    (spool/add! dir (.plus start (Duration/ofSeconds (* 30 i))) (.getBytes (str "payload " i)))))

(defn- send! [dir url]
  (sender/send-pending! {:payloads      dir
                         :url           url
                         :token         "pgcustard_test"
                         :send-until-ms Long/MAX_VALUE
                         :spacing-ms    0}))

(defn- stored [code] [200 (str "{\"status\":\"" code "\"}")])

(defn- refused [status code] [status (str "{\"error\":\"" code "\"}")])

(deftest payloads-are-sent-oldest-first-as-they-are-stored
  (with-temp-dir
    (fn [dir]
      (spooled! dir 3)
      (serving [(stored "stored")]
        (fn [url received]
          (is (= {:outcome :delivered :sent 3} (select-keys (send! dir url) [:outcome :sent])))
          (is (empty? (spool/pending dir)))
          (is (= ["payload 0" "payload 1" "payload 2"] (map #(String. ^bytes (:body %)) @received)))
          (is (every? #(= "Bearer pgcustard_test" (:authorization %)) @received))
          (is (every? #(str/starts-with? (:user-agent %) "pgcustard-collector") @received)))))))

(defn- logged-levels [f]
  (let [levels (atom [])]
    (log/with-merged-config {:appenders {:println {:enabled? false}
                                         :capture {:enabled? true :fn #(swap! levels conj (:level %))}}}
      (f))
    @levels))

(deftest a-payload-the-server-will-never-accept-is-deleted
  (with-temp-dir
    (fn [dir]
      (spooled! dir 4)
      (serving [(refused 400 "too_old") (refused 413 "too_large") (refused 400 "unreadable_payload")
                (stored "already_stored")]
        (fn [url received]
          (let [result (atom nil)
                levels (logged-levels #(reset! result (send! dir url)))]
            (testing "only the one stored counts as sent"
              (is (= {:outcome :delivered :sent 1} (select-keys @result [:outcome :sent]))))
            (testing "a payload too old to keep needs nobody, the others need us"
              (is (= [:warn :error :error] levels))))
          (is (= 4 (count @received)))
          (is (empty? (spool/pending dir))))))))

(deftest anything-else-keeps-the-payloads-and-pauses-sending
  (doseq [reply [(refused 400 "unsupported_payload_version") (refused 401 "invalid_token")
                 (refused 403 "not_accepting") (refused 415 "gzip_required")
                 (refused 425 "ahead_of_server_clock") [429 "<html>Too many requests</html>"]
                 (refused 500 "server_error") [503 ""]
                 [200 "<html>Welcome</html>"] (stored "something_else")]]
    (with-temp-dir
      (fn [dir]
        (spooled! dir 2)
        (serving [reply]
          (fn [url received]
            (let [result (send! dir url)]
              (is (= :retry-later (:outcome result)) (str reply))
              (is (= (first reply) (:status result)))
              (is (zero? (:sent result))))
            (is (= 1 (count @received)))
            (is (= 2 (count (spool/pending dir))))))))))

(defn- says-more-than-a-class-name? [error]
  (and (string? error) (not (re-matches #"[A-Za-z.]*Exception" error))))

(deftest an-unreachable-server-keeps-the-payloads
  (with-temp-dir
    (fn [dir]
      (spooled! dir 1)
      (let [port   (with-open [socket (ServerSocket. 0)] (.getLocalPort socket))
            result (send! dir (str "http://127.0.0.1:" port))]
        (is (= :retry-later (:outcome result)))
        (is (says-more-than-a-class-name? (:error result)) (:error result))
        (is (= 1 (count (spool/pending dir))))))))

(deftest a-host-that-doesnt-resolve-is-named
  (with-temp-dir
    (fn [dir]
      (spooled! dir 1)
      (let [result (send! dir "http://no-such-host.invalid")]
        (is (= :retry-later (:outcome result)))
        (is (says-more-than-a-class-name? (:error result)) (:error result))
        (is (= 1 (count (spool/pending dir))))))))

(deftest sending-stops-when-the-next-collection-is-due
  (with-temp-dir
    (fn [dir]
      (spooled! dir 1)
      (serving [(stored "stored")]
        (fn [url received]
          (is (nil? (sender/send-pending! {:payloads dir :url url :token "t" :send-until-ms 0 :spacing-ms 0})))
          (is (empty? @received))
          (is (= 1 (count (spool/pending dir)))))))))

(deftest proxy-settings-are-parsed
  (doseq [[setting expected] [["http://proxy.corp:3128" {:host "proxy.corp" :port 3128}]
                              ["proxy.corp:3128" {:host "proxy.corp" :port 3128}]
                              ["http://proxy.corp:3128/" {:host "proxy.corp" :port 3128}]
                              ["http://[::1]:3128" {:host "::1" :port 3128}]
                              ["http://user:secret@proxy.corp:3128" {:issue :credentials}]
                              ["http://proxy.corp" {:issue :invalid}]
                              ["https://proxy.corp:3128" {:issue :invalid}]
                              ["socks5://proxy.corp:1080" {:issue :invalid}]
                              ["http://proxy.corp:3128/path" {:issue :invalid}]
                              ["http://proxy.corp:99999" {:issue :invalid}]
                              ["not a url" {:issue :invalid}]]]
    (is (= expected (sender/parse-proxy setting)) setting)))

(defn- send-through! [dir url proxy]
  (sender/send-pending! {:payloads      dir
                         :url           url
                         :token         "pgcustard_test"
                         :send-until-ms Long/MAX_VALUE
                         :spacing-ms    0
                         :proxy         proxy}))

(deftest payloads-go-through-the-proxy-when-there-is-one
  (with-temp-dir
    (fn [dir]
      (spooled! dir 1)
      (serving [(stored "stored")]
        (fn [proxy-url received]
          (let [result (send-through! dir "http://pgcustard.invalid" {:host "127.0.0.1" :port (.getPort (URI. proxy-url))})]
            (is (= :delivered (:outcome result)))
            (is (= 1 (count @received)))))))))

(deftest an-unreachable-proxy-is-named
  (with-temp-dir
    (fn [dir]
      (spooled! dir 1)
      (let [port   (with-open [socket (ServerSocket. 0)] (.getLocalPort socket))
            result (send-through! dir "https://pgcustard.invalid" {:host "127.0.0.1" :port port})]
        (is (= :retry-later (:outcome result)))
        (is (str/includes? (:error result) (str "proxy at 127.0.0.1:" port)) (:error result))
        (is (= 1 (count (spool/pending dir))))))))

(deftest a-proxy-host-that-doesnt-resolve-is-named
  (with-temp-dir
    (fn [dir]
      (spooled! dir 1)
      (let [result (send-through! dir "https://pgcustard.invalid" {:host "no-such-proxy.invalid" :port 3128})]
        (is (= :retry-later (:outcome result)))
        (is (str/includes? (:error result) "proxy host no-such-proxy.invalid") (:error result))))))
