(ns pgcustard.postgres-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.string :as str]
            [pgcustard.ca-bundles :as ca-bundles]
            [pgcustard.postgres :as postgres]))

(def ^:private valid
  {:host "db.example.com" :port 5432 :database "app"
   :user "pgcustard" :password "s3cr3t" :sslmode "require"})

(defn- problems [connection]
  (postgres/connection-problems connection))

(defn- problem-about? [connection fragment]
  (boolean (some #(str/includes? % fragment) (problems connection))))

(defn- url-for [connection]
  (postgres/check-connection! connection)
  (postgres/jdbc-url connection {}))

(defn- with-ca-file [f]
  (let [file (java.io.File/createTempFile "pgcustard-ca" ".crt")]
    (spit file "-----BEGIN CERTIFICATE-----\n")
    (try (f (.getAbsolutePath file)) (finally (.delete file)))))

(deftest accepts-a-complete-connection
  (is (empty? (problems valid)))

  (testing "port falls back to the Postgres default"
    (is (empty? (problems (dissoc valid :port))))
    (is (str/includes? (url-for (dissoc valid :port)) "//db.example.com:5432/")))

  (testing "a port arriving from the environment as a string is accepted"
    (is (empty? (problems (assoc valid :port "6543"))))
    (is (str/includes? (url-for (assoc valid :port "6543")) ":6543/"))))

(deftest rejects-an-unusable-connection
  (is (problem-about? nil "not configured"))
  (is (problem-about? {} "not configured"))
  (is (problem-about? "jdbc:postgresql://db.example.com/app" "not a URL"))
  (is (problem-about? (dissoc valid :host) "host"))
  (is (problem-about? (dissoc valid :database) "database"))
  (is (problem-about? (dissoc valid :user) "user"))
  (is (problem-about? (dissoc valid :sslmode) "sslmode"))

  (testing "a missing password is allowed, for trust and IAM authentication"
    (is (empty? (problems (dissoc valid :password)))))

  (testing "every problem is reported at once"
    (is (<= 3 (count (problems {:host "" :database "" :user "" :port 0}))))))

(deftest rejects-out-of-range-ports
  (doseq [port [0 65536 -1 "not-a-number" "" 99999]]
    (is (problem-about? (assoc valid :port port) "port")
        (str "port " (pr-str port) " should be rejected"))))

(deftest refuses-to-smuggle-driver-parameters
  (testing "a host cannot carry a path, a query string, or extra parameters"
    (doseq [host ["evil.example.com/db?sslfactory=Foo"
                  "evil.example.com?socketFactory=Foo"
                  "evil.example.com&ApplicationName=x"
                  "evil.example.com/"
                  "db.example.com:5432"
                  "db example com"]]
      (is (problem-about? (assoc valid :host host) "host")
          (str "host " (pr-str host) " should be rejected"))))

  (testing "a database name cannot carry a query string"
    (doseq [database ["app?sslfactory=Foo" "app&x=1" "app/other" "app db"]]
      (is (problem-about? (assoc valid :database database) "database")
          (str "database " (pr-str database) " should be rejected")))))

(deftest accepts-the-supported-sslmodes
  (doseq [mode ["disable" "require" "verify-full"]]
    (is (empty? (problems (assoc valid :sslmode mode)))
        (str "sslmode " (pr-str mode) " should be accepted"))
    (is (str/includes? (url-for (assoc valid :sslmode mode)) (str "sslmode=" mode))))

  (testing "verify-ca is accepted only alongside the issuer it is meant to trust"
    (with-ca-file
      (fn [path]
        (let [connection (assoc valid :sslmode "verify-ca" :sslrootcert path)]
          (is (empty? (problems connection)))
          (is (str/includes? (url-for connection) "sslmode=verify-ca"))))))

  (testing "a connection that does not say how it is protected is refused"
    (is (problem-about? (dissoc valid :sslmode) "sslmode"))))

(deftest refuses-verify-ca-against-every-public-authority
  (testing "checking the chain without checking the name means nothing when anyone can be issued one"
    (is (problem-about? (assoc valid :sslmode "verify-ca") "verify-ca"))
    (is (thrown? clojure.lang.ExceptionInfo (url-for (assoc valid :sslmode "verify-ca")))))

  (testing "verify-full may still fall back to the JRE store, because it checks the name"
    (is (empty? (problems (assoc valid :sslmode "verify-full"))))))

(deftest rejects-every-other-sslmode
  (testing "modes that complete a plaintext connection without saying so are refused"
    (doseq [mode ["prefer" "allow"]]
      (is (problem-about? (assoc valid :sslmode mode) "sslmode")
          (str "sslmode " (pr-str mode) " should be rejected"))))

  (testing "anything the driver would not recognise is refused"
    (doseq [mode ["VERIFY-FULL" "" "verify_full" "yes"]]
      (is (problem-about? (assoc valid :sslmode mode) "sslmode")
          (str "sslmode " (pr-str mode) " should be rejected")))))

(deftest builds-a-url-the-driver-can-use
  (testing "the password never reaches the URL"
    (is (not (str/includes? (url-for valid) "s3cr3t"))))

  (testing "the collector's own driver settings are always present"
    (doseq [param ["connectTimeout=10" "ApplicationName=pgcustard" "tcpKeepAlive=true"]]
      (is (str/includes? (url-for valid) param))))

  (testing "an IPv6 literal is bracketed"
    (is (str/includes? (url-for (assoc valid :host "2001:db8::1"))
                       "//[2001:db8::1]:5432/")))

  (testing "an invalid connection is refused before connecting"
    (is (thrown? clojure.lang.ExceptionInfo (url-for (assoc valid :host "evil.com/x?a=b"))))))

(deftest trusts-the-jvm-store-when-no-ca-file-is-given
  (testing "verify-full reaches the JRE trust store, not the driver's missing default"
    (let [url (url-for (assoc valid :sslmode "verify-full"))]
      (is (str/includes? url "sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory"))
      (is (str/includes? url "sslmode=verify-full"))))

  (testing "sslrootcert system asks for the same store explicitly"
    (let [system (assoc valid :sslmode "verify-full" :sslrootcert "system")]
      (is (str/includes? (url-for system) "sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory"))
      (is (not (problem-about? system "sslrootcert")))))

  (testing "a non-verifying mode gets no factory, so the driver keeps its own behaviour"
    (doseq [mode ["disable" "require"]]
      (is (not (str/includes? (url-for (assoc valid :sslmode mode)) "sslfactory")))))

  (testing "a connection with no sslmode is never attempted at all"
    (is (thrown? clojure.lang.ExceptionInfo (url-for (dissoc valid :sslmode))))))

(deftest a-bundled-trust-uses-its-shipped-bundle
  (doseq [trust ca-bundles/trusts]
    (let [connection (assoc valid :sslmode "verify-full" :sslrootcert trust)
          url        (url-for connection)
          path       (java.net.URLDecoder/decode (second (re-find #"sslrootcert=([^&]+)" url)) "UTF-8")]
      (is (empty? (problems connection)) trust)
      (is (not (str/includes? url "sslfactory")) trust)
      (is (= (ca-bundles/pem trust) (slurp path)) trust))))

(deftest uses-a-ca-file-in-place-of-the-jvm-store
  (with-ca-file
    (fn [path]
      (let [url (url-for (assoc valid :sslmode "verify-full" :sslrootcert path))]
        (is (str/includes? url "sslrootcert="))
        (is (not (str/includes? url "sslfactory")))))))

(deftest rejects-a-ca-file-that-cannot-be-used
  (testing "a path that is not a readable file"
    (is (problem-about? (assoc valid :sslmode "verify-full" :sslrootcert "/no/such/ca.crt")
                        "sslrootcert")))

  (testing "an empty setting is a mistake, not an absence"
    (is (problem-about? (assoc valid :sslmode "verify-full" :sslrootcert "") "sslrootcert")))

  (testing "a CA file is pointless unless the mode verifies"
    (with-ca-file
      (fn [path]
        (doseq [mode [nil "disable" "require"]]
          (is (problem-about? (assoc valid :sslmode mode :sslrootcert path) "sslrootcert")
              (str "sslrootcert with sslmode " (pr-str mode) " should be rejected"))))))

  (testing "a readable CA file with a verifying mode is accepted"
    (with-ca-file
      (fn [path]
        (is (empty? (problems (assoc valid :sslmode "verify-full" :sslrootcert path))))))))

(deftest encodes-a-ca-path-the-driver-would-otherwise-mangle
  (testing "the driver url-decodes parameters, so a plus would arrive as a space"
    (is (str/includes?
          (postgres/url-encode "/tmp/my+certs/ca.crt")
          "%2B"))
    (is (= "/tmp/my+certs/ca.crt"
           (java.net.URLDecoder/decode (postgres/url-encode "/tmp/my+certs/ca.crt") "UTF-8")))))
