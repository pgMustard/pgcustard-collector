(ns pgcustard.errors-test
  (:require [clojure.test :refer [deftest testing is]]
            [pgcustard.errors :as errors])
  (:import (java.net ConnectException SocketException SocketTimeoutException UnknownHostException)
           (java.sql SQLTransientConnectionException)
           (javax.net.ssl SSLHandshakeException)
           (org.postgresql.util PSQLException PSQLState ServerErrorMessage)))

(defn- wrapped [& messages]
  (reduce (fn [cause message] (RuntimeException. ^String message ^Throwable cause))
          nil
          (reverse messages)))

(deftest does-not-repeat-itself
  (testing "a vaguer cause that adds something is kept"
    (is (= "Connection to db.example.com:5432 refused. <- Connection refused"
           (errors/error-message (wrapped "Connection to db.example.com:5432 refused." "Connection refused")))))

  (testing "no cause to add"
    (is (= "relation \"x\" does not exist" (errors/error-message (wrapped "relation \"x\" does not exist")))))

  (testing "the cause repeats the wrapper"
    (is (= "boom" (errors/error-message (wrapped "boom" "boom")))))

  (testing "the wrapper already quotes the cause"
    (is (= "collection failed: boom" (errors/error-message (wrapped "collection failed: boom" "boom")))))

  (testing "an exception carrying no message at all falls back to its type"
    (is (= "RuntimeException" (errors/error-message (RuntimeException.))))
    (is (= "deepest" (errors/error-message (RuntimeException. nil (RuntimeException. "deepest")))))))

(defn- refused-by-server []
  (PSQLException. (ServerErrorMessage. "SFATAL\u0000C28P01\u0000Mpassword authentication failed for user \"u\"\u0000")))

(deftest tells-a-server-refusal-from-a-failure-to-reach-it
  (is (true? (errors/server-error? (refused-by-server))))
  (testing "however deep a wrapper buries it"
    (is (true? (errors/server-error? (RuntimeException. "wrapped" ^Throwable (refused-by-server))))))
  (testing "a handshake the driver gave up on never reached the server's login"
    (is (false? (errors/server-error? (PSQLException. "SSL error" PSQLState/CONNECTION_FAILURE
                                                  (SSLHandshakeException. "PKIX path building failed"))))))
  (is (false? (errors/server-error? (RuntimeException. "boom")))))

(defn- unable-to-connect [cause]
  (PSQLException. "The connection attempt failed." PSQLState/CONNECTION_UNABLE_TO_CONNECT cause))

(deftest finds-the-sqlstate-a-wrapper-hides
  (is (= "28P01" (errors/sql-state (RuntimeException. "wrapped" ^Throwable (refused-by-server)))))
  (is (nil? (errors/sql-state (RuntimeException. "boom")))))

(deftest a-connection-error-is-one-a-retry-might-get-past
  (is (true? (errors/connection-error? (unable-to-connect (ConnectException. "Connection refused")))))
  (is (true? (errors/connection-error? (unable-to-connect (UnknownHostException. "h")))))
  (is (true? (errors/connection-error? (PSQLException. "An I/O error occurred while sending to the backend."
                                                       PSQLState/CONNECTION_FAILURE))))
  (testing "a pool that couldn't hand out a connection"
    (is (true? (errors/connection-error? (SQLTransientConnectionException. "timed out waiting for a connection")))))
  (is (false? (errors/connection-error? (refused-by-server))))
  (is (false? (errors/connection-error? (RuntimeException. "boom")))))

(deftest a-refused-login-is-any-invalid-authorization-state
  (is (true? (errors/login-refused? "28P01")))
  (is (true? (errors/login-refused? "28000")))
  (is (false? (errors/login-refused? "08001")))
  (is (false? (errors/login-refused? nil))))

(deftest tells-a-server-that-never-answered-from-one-that-did
  (is (true? (errors/unreachable? (unable-to-connect (SocketTimeoutException. "Connect timed out")))))
  (is (true? (errors/unreachable? (unable-to-connect (ConnectException. "Connection refused")))))
  (testing "a network with no route to the host, as worded by musl and glibc"
    (is (true? (errors/unreachable? (unable-to-connect (SocketException. "Network unreachable")))))
    (is (true? (errors/unreachable? (unable-to-connect (SocketException. "Network is unreachable"))))))
  (testing "a connection dropped after it was made"
    (is (false? (errors/unreachable? (unable-to-connect (SocketException. "Connection reset"))))))
  (testing "a host name that doesn't resolve was never sent a packet"
    (is (false? (errors/unreachable? (unable-to-connect (UnknownHostException. "h"))))))
  (is (false? (errors/unreachable? (refused-by-server)))))
