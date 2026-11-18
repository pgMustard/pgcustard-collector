(ns pgcustard.errors
  (:require [clojure.string :as str])
  (:import (org.postgresql.util PSQLException)))

(defn cause-chain [^Throwable t]
  (take-while some? (iterate #(some-> ^Throwable % (.getCause)) t)))

(defn- already-said? [kept message]
  (boolean (some #(str/includes? % message) kept)))

(defn error-message [^Throwable t]
  (let [distinct-messages (->> (cause-chain t)
                               (map #(.getMessage ^Throwable %))
                               (remove str/blank?)
                               (reduce (fn [kept message]
                                         (if (already-said? kept message)
                                           kept
                                           (conj kept message)))
                                       []))]
    (if (seq distinct-messages)
      (str/join " <- " distinct-messages)
      (or (.getMessage t) (.getSimpleName (class t))))))

(defn server-error? [^Throwable t]
  (boolean (some #(and (instance? PSQLException %) (.getServerErrorMessage ^PSQLException %))
                 (cause-chain t))))

(defn unreachable? [^Throwable t]
  (boolean (some #(or (instance? java.net.ConnectException %)
                      (instance? java.net.NoRouteToHostException %)
                      (instance? java.net.SocketTimeoutException %)
                      (and (instance? java.net.SocketException %)
                           (re-find #"(?i)network (is )?unreachable" (str (.getMessage ^Throwable %)))))
                 (cause-chain t))))

(defn connection-error? [^Throwable t]
  (boolean (some #(or (instance? java.net.ConnectException %)
                      (instance? java.net.SocketTimeoutException %)
                      (instance? java.net.UnknownHostException %)
                      (instance? java.sql.SQLTransientConnectionException %)
                      (and (instance? java.sql.SQLException %)
                           (some-> (.getSQLState ^java.sql.SQLException %) (str/starts-with? "08"))))
                 (cause-chain t))))

(defn login-refused? [sqlstate]
  (boolean (some-> sqlstate (str/starts-with? "28"))))

(defn sql-state [^Throwable t]
  (some #(when (instance? java.sql.SQLException %) (.getSQLState ^java.sql.SQLException %))
        (cause-chain t)))
