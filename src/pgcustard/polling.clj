(ns pgcustard.polling
  (:require [clojure.java.jdbc :as jdbc]
            [pgcustard.collect :as collect]
            [pgcustard.errors :as errors])
  (:import [java.sql Connection]
           [java.time Instant]))

(def retry-pause-ms 2000)

(def login-refused-interval-ms (* 5 60 1000))

(defn- close-quietly [^Connection connection]
  (try (.close connection)
       (catch Exception _ nil)))

(defn- open-connection [db-spec]
  (let [opened (jdbc/get-connection db-spec)]
    (try (collect/configure-session! {:connection opened})
         opened
         (catch Exception e
           (close-quietly opened)
           (throw e)))))

(defn- valid? [^Connection connection]
  (try (.isValid connection 5)
       (catch Exception _ false)))

(defn release! [held]
  (some-> @held close-quietly)
  (reset! held nil))

(defn collector [held db-spec]
  (fn [lookups]
    (let [current (if (and @held (valid? @held))
                    @held
                    (do (release! held)
                        (open-connection db-spec)))]
      (reset! held current)
      (let [collected (collect/collection {:connection current} lookups)]
        (when (some-> (get-in collected [:failure :error]) errors/connection-error?)
          (release! held))
        collected))))

(defn prober [db-spec]
  (fn []
    (try (close-quietly (open-connection db-spec))
         true
         (catch Exception _ false))))

(defn- attempt [collect! ^Instant at]
  (try {:at at :collected (collect! at)}
       (catch Exception e
         {:at at :error e})))

(defn- connection-failed? [{:keys [collected error]}]
  (boolean (some-> (or error (get-in collected [:failure :error])) errors/connection-error?)))

(defn- collect-with-retry [{:keys [collect! probe! retry-pause-ms now!]} now]
  (let [first-attempt (attempt collect! now)]
    (if-not (connection-failed? first-attempt)
      first-attempt
      (do (Thread/sleep (long retry-pause-ms))
          (let [retry (attempt collect! (now!))]
            (cond-> retry
              (connection-failed? retry) (assoc :reachable (boolean (and (:collected retry) (probe!))))))))))

(defn- refusal [{:keys [error]} ^Instant at]
  (when (and error (errors/login-refused? (errors/sql-state error)))
    {:error error :until (.plusMillis at login-refused-interval-ms)}))

(defn outcome [ctx state ^Instant now]
  (let [refused (:refused @state)]
    (if (and refused (.isBefore now ^Instant (:until refused)))
      {:at now :error (:error refused) :held? true}
      (let [result (collect-with-retry ctx now)]
        (swap! state assoc :refused (refusal result (:at result)))
        result))))

(defn reported [{:keys [collected error reachable]}]
  (cond-> (or collected {:failure {:section "connection" :error error}})
    (false? reachable) (assoc :reachable false)))
