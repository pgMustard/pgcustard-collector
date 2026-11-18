(ns pgcustard.history
  (:require [clojure.data.json :as json]
            [pgcustard.spool :as spool]
            [taoensso.timbre :as log])
  (:import [java.io File]
           [java.nio.charset StandardCharsets]
           [java.nio.file CopyOption Files StandardCopyOption]
           [java.sql Timestamp]
           [java.time Duration Instant]))

(def ^:private version 1)

(def frozen-after (Duration/ofDays 7))

(def xmin-window (Duration/ofDays 7))

(def ^:private text-memory (Duration/ofDays 1))

(def counter-memory (Duration/ofDays 100))

(def empty-state {:texts {} :counters {} :xmin {}})

(defn- within? [^Instant at ^Duration window ^Instant now]
  (.isAfter at (.minus now window)))

(defn- keep-within [entries window now instant-of]
  (into {} (filter (fn [[_ entry]] (within? (instant-of entry) window now))) entries))

(defn- same-counter? [a b]
  (and (some? a) (some? b) (== a b)))

(defn- reset-instant [stats-since]
  (some-> ^Timestamp stats-since .toInstant))

(defn- unchanged? [counted {:keys [calls stats_since]}]
  (and counted
       (same-counter? (:calls counted) calls)
       (= (:stats_since counted) (reset-instant stats_since))))

(defn- frozen? [{:keys [counters]} now {:keys [queryid] :as row}]
  (let [counted (counters queryid)]
    (and (unchanged? counted row)
         (not (within? (:since counted) frozen-after now)))))

(defn- text-known? [{:keys [texts]} now queryid]
  (some-> (texts queryid) (within? text-memory now)))

(defn lookups [state ^Instant now]
  {:frozen-queryids          (fn [rows] (into #{} (comp (filter #(frozen? state now %)) (map :queryid)) rows))
   :missing-query-texts      (fn [queryids] (remove #(text-known? state now %) queryids))
   :unresolved-xmin-queryids (fn [] (vec (keys (:xmin state))))})

(defn- count-row [counters now {:keys [queryid calls stats_since] :as row}]
  (if (or (nil? queryid) (unchanged? (counters queryid) row))
    counters
    (assoc counters queryid {:calls calls :stats_since (reset-instant stats_since) :since now})))

(defn record [state ^Instant now collected]
  (let [texts  (into (keep-within (:texts state) text-memory now identity)
                     (map (fn [{:keys [queryid]}] [queryid now]))
                     (:query_texts collected))
        holder (get-in collected [:database :optional :query_id])]
    {:texts    texts
     :counters (keep-within (reduce #(count-row %1 now %2) (:counters state) (:queries collected))
                            counter-memory now :since)
     :xmin     (into {}
                     (remove (fn [[queryid _]] (contains? texts queryid)))
                     (cond-> (keep-within (:xmin state) xmin-window now identity)
                       holder (assoc holder now)))}))

(defn- to-json [{:keys [texts counters xmin]}]
  (json/write-str
    {:history_version version
     :texts           (update-keys (update-vals texts str) str)
     :counters        (update-keys (update-vals counters (fn [{:keys [calls stats_since since]}]
                                                           {:calls       calls
                                                            :stats_since (some-> stats_since str)
                                                            :since       (str since)}))
                                   str)
     :xmin            (update-keys (update-vals xmin str) str)}))

(defn- parse-instant [text]
  (Instant/parse text))

(defn- from-json [{:strs [texts counters xmin]}]
  {:texts    (update-keys (update-vals texts parse-instant) parse-long)
   :counters (update-keys (update-vals counters (fn [{:strs [calls stats_since since]}]
                                                  {:calls       calls
                                                   :stats_since (some-> stats_since parse-instant)
                                                   :since       (parse-instant since)}))
                          parse-long)
   :xmin     (update-keys (update-vals xmin parse-instant) parse-long)})

(defn- read-state [^File file]
  (let [parsed (json/read-str (slurp file))]
    (when (= version (get parsed "history_version"))
      (from-json parsed))))

(defn- set-aside! [^File file]
  (let [old (File. (str (.getPath file) ".old"))]
    (Files/move (.toPath file) (.toPath old) (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING]))
    (log/warn (str "couldn't use " (.getName file) ", so the collector is starting afresh (the file has been moved to "
                   (.getName old) ")"))
    empty-state))

(defn load-state [^File file]
  (if (.exists file)
    (or (try (read-state file)
             (catch Exception _ nil))
        (set-aside! file))
    empty-state))

(defn save! [^File file state]
  (spool/write-atomically! file (.getBytes ^String (to-json state) StandardCharsets/UTF_8)))
