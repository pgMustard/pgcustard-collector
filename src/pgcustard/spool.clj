(ns pgcustard.spool
  (:require [clojure.string :as str]
            [pgcustard.payload :as payload]
            [taoensso.timbre :as log])
  (:import [java.io File]
           [java.nio.channels FileChannel FileLock OverlappingFileLockException]
           [java.nio.file CopyOption FileSystems Files OpenOption StandardCopyOption StandardOpenOption]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]
           [java.time Instant ZoneOffset]
           [java.time.format DateTimeFormatter]))

(def ^:private ^DateTimeFormatter name-format
  (.withZone (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss.SSS'Z'") ZoneOffset/UTC))

(def ^:private payload-name #"(\d{8}T\d{6}\.\d{3}Z)\.json\.gz")

(def max-total-bytes (* 1024 1024 1024))

(defn- private-directory-attributes []
  (into-array FileAttribute
              (when (contains? (.supportedFileAttributeViews (FileSystems/getDefault)) "posix")
                [(PosixFilePermissions/asFileAttribute (PosixFilePermissions/fromString "rwx------"))])))

(defn prepare! [^File dir]
  (Files/createDirectories (.toPath dir) (private-directory-attributes))
  (doseq [^File file (.listFiles dir)
          :when (str/ends-with? (.getName file) ".tmp")]
    (.delete file)))

(defn write-atomically! [^File file ^bytes contents]
  (let [temp (Files/createTempFile (.toPath (.getParentFile file)) "." ".tmp" (into-array FileAttribute []))]
    (try
      (Files/write temp contents ^"[Ljava.nio.file.OpenOption;" (into-array OpenOption []))
      (Files/move temp (.toPath file) (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE
                                                              StandardCopyOption/REPLACE_EXISTING]))
      (catch Exception e
        (Files/deleteIfExists temp)
        (throw e)))))

(defn file-name [^Instant collected-at]
  (str (.format name-format collected-at) ".json.gz"))

(defn collected-at [^File file]
  (when-let [[_ stamp] (re-matches payload-name (.getName file))]
    (Instant/from (.parse name-format stamp))))

(defn add! [^File dir ^Instant collected-at ^bytes gzipped]
  (write-atomically! (File. dir ^String (file-name collected-at)) gzipped))

(defn pending [^File dir]
  (sort-by #(.getName ^File %) (filter collected-at (.listFiles dir))))

(defn- drop! [files reason]
  (when-let [[first-file :as files] (seq files)]
    (doseq [^File file files]
      (.delete file))
    (let [one? (= 1 (count files))]
      (log/warn (str "deleted " (count files) " unsent payload" (when-not one? "s") " collected "
                     (if one?
                       (str "at " (collected-at first-file))
                       (str "from " (collected-at first-file) " to " (collected-at (last files))))
                     ", " (reason one?))))))

(defn- oldest-over [max-bytes files]
  (let [fitting (count (take-while #(<= % max-bytes)
                                   (reductions + (map #(.length ^File %) (reverse files)))))]
    (take (- (count files) fitting) files)))

(defn enforce-caps! [^File dir ^Instant now max-bytes]
  (let [oldest-accepted (.minus now payload/max-age)
        [expired kept]  (split-with #(.isBefore ^Instant (collected-at %) oldest-accepted) (pending dir))]
    (drop! expired #(str "as " (if % "it's" "they're") " over " (.toDays payload/max-age) " days old"))
    (drop! (oldest-over max-bytes kept) (constantly "to keep the total under 1GB"))))

(defn lock! ^FileLock [^File file]
  (let [channel (FileChannel/open (.toPath file) (into-array OpenOption [StandardOpenOption/CREATE
                                                                         StandardOpenOption/WRITE]))]
    (or (try (.tryLock channel)
             (catch OverlappingFileLockException _ nil))
        (do (.close channel)
            nil))))
