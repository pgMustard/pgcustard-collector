(ns pgcustard.ca-bundles
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (java.io ByteArrayInputStream)
           (java.security.cert CertificateFactory)))

(def bundles
  [{:trust         "amazon-rds"
    :resource      "certificates/amazon-rds-global-bundle.pem"
    :issuer-prefix "Amazon RDS "}
   {:trust         "supabase"
    :resource      "certificates/supabase-root-2021-ca.pem"
    :issuer-prefix "Supabase "}])

(def trusts (mapv :trust bundles))

(defn parse-certificates [^String pem]
  (vec (.generateCertificates (CertificateFactory/getInstance "X.509")
                              (ByteArrayInputStream. (.getBytes pem "US-ASCII")))))

(def ^:private pems
  (delay (into {} (for [{:keys [trust resource]} bundles]
                    [trust (slurp (io/resource resource))]))))

(def ^:private parsed (delay (update-vals @pems parse-certificates)))

(defn pem [trust] (@pems trust))

(defn certificates [trust] (@parsed trust))

(defn issued-under [issuer]
  (first (filter #(str/starts-with? (str issuer) (:issuer-prefix %)) bundles)))
