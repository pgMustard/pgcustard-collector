(ns pgcustard.literals
  (:require [clojure.string :as str])
  (:import [java.util.regex Pattern]))

(def ^:private ^Pattern dollar-quote-tag #"\$(?:[A-Za-z_\u0080-￿][A-Za-z0-9_\u0080-￿]*)?\$")

(def ^:private ^Pattern digit-groups #"[0-9]+(?:[ -][0-9]+)*")

(def ^:private ascii-digit? (set "0123456789"))

(def ^:private string-prefixes {"e" true "b" false "x" false "n" false})

(defn- char-at [^String s i]
  (when (< i (.length s))
    (.charAt s (int i))))

(defn- identifier-start? [c]
  (and (some? c)
       (or (Character/isLetter (char c)) (= \_ c) (> (int c) 127))))

(defn- identifier-part? [c]
  (and (some? c)
       (or (identifier-start? c) (Character/isDigit (char c)) (= \$ c))))

(defn- identifier-end [^String s i]
  (loop [j i]
    (if (identifier-part? (char-at s j)) (recur (inc j)) j)))

(defn- line-end [^String s i]
  (let [j (.indexOf s "\n" (int i))]
    (if (neg? j) (.length s) j)))

(defn- block-comment-end [^String s i]
  (loop [j (+ i 2) depth 1]
    (cond (zero? depth)                                         j
          (>= j (.length s))                                    (.length s)
          (and (= \/ (char-at s j)) (= \* (char-at s (inc j)))) (recur (+ j 2) (inc depth))
          (and (= \* (char-at s j)) (= \/ (char-at s (inc j)))) (recur (+ j 2) (dec depth))
          :else                                                 (recur (inc j) depth))))

(defn- quoted-end [^String s i quote backslash-escapes?]
  (loop [j (inc i)]
    (let [c (char-at s j)]
      (cond (nil? c)                                         (.length s)
            (and backslash-escapes? (= \\ c))                (recur (+ j 2))
            (and (= quote c) (= quote (char-at s (inc j)))) (recur (+ j 2))
            (= quote c)                                      (inc j)
            :else                                            (recur (inc j))))))

(defn- dollar-quote-end [^String s i]
  (let [m (doto (.matcher dollar-quote-tag s) (.region (int i) (.length s)))]
    (when (.lookingAt m)
      (let [tag   (.group m)
            close (.indexOf s tag (.end m))]
        (if (neg? close) (.length s) (+ close (count tag)))))))

(defn- luhn-digit [i d]
  (if (odd? i)
    (let [doubled (* 2 d)] (if (> doubled 9) (- doubled 9) doubled))
    d))

(defn- luhn? [^String digits]
  (let [sum (->> (reverse digits)
                 (map #(Character/digit (char %) 10))
                 (map-indexed luhn-digit)
                 (reduce +))]
    (zero? (mod sum 10))))

(defn- digit-runs [groups]
  (for [from (range (count groups))
        run  (->> (subvec groups from) (reductions str) (take-while #(<= (count %) 19)))]
    run))

(defn- luhn-number? [^String text]
  (some #(and (<= 10 (count %)) (luhn? %))
        (digit-runs (str/split text #"[ -]"))))

(defn- number-token [^String s i]
  (let [m (doto (.matcher digit-groups s) (.region (int i) (.length s)))]
    (.lookingAt m)
    [(if (luhn-number? (.group m)) :literal :keep) (.end m)]))

(defn- luhn-numbers-between [^String s start end]
  (let [m (doto (.matcher digit-groups s) (.region (int start) (int end)))]
    (loop [spans []]
      (if (.find m)
        (recur (cond-> spans (luhn-number? (.group m)) (conj [(.start m) (.end m)])))
        spans))))

(defn- word-token [^String s i]
  (let [end  (identifier-end s i)
        word (str/lower-case (subs s i end))]
    (cond (and (contains? string-prefixes word) (= \' (char-at s end)))
          [:literal (quoted-end s end \' (string-prefixes word))]

          (and (= "u" word) (= \& (char-at s end)) (= \' (char-at s (inc end))))
          [:literal (quoted-end s (inc end) \' false)]

          :else
          [:keep end])))

(defn- token [^String s i]
  (let [c    (char-at s i)
        next (char-at s (inc i))]
    (cond (and (= \- c) (= \- next)) [:comment (line-end s i)]
          (and (= \/ c) (= \* next)) [:comment (block-comment-end s i)]
          (= \" c)                   [:keep (quoted-end s i \" false)]
          (= \' c)                   [:literal (quoted-end s i \' false)]
          (= \$ c)                   (if-let [end (dollar-quote-end s i)] [:literal end] [:keep (inc i)])
          (ascii-digit? c)           (number-token s i)
          (identifier-start? c)      (word-token s i)
          :else                      [:keep (inc i)])))

(defn- literal-spans [^String s]
  (loop [i 0 spans []]
    (if (>= i (.length s))
      spans
      (let [[kind end] (token s i)]
        (recur (long end) (case kind
                            :literal (conj spans [i end])
                            :comment (into spans (luhn-numbers-between s i end))
                            spans))))))

(defn- segments [^String s]
  (let [[out kept-from] (reduce (fn [[out kept-from] [start end]]
                                  [(conj out (subs s kept-from start) ::literal) end])
                                [[] 0]
                                (literal-spans s))]
    (conj out (subs s kept-from))))

(defn- highest-parameter [kept]
  (reduce max 0 (for [part kept
                      [_ digits] (re-seq #"\$(\d+)" part)]
                  (or (parse-long digits) 0))))

(defn redact [^String text]
  (let [parts (segments text)]
    (if (some #{::literal} parts)
      (->> parts
           (reduce (fn [[out n] part]
                     (if (= ::literal part)
                       [(conj out (str "$" n)) (inc n)]
                       [(conj out part) n]))
                   [[] (inc (highest-parameter (filter string? parts)))])
           first
           (apply str))
      text)))
