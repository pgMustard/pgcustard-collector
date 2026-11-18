(ns pgcustard.literals-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgcustard.literals :as literals]))

(deftest a-literal-becomes-the-next-parameter
  (is (= "alter role alice password $1" (literals/redact "alter role alice password 'hunter2'")))
  (is (= "select $1 from t where a = $3 and b = $2"
         (literals/redact "select $1 from t where a = 'x' and b = $2")))
  (is (= "create subscription s connection $1 publication p"
         (literals/redact "create subscription s connection 'host=x password=secret' publication p"))))

(deftest a-text-without-literals-is-unchanged
  (doseq [text ["select $1::varchar(255) from t group by 1, 2 order by 1"
                "select \"it's\", a$b from t where c = $1"
                "/* a row's delta */ select $1 -- it's fine\nfrom t"]]
    (is (identical? text (literals/redact text)))))

(deftest every-kind-of-string-is-redacted
  (testing "doubled quotes stay inside the string"
    (is (= "select $1, $2" (literals/redact "select 'it''s', ''"))))
  (testing "backslashes escape only in E strings"
    (is (= "select $1, $2" (literals/redact "select E'it\\'s', 'x'")))
    (is (= "select $1, $2" (literals/redact "select 'C:\\', 'x'"))))
  (testing "prefixes go with their string"
    (is (= "select $1, $2, $3, $4" (literals/redact "select x'1f', B'01', n'abc', U&'d\\0061t'"))))
  (testing "a type name before a string stays"
    (is (= "select date $1, interval $2" (literals/redact "select date '2020-01-01', interval '1 day'")))
    (is (= "select note $1" (literals/redact "select note 'x'")))))

(deftest dollar-quoted-bodies-are-redacted
  (is (= "do $1" (literals/redact "do $$ begin perform 'x'; end $$")))
  (is (= "create function f() returns int as $1 language sql"
         (literals/redact "create function f() returns int as $fn$ select 1 $$ $fn$ language sql")))
  (testing "parameters and identifiers with $ aren't dollar quotes"
    (is (= "select a$b, $1 from t where c = $2" (literals/redact "select a$b, $1 from t where c = 'x'")))))

(deftest quotes-in-identifiers-and-comments-are-kept
  (is (= "select \"it's\" from t where a = $1" (literals/redact "select \"it's\" from t where a = 'x'")))
  (is (= "select 1 -- it's\nfrom t where a = $1" (literals/redact "select 1 -- it's\nfrom t where a = 'x'")))
  (is (= "/* a /* nested */ row's */ select $1" (literals/redact "/* a /* nested */ row's */ select 'x'")))
  (is (= "/*action='show'*/ select $2 where a = $1" (literals/redact "/*action='show'*/ select 'x' where a = $1"))))

(deftest an-unterminated-literal-redacts-the-rest
  (is (= "select $1" (literals/redact "select 'abc, secret")))
  (is (= "do $1" (literals/redact "do $$ begin perform 'secret';"))))
