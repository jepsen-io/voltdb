(ns replication-model.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [replication-model.core :refer :all]))

(deftest net-nodes-test
  (is (-> (state 3)
          :net
          net-nodes
          sort
          (= [0 1 2]))))

(deftest broadcast-test
  (is (-> (state 3)
          :net
          (broadcast 1 :hi)
          (= {[0 0] []
              [0 1] []
              [0 2] []
              [1 0] [:hi]
              [1 1] []
              [1 2] [:hi]
              [2 0] []
              [2 1] []
              [2 2] []}))))

(deftest recv-msg-test
  (let [n (-> (state 3)
              :net
              (send-msg 1 0 :a)
              (send-msg 2 0 :b)
              (send-msg 1 0 :c))]
    (testing "ordered messages from 1"
      (is (= :a (last (recv-msg n 1 0))))
      (is (= :c (-> (recv-msg n 1 0) first
                    (recv-msg 1 0) last))))

    (testing "no order between queues"
      (is (= :b (last (recv-msg n 2 0)))))

    (testing "nil response for no more messages"
      (is (= nil (-> (recv-msg n 2 0) first
                     (recv-msg 2 0)))))

    (testing "all messages for 0"
      (is (= #{:a :b :c}
             (loop [net n
                    msgs #{}]
               (if-let [[net' _ _ msg] (recv-msg net 0)]
                 (recur net' (conj msgs msg))
                 msgs)))))))

(deftest txn-test
  (binding [clojure.pprint/*print-miser-width* 110
            clojure.pprint/*print-right-margin* 110]
    (pprint (nth (iterate step (state 3)) 100))

    (let [v (violations (state 3) 100000 20)
          vb (boolean v)]
      (or (is (not vb))
          (do (println "Found violation in" (dec (count (:states v)))
                       "transitions:")
              (doseq [s (:states v)]
                (prn)
                (pprint s))
              (prn)
              (println "Violation was:")
              (pprint (:error v)))))))
