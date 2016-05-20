(ns replication-model.core
  "A model of voltdb's replication algorithm"
  (:require [clojure.set :as set]
            [clojure.pprint :refer [pprint]]))

;; Util

(defn ->pprint [x desc]
  (prn)
  (prn desc)
  (pprint x)
  x)

;; Network

(defn net
  "A network is a set of dequeues indexed by [sender recipient] ids, each
  representing a TCP socket."
  [node-ids]
  (into {} (for [a node-ids, b node-ids]
             [[a b] (clojure.lang.PersistentQueue/EMPTY)])))

(defn net-nodes
  "What nodes are in a net?"
  [net]
  (distinct (map first (keys net))))

(defn net-empty?
  "Are any messages in the net?"
  [net]
  (every? empty? (vals net)))

(defn send-msg
  "Sends a message on the given network from a to b. Returns net'."
  [net a b msg]
  (assert (contains? net [a b]))
  (update net [a b] conj msg))

(defn broadcast
  "Sends a message from node a to bs, or all other nodes in net. Returns net'."
  ([net a msg]
   (broadcast net a (remove #{a} (net-nodes net)) msg))
  ([net a bs msg]
   (reduce (fn [net b] (send-msg net a b msg))
           net
           bs)))

(defn recv-msg
  "Receives the next message from the network from a to b, or a random message
  for b, if no a is given, or any random message if no nodes are given. Returns
  [net' a b msg], or nil if no message pending."
  ([net a b]
   (let [k [a b]
         q (get net k)]
     (when-let [msg (peek q)]
       [(assoc net k (pop q)) a b msg])))
  ([net b]
   (->> net
        net-nodes
        shuffle
        (keep #(recv-msg net % b))
        first))
  ([net]
   (->> net
        keys
        shuffle
        (keep (fn [[a b]] (recv-msg net a b)))
        first)))

(defn drop-conn
  "Drops a random or given network connection."
  ([net]
   (let [nodes (net-nodes net)]
     (drop-conn net (rand-nth nodes) (rand-nth nodes))))
  ([net a b]
   (assoc net [a b] (clojure.lang.PersistentQueue/EMPTY))))

;; State

(defn node
  "A fresh node with the given id"
  [nodes id]
  {:id      id
   :leader? false
   :cluster (set nodes)
   :applied (sorted-set)
   :waiting {}})

(defn state
  "A fresh state with n nodes"
  [n]
  (let [node-ids (range n)]
    {:next-op   0
     :nodes     (-> (zipmap node-ids (map (partial node node-ids)node-ids))
                    (assoc-in [0 :leader?] true))
     :net       (net node-ids)
     :returned  (sorted-set)
     :history   []}))

(defn rand-node-id
  "Random node id in state"
  [state]
  (rand-nth (keys (:nodes state))))

;; Invariants

(defn lost-writes
  "Lost writes are those which have been returned but are not present on a
  leader."
  [state]
  (let [returned (:returned state)]
    (->> state
         :nodes
         vals
         (filter :leader?)
         (keep (fn [node]
                 (let [lost (set/difference returned (:applied node))]
                   (when-not (empty? lost)
                     {:node (:id node)
                      :lost lost}))))
         seq)))

;; State transitions

(defn step-start-op
  "Picks a leader, applies an op to that node locally, and broadcasts an
  [:apply op] message to all nodes in the leader's cluster."
  [state]
  (when-let [id (->> state :nodes vals (filter :leader?) seq rand-nth :id)]
    (let [op         (:next-op state)
          leader     (get (:nodes state) id)
          recipients (-> (:cluster leader)
                         (disj id))
          leader' (-> leader
                      (update :applied conj op)
                      (assoc-in [:waiting op] recipients))]
      (assoc state
             :next-op (inc op)
             :nodes   (assoc (:nodes state) id leader')
             :net     (broadcast (:net state) id recipients [:apply op])
             :history (conj (:history state) {:step          :start-op
                                              :node          id
                                              :op            op
                                              :broadcast-to  recipients})))))

(defn step-recv-msg
  "Processes a random message, or returns nil if no messages pending."
  [s]
  (when-let [[net' a b msg] (recv-msg (:net s))]
    (condp = (first msg)
      ; Apply message locally and acknowledge
      :apply (let [op (second msg)]
               (-> s
                   (update-in [:nodes b :applied] conj op)
                   (assoc :net (send-msg net' b a [:ack op]))
                   (update :history conj {:step :apply
                                          :node b
                                          :from a
                                          :op   op})))

      ; Handle an acknowledgement by removing it from the op's wait set.
      :ack (let [op (second msg)]
             (-> s
                 (update-in [:nodes b :waiting op] disj a)
                 (assoc :net net')
                 (update :history conj {:step :ack
                                        :node b
                                        :from a
                                        :op   op}))))))

(defn step-return-op
  "A node with an empty waiting set for a given write can return a write to the
  client."
  [s]
  (->> (shuffle (vals (:nodes s)))
       (keep (fn [node]
               (->> (shuffle (vec (:waiting node)))
                    (keep (fn [[op waiting-on]]
                            (when (empty? waiting-on)
                              ; We can return this.
                              (-> s
                                  (update :returned conj op)
                                  (update-in [:nodes (:id node) :waiting]
                                             dissoc op)
                                  (update :history conj {:step :return-op
                                                         :node node
                                                         :op   op})))))
                    first)))
       first))

(defn step-conn-lost
  "A network connection could drop, discarding all messages in flight."
  [s]
  (-> s
      (update :net drop-conn)
      (update :history conj {:step :conn-lost})))

(defn step-resolve-fault
  "Take a node which believes itself to be a part of its cluster. Declare
  another node in the cluster, preferably a leader, dead, and atomically shrink
  all remaining nodes' clusters to the remaining set of live nodes.

  If the new cluster contains no leader, chooses a new one."
  [s]
  (when-let [node (->> s :nodes vals
                       (filter #(contains? (:cluster %) (:id %)))
                       seq
                       rand-nth)]
    (let [c (:cluster node)]
      (when-let [candidates (seq (disj c (:id node)))]
        (let [dead    (rand-nth candidates)
              c'      (disj c dead)
              leaders (->> c
                           (map (:nodes s))
                           (filter :leader?)
                           (map :id)
                           set)
              leader' (or (some c' leaders)
                          (->> c' seq rand-nth))]
          (-> s
              (assoc :nodes (->> (:nodes s)
                                 (map (fn [[id n]]
                                        (if-not (c' id)
                                          ; Not part of the new cluster; skip
                                          [id n]
                                          ; Part of the new cluster; update
                                          (let [n (if (= id leader')
                                                    (assoc n :leader? true)
                                                    n)]
                                            [id (assoc n :cluster c')]))))
                                 (into {})))
              (update :history conj {:step      :resolve-fault
                                     :node      (:id node)
                                     :dead      dead
                                     :cluster   c
                                     :cluster'  c'
                                     :leaders   leaders
                                     :leader'   leader'})))))))

(defn step
  "Generalized state transition"
  [state]
  (or (step-return-op state)
      (when (< (rand) 0.1)
        (step-conn-lost state))
      (when (< (rand) 0.1)
        (step-resolve-fault state))
      (when (< (rand) 0.9)
        (step-recv-msg state))
      (step-start-op state)
      state))

(defn bad-history
  "Given a sequence of states and an error predicate, applies the predicate to
  every state and finds the shortest prefix of history where the predicate
  holds. Yields {:states [...] :error ...} or nil if no error found."
  ([pred states]
   (bad-history pred [] states))
  ([pred passed states]
    (when (seq states)
      (if-let [err (pred (first states))]
        {:states (conj passed (first states))
         :error  err}
        (recur pred (conj passed (first states)) (next states))))))

(defn violations
  "Performs n explorations of len steps, looking for an invariant violation."
  [state n len]
  (let [procs (.. Runtime getRuntime availableProcessors)]
    (->> (range procs)
         (map (fn [_]
                (future
                  (->> (range (Math/ceil (/ n procs)))
                       (keep (fn [i]
                               (->> state
                                    (iterate step)
                                    (take len)
                                    (bad-history lost-writes))))
                       (sort-by (comp count :states))
                       first))))
         doall
         (map deref)
         (remove nil?)
         (sort-by (comp count :states))
         first)))
