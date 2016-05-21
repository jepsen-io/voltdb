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

(defn rand-non-empty-subset
  "Take some elements from coll as a set"
  [coll]
  (set (take (inc (rand-int (count coll))) (shuffle coll))))

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
  {:id      id                  ; Our node id
   :alive?  true                ; Is this node alive
   :leader? false               ; Is this node a leader
   :prev-cluster (set nodes)    ; The previous set of cluster node ids
   :cluster (set nodes)         ; Set of cluster node ids
   :applied (sorted-set)        ; Set of applied writes
   :waiting {}                  ; Map of op ids to sets of nodes waiting
   :returns {}})                ; Map of op ids to planned return values

(defn state
  "A fresh state with n nodes"
  [n]
  (let [node-ids (range n)]
    {:next-op   0
     :nodes     (-> (zipmap node-ids (map (partial node node-ids)node-ids))
                    (assoc-in [0 :leader?] true))
     :net       (net node-ids)
     :written   (sorted-set)
     :history   []}))

(defn rand-node-id
  "Random node id in state"
  [state]
  (rand-nth (keys (:nodes state))))

;; Invariants

(defn stale-reads
  "Stale reads are possible when a returned op is not present on a leader."
  [state]
  (let [written (:written state)]
    (->> state
         :nodes
         vals
         (filter :leader?)
         (keep (fn [node]
                 (let [lost (set/difference written (:applied node))]
                   (when-not (empty? lost)
                     {:node (:id node)
                      :stale lost}))))
         seq)))

(defn lost-writes
  "We detect lost writes by the presence of a :lost key in the state."
  [state]
  (when-let [lost (:lost state)]
    {:lost-writes lost}))

(defn returns-waiting
  "Ensures waiting and return maps are in sync."
  [state]
  (when-not (->> state
                 :nodes
                 vals
                 (every? (fn [node]
                           (= (set (keys (:waiting node)))
                              (set (keys (:returns node)))))))
      {:returns-waiting :not-equal}))

(defn invariant
  "All invariants of interest."
  [state]
  (or (lost-writes state)
      (returns-waiting state)))

;; Operations

(defn op
  "Generates a new op for a state. Returns [op-type value].

  There are two types of operations.

  :write  A write operation adds the given value to the :applied set of each
          node, and returns to the :written set.
  :check  A check operation checks to see if any of the given values are
          missing, makes no changes to the node's state, and returns to the
          :lost set, if any ops were not found."
  [s]
  (if (< (rand) 0.5)
    [:write (:next-op s)]
    [:check (:written s)]))

(defn apply-op
  "Applies an op to a node and returns [node', ok?, return-val]"
  [node [type value]]
  (case type
    :write [(update node :applied conj value) value]
    :check [node (set/difference value (:applied node))]))

;; State transitions

(defn step-start-op
  "Picks a live leader, applies an op to that node locally, records an intended
  return value, and broadcasts an op message to all nodes in the leader's
  cluster."
  [state]
  (when-let [id (->> state :nodes vals
                     (filter :leader?)
                     (filter :alive?)
                     seq rand-nth :id)]
    (let [op-id               (:next-op state)
          [type value :as op] (op state)
          leader              (get (:nodes state) id)
          recipients          (-> (:cluster leader)
                                  (disj id))
          [leader' retval]    (apply-op leader op)
          leader'             (-> leader'
                                  (assoc-in [:waiting op-id] recipients)
                                  (assoc-in [:returns op-id] [type retval]))]
      (assoc state
             :next-op (inc op-id)
             :nodes   (assoc (:nodes state) id leader')
             :net     (broadcast (:net state) id recipients [:apply op-id op])
             :history (conj (:history state) {:step          :start-op
                                              :node          id
                                              :op-id         op-id
                                              :op            op
                                              :broadcast-to  recipients})))))

(defn step-recv-msg
  "Processes a random message on an alive node, or returns nil if no messages
  pending. Emits a response to the sender. Nodes reject message from outside
  their cluster."
  [s]
  (when-let [[net' a b msg] (recv-msg (:net s))]
    (let [node (get (:nodes s) b)]
      (when (and (:alive? node) ((:cluster node) a))
        (condp = (first msg)
          ; Apply message locally and acknowledge
          :apply (let [[_ op-id [type value :as op]] msg
                       [node' res] (apply-op node op)
                       response-msg [:ack op-id res]]
                   (-> s
                       (assoc-in [:nodes b] node')
                       (assoc :net (send-msg net' b a response-msg))
                       (update :history conj {:step  :apply
                                              :node  b
                                              :from  a
                                              :op-id op-id
                                              :op    op})))

        ; Handle an acknowledgement by removing it from the op's wait set.
        ; If it's already been removed, noop.
        :ack (let [[_ op-id res] msg
                   waiting (get (:waiting node) op-id)
                   s' (if waiting
                        (update-in s [:nodes b :waiting op-id] disj a)
                        s)]
               (-> s'
                   (assoc :net net')
                   (update :history conj {:step  :ack
                                          :node  b
                                          :from  a
                                          :op-id op-id
                                          :noop? (not waiting)})))

        ; Handle a negative acknowledgement by canceling the operation
        ; entirely, removing it from pending and returns.
        :nack (let [[_ op-id] msg]
                (-> s
                    (update-in [:nodes b :waiting] dissoc op-id)
                    (update-in [:nodes b :returns] dissoc op-id)
                    (assoc :net net')
                    (update :history conj {:step :nack
                                           :node b
                                           :from a
                                           :op-id op-id}))))))))

(defn return-op
  "Takes a state, a node, and an op id to return. Clears the op id from the
  node's waiting and returns state, and returns a value to the client (global
  state).

    :write ops are added to the state's :written set
    :check sets, if nonempty, are added to the state's :lost set"
  [s node op-id]
  (let [[type value :as return] (get (:returns node) op-id)]
    (-> (case type
          :write (update s :written conj value)
          :check (if (empty? value)
                   s
                   (update s :lost set/union value)))
        (update-in [:nodes (:id node) :waiting] dissoc op-id)
        (update-in [:nodes (:id node) :returns] dissoc op-id)
        (update :history conj {:step   :return-op
                               :node   (:id node)
                               :op-id  op-id
                               :return return}))))

(defn step-return-op
  "An alive node with an empty waiting set for a given op can use its :returns
  map to return a value to the client (global state)."
  [s]
  (->> (shuffle (vals (:nodes s)))
       (filter :alive?)
       (keep (fn [node]
;               (prn :considering node)
               (->> (shuffle (vec (:waiting node)))
                    (keep (fn [[op-id waiting-on]]
                            (when (empty? waiting-on)
                              (return-op s node op-id))))
                    first)))
       first))

(defn step-conn-lost
  "A network connection could drop, discarding all messages in flight."
  [s]
  (-> s
      (update :net drop-conn)
      (update :history conj {:step :conn-lost})))

(defn apply-cluster-change
  "Applies a cluster change to the local node, returning new node state.
  Returns nil if change would be invalid."
  [node change]
  (when (= (:cluster node) (:cluster change))
    (assoc node
           :leader?      (= (:id node) (:leader change))
           :prev-cluster (:cluster node)
           :cluster      (:cluster' change))))

(defn step-resolve-fault
  "Take an alive node which believes itself to be a part of its cluster. Take
  all nodes which share our belief about the cluster state, and declare at
  least one of them dead, forming a new candidate cluster. Then broadcast a
  message for them to adopt the newly selected membership.

  If the new cluster contains no leader, chooses a new one."
  [s]
  (when-let [node (->> s :nodes vals
                       (filter :alive?)
                       (filter #(contains? (:cluster %) (:id %)))
                       seq
                       rand-nth)]
    (let [c (:cluster node)
          ; In establishing consensus for the new set, we're going to
          ; deal only with live nodes--won't even try to talk to or include
          ; dead ones. We also exclude any nodes which disagree on our
          ; current cluster.
          candidates (->> (disj c (:id node))
                          (map (:nodes s))
                          (filter :alive?)
                          (filter #(= c (:cluster %)))
                          (map :id)
                          set)]
      (when (seq candidates)
        (let [dead    (rand-non-empty-subset candidates)
              c'      (set/difference c dead)
              leaders (->> (:nodes s)
                           (filter :leader?)
                           (filter :alive?)
                           (map :id)
                           set)
              leader' (or (some c' leaders)
                          (->> c' seq rand-nth))
              change  {:cluster   c
                       :cluster'  c'
                       :leader    leader'}]
          (-> s
              ; Apply change locally
              (update-in [:nodes (:id node)] (appply-cluster-change change))
              ; Broadcast to peers
              (assoc-in [:nodes (:id node) :cluster ; TODO HERE
              (assoc :nodes (->> (:nodes s)
                                 (map (fn [[id n]]
                                        (if-not (c' id)
                                          ; Not part of the new cluster; skip
                                          [id n]
                                          ; Part of the new cluster; update
                                          (let [n (if (= id leader')
                                                    (assoc n :leader? true)
                                                    n)]
                                            [id (assoc n
                                                       :prev-cluster
                                                       (:cluster n)
                                                       :cluster c')]))))
                                 (into {})))
              (update :history conj {:step      :resolve-fault
                                     :node      (:id node)
                                     :dead      dead
                                     :cluster   c
                                     :cluster'  c'
                                     :leaders   leaders
                                     :leader'   leader'})))))))

(defn step-detect-partition
  "A node can continue running if its current cluster comprises a majority (or
  is exactly half but contains the blessed node) of the previously known
  cluster."
  ([s]
   (when-let [node-id (->> s :nodes (filter :alive?) rand-nth :id)]
    (step-detect-partition s node-id)))
  ([s id]
   (let [node (-> s :nodes (get id))
         c0   (:prev-cluster node)
         c    (:cluster node)
         frac (/ (count c) (count c0))
         maj? (or (< 1/2 frac)
                  (and (= 1/2 frac) (= 0 id)))]
     (when (and (not maj?) (:alive? node))
       (-> s
           (assoc-in [:nodes id :alive?] false)
           (assoc-in [:nodes id :leader] false)
           (assoc-in [:nodes id :waiting] {})
           (assoc-in [:nodes id :returns] {})
           (update :history conj {:step  :detect-partition
                                  :node  id}))))))

(defn step-clear-waiting
  "After fault resolution and partition detection, nodes can clear out any
  waiting entries from nodes no longer in their cluster."
  ([s id]
   (let [node      (get (:nodes s) id)
         waiting   (:waiting node)
         cluster   (:cluster node)
         waiting'  (->> waiting
                        (map (fn [[op nodes]]
                               [op (set/intersection nodes cluster)]))
                        (into {}))]
     (when (not= waiting waiting')
       (-> s
           (assoc-in [:nodes id :waiting] waiting')
           (update :history conj {:step     :clear-waiting
                                  :node     id
                                  :waiting  waiting
                                  :waiting' waiting'}))))))

(defn on-live
  "Applies (step state node-id) to every live node in the cluster--when steps
  return nil, preserves state as is. Returns new state."
  [state step]
  (->> state :nodes vals (filter :alive?) (map :id)
       (reduce (fn [state node-id]
                 (or (step state node-id) state))
               state)))

(defn step
  "Generalized state transition"
  [state]
  (or (step-return-op state)
      (when (< (rand) 0.01)
        (step-conn-lost state))
      (when (< (rand) 0.1)
        ; Atomic process: fault resolution on any node, then a full round of
        ; partition detection, then a full round of clear-waiting.
        (-> state
            step-resolve-fault
            (on-live step-detect-partition)
            (on-live step-clear-waiting)))
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
                                    (bad-history invariant))))
                       (sort-by (comp count :states))
                       first))))
         doall
         (map deref)
         (remove nil?)
         (sort-by (comp count :states))
         first)))
