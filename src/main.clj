(ns main
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]
            [clojure.data.csv :as csv]))

(defn start-xtdb! []
  (letfn [(kv-store [dir]
            {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                        :db-dir (io/file dir)
                        :sync? true}})]
    (xt/start-node
     {:xtdb/tx-log (kv-store "data/dev/tx-log")
      :xtdb/document-store (kv-store "data/dev/doc-store")
      :xtdb/index-store (kv-store "data/dev/index-store")})))

(def xtdb-node (start-xtdb!))
;; note that attempting to eval this expression more than once before first calling `stop-xtdb!` will throw a RocksDB locking error
;; this is because a node that depends on native libraries must be `.close`'d explicitly

(defn stop-xtdb! []
  (.close xtdb-node))

(def id (atom 0))

(defn id! []
  (swap! id inc))

(defn with-id [coll]
  (map #(assoc % :xt/id (id!)) coll))

(defn put! [xs]
  (xt/submit-tx xtdb-node
                (->> xs
                     (mapv (fn [x]
                             [::xt/put x])))))

(def data
  [{:a/key "A" :a/name "Alex"}
   {:b/key "A" :b/age 100}
   {:c/key "A" :c/job "developer"}
   {:a/key "B" :a/name "Bob"}
   {:b/key "B" :b/age 50}
   {:a/key "C" :a/name "Charlie"}
   {:b/key "C" :b/age 25}
   {:c/key "C" :c/job "youtuber"}
   {:tree/parent "A"
    :tree/key "B"
    :tree/leaf? false}
   {:tree/parent "B"
    :tree/key "C"
    :tree/leaf? true}])

(defn setup! []
  (put! (with-id data)))
;; (setup!)

(defn base-case []
  (xt/q
    (xt/db xtdb-node)
    '{:find [out]
      :where [(walk-tree "A" h [] out)]
      :rules [[(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key h]
               [j :tree/leaf? true]
               (build-record h data)
               [(conj path data) out]]
              [(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key more]
               [j :tree/leaf? false]
               (build-record more data)
               [(conj path data) path1]
               (walk-tree more h path1 out)]
              [(build-record fk ret)
               [e :a/key fk]
               [e :a/name name]
               [eb :b/key fk]
               [eb :b/age age]
               ;; [ec :c/key fk]
               ;; [ec :job job]
               [t :tree/key fk]
               [t :tree/parent p]
               [(hash-map :key fk
                          :parent p
                          :name name
                          :age age
                          ;; :job job
                          )
                ret]]]}))
;; (base-case)
;; A -> B -> C is hierarchical relationship with A being the grandparent of C
;; #{[[{:age 50, :key "B", :name "Bob", :parent "A"} {:age 25, :key "C", :name "Charlie", :parent "B"}]]}

(defn with-job []
  (xt/q
    (xt/db xtdb-node)
    '{:find [out]
      :where [(walk-tree "A" h [] out)]
      :rules [[(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key h]
               [j :tree/leaf? true]
               (build-record h data)
               [(conj path data) out]]
              [(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key more]
               [j :tree/leaf? false]
               (build-record more data)
               [(conj path data) path1]
               (walk-tree more h path1 out)]
              [(build-record fk ret)
               [e :a/key fk]
               [e :a/name name]
               [eb :b/key fk]
               [eb :b/age age]
               [ec :c/key fk]
               [ec :c/job job]
               [t :tree/key fk]
               [t :tree/parent p]
               [(hash-map :key fk
                          :parent p
                          :name name
                          :age age
                          :job job)
                ret]]]}))
;; (with-job)
;; In this case we expect the result to be empty because we try to extract `:job` joining by `:key`
;; #{} because "B" does not have a job

(defn initialise-as-nil []
  (xt/q
    (xt/db xtdb-node)
    '{:find [out]
      :where [(walk-tree "A" h [] out)]
      :rules [[(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key h]
               [j :tree/leaf? true]
               (build-record h data)
               [(conj path data) out]]
              [(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key more]
               [j :tree/leaf? false]
               (build-record more data)
               [(conj path data) path1]
               (walk-tree more h path1 out)]
              [(build-record fk ret)
               [e :a/key fk]
               [e :a/name name]
               [eb :b/key fk]
               [eb :b/age age]
               [(identity nil) job]
               [t :tree/key fk]
               [t :tree/parent p]
               [(hash-map :key fk
                          :parent p
                          :name name
                          :age age
                          :job job)
                ret]]]})
  )
;; (initialise-as-nil)
;; As I'm still learning datalog this is me proving that the structure of the
;; query works if you just initialise :job to nil without joining anything
;; #{[[{:age 50, :job nil, :key "B", :name "Bob", :parent "A"} {:age 25, :job nil, :key "C", :name "Charlie", :parent "B"}]]}

(defn with-or-join []
  (xt/q
    (xt/db xtdb-node)
    '{:find [out]
      :where [(walk-tree "A" h [] out)]
      :rules [[(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key h]
               [j :tree/leaf? true]
               (build-record h data)
               [(conj path data) out]]
              [(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key more]
               [j :tree/leaf? false]
               (build-record more data)
               [(conj path data) path1]
               (walk-tree more h path1 out)]
              [(build-record fk ret)
               [e :a/key fk]
               [e :a/name name]
               [eb :b/key fk]
               [eb :b/age age]
               (or-join
                 [job fk]
                 (and
                   [ec :c/key fk]
                   [ec :c/job job])
                 (and [(identity nil) job]))
               [t :tree/key fk]
               [t :tree/parent p]
               [(hash-map :key fk
                          :parent p
                          :name name
                          :age age
                          :job job)
                ret]]]}))
;; (with-or-join)
;; here we combine the join clause with the initialise as nil clause to semantically outer join - find :c/job where its available for :key
;; but we get an unexpected exception
;; user=> Execution error (IllegalArgumentException) at xtdb.error/illegal-arg (error.clj:12).
;; Clause refers to unknown variable: path122813 {:pred {:pred-fn #object[clojure.core$conj__5455 0x36194a07 "clojure.core$conj__5455@36194a07"], :args [path122813 data22841]}, :return [:scalar out]}
;; desired output: #{[[{:age 50, :job nil, :key "B", :name "Bob", :parent "A"} {:age 25, :job "youtuber", :key "C", :name "Charlie", :parent "B"}]]}

(defn with-multi-clause-rule []
  (xt/q
    (xt/db xtdb-node)
    '{:find [out]
      :where [(walk-tree "A" h [] out)]
      :rules [[(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key h]
               [j :tree/leaf? true]
               (build-record h data)
               [(conj path data) out]]
              [(get-job-if-available fk job)
               [ec :c/key fk]
               [ec :c/job job]]
              [(get-job-if-available fk job)
               (not [ec :c/key fk])
               [(identity nil) job]]
              [(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key more]
               [j :tree/leaf? false]
               (build-record more data)
               [(conj path data) path1]
               (walk-tree more h path1 out)]
              [(build-record fk ret)
               [e :a/key fk]
               [e :a/name name]
               [eb :b/key fk]
               [eb :b/age age]
               [t :tree/key fk]
               [t :tree/parent p]
               (get-job-if-available fk job)
               [(hash-map :key fk
                          :parent p
                          :name name
                          :age age
                          :job job)
                ret]]]}))
;; (with-multi-clause-rule)
;; here we try to achieve outer join semantics by using a rule that has multiple branches: either a join or initialize the logic variable as nil
;; but we get an unexpected exception
;; user=> Execution error (IllegalArgumentException) at xtdb.error/illegal-arg (error.clj:12).
;; Clause refers to unknown variable: path123296 {:pred {:pred-fn #object[clojure.core$conj__5455 0x36194a07 "clojure.core$conj__5455@36194a07"], :args [path123296 data23328]}, :return [:scalar out]}
;; desired output: #{[[{:age 50, :job nil, :key "B", :name "Bob", :parent "A"} {:age 25, :job "youtuber", :key "C", :name "Charlie", :parent "B"}]]}

(defn with-multi-clause-rule-not-join []
  (xt/q
    (xt/db xtdb-node)
    '{:find [out]
      :where [(walk-tree "A" h [] out)]
      :rules [[(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key h]
               [j :tree/leaf? true]
               (build-record h data)
               [(conj path data) out]]
              [(get-job-if-available fk job)
               [ec :c/key fk]
               [ec :c/job job]]
              [(get-job-if-available fk job)
               ;; (not [ec :c/key fk]) ;; also doesn't work
               (not-join [fk]
                  [ec :c/key fk])
               [(identity nil) job]]
              [(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key more]
               [j :tree/leaf? false]
               (build-record more data)
               [(conj path data) path1]
               (walk-tree more h path1 out)]
              [(build-record fk ret)
               [e :a/key fk]
               [e :a/name name]
               [eb :b/key fk]
               [eb :b/age age]
               [t :tree/key fk]
               [t :tree/parent p]
               (get-job-if-available fk job)
               [(hash-map :key fk
                          :parent p
                          :name name
                          :age age
                          :job job)
                ret]]]}))
;; (with-multi-clause-rule-not-join)
;; user=> Execution error (IllegalArgumentException) at xtdb.error/illegal-arg (error.clj:12).
;; Clause refers to unknown variable: path123466 {:pred {:pred-fn #object[clojure.core$conj__5455 0x36194a07 "clojure.core$conj__5455@36194a07"], :args [path123466 data23499]}, :return [:scalar out]}
;; desired output: #{[[{:age 50, :job nil, :key "B", :name "Bob", :parent "A"} {:age 25, :job "youtuber", :key "C", :name "Charlie", :parent "B"}]]}

(defn with-subq []
  (xt/q
    (xt/db xtdb-node)
    '{:find [out]
      :where [(walk-tree "A" h [] out)]
      :rules [[(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key h]
               [j :tree/leaf? true]
               (build-record h data)
               [(conj path data) out]]
              [(get-job-if-available fk job)
               [(q {:find [job2]
                    :where [[xxx :c/key k]
                            [xxx :c/job job2]]
                    :in [k]}
                   fk)
                [job]]]
              [(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key more]
               [j :tree/leaf? false]
               (build-record more data)
               [(conj path data) path1]
               (walk-tree more h path1 out)]
              [(build-record fk ret)
               [e :a/key fk]
               [e :a/name name]
               [eb :b/key fk]
               [eb :b/age age]
               [t :tree/key fk]
               [t :tree/parent p]
               (get-job-if-available fk job)
               [(hash-map :key fk
                          :parent p
                          :name name
                          :age age
                          :job job)
                ret]]]}))
;; (with-subq)
;; this provide the desired outcome of outer joining job information when present but produces an awkward data structure
;; #{[[{:age 50, :job nil, :key "B", :name "Bob", :parent "A"} {:age 25, :job ["youtuber"], :key "C", :name "Charlie", :parent "B"}]]}


(defn with-subq-shadowing []
  (xt/q
    (xt/db xtdb-node)
    '{:find [out]
      :where [(walk-tree "A" h [] out)]
      :rules [[(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key h]
               [j :tree/leaf? true]
               (build-record h data)
               [(conj path data) out]]
              [(get-job-if-available fk job)
               [(q {:find [job fk]
                    :keys [job fk]
                    :where [[xxx :c/key fk]
                            [xxx :c/job job]]
                    :in [fk]}
                   fk)
                [job]]]
              [(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key more]
               [j :tree/leaf? false]
               (build-record more data)
               [(conj path data) path1]
               (walk-tree more h path1 out)]
              [(build-record fk ret)
               [e :a/key fk]
               [e :a/name name]
               [eb :b/key fk]
               [eb :b/age age]
               [t :tree/key fk]
               [t :tree/parent p]
               (get-job-if-available fk job)
               [(hash-map :key fk
                          :parent p
                          :name name
                          :age age
                          :job job)
                ret]]]}))
;; (with-subq-shadowing)
;; #{[[{:age 50, :job nil, :key "B", :name "Bob", :parent "A"} {:age 25, :job {:h "C", :job21568 "youtuber"}, :key "C", :name "Charlie", :parent "B"}]]}
;; nested datastructure :job {:h "C", :job21568 "youtuber"} should have keys {:fk "C", :job "youtuber"}

(defn with-or-and-not-join []
  (xt/q
    (xt/db xtdb-node)
    '{:find [out]
      :where [(walk-tree "A" h [] out)]
      :rules [[(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key h]
               [j :tree/leaf? true]
               (build-record h data)
               [(conj path data) out]]
              [(walk-tree f h path out)
               [j :tree/parent f]
               [j :tree/key more]
               [j :tree/leaf? false]
               (build-record more data)
               [(conj path data) path1]
               (walk-tree more h path1 out)]
              [(build-record fk ret)
               [e :a/key fk]
               [e :a/name name]
               [eb :b/key fk]
               [eb :b/age age]
               (or
                 (and
                   [ec :c/key fk]
                   [ec :c/job job])
                 (and (not-join [ec fk]
                           [ec :c/key fk]
                           [ec :c/job job])
                      [(identity nil) job]))
               [t :tree/key fk]
               [t :tree/parent p]
               [(hash-map :key fk
                          :parent p
                          :name name
                          :age age
                          :job job)
                ret]]]}))

;; (with-or-and-not-join)
;; try using (or ... (and <join>) (and (not-join ...)))
;; Execution error (NullPointerException) at xtdb.query/build-or-constraints$iter$fn$fn$or-constraint$iter$fn$fn$fn$iter$fn$fn$iter$fn$fn (query.clj:1159).
;; Cannot read field "result_index" because "var_binding" is null
;; desired output: #{[[{:age 50, :job nil, :key "B", :name "Bob", :parent "A"} {:age 25, :job "youtuber", :key "C", :name "Charlie", :parent "B"}]]}
