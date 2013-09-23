(ns tweetstorm.core
  (:import [backtype.storm StormSubmitter LocalCluster] [com.google.common.cache CacheBuilder] [java.util.concurrent TimeUnit] [java.lang Math])
  (:use [backtype.storm clojure config])
  (:gen-class :main true))

(require '[tweetstorm.twitter :as twitter])

(defprotocol SpoutSource
  (next-tweet [this]))

(defn add-to
  "adds an item to the list"
  [tweets item]
  (swap! tweets conj item))

(defn first-from
  "first item"
  [tweets]
  (let [item (first @tweets)]
    (do (swap! tweets rest) item)))

(defn take-from
  "returns the first item"
  [tweets]
  (do (while (empty? @tweets) (Thread/sleep 5)) (first-from tweets)) )

(defrecord Tweet [created user tweet])

(deftype TwitterBridge [tweets]
  SpoutSource
  (next-tweet [this] (take-from tweets))
  twitter/TwitterClient
  (receive [this name text created] (add-to tweets [name text created])))

(def bridge (TwitterBridge. (atom [])))

(defn stop-word?
  [word]
  (contains? #{"lol" "and" "a" "the" "rt" "co" "http" "you" "to" "me" "que" "de" "no" "my" "it" "is" "la" "in" "for" "of" "on" "that" "so" "se" "do"} word))

(defn candidate?
  [[word count] counts]
  (let [[_ current-last] (last counts)]
    (> count current-last)))

(defn sort-by-value
  [previous]
  (into (sorted-map-by (fn [key1 key2] (compare [(get previous key2) key2] [(get previous key1) key1]))) previous))

(defn add-and-sort-top
  [number-in-leaderboard entry counts]
  (let [with-addition (conj counts entry)]
    (sort-by-value (into {} (take number-in-leaderboard (sort-by-value with-addition))))))

(defn split-tweet
  [tweet]
  (try  (seq (.split #"[\s\W\p{C}]+" tweet))
        (catch Exception e (to-array []))))
(defn clean-tweet
  "removes white space and punctuation and downcases the words of the tweet"
  [tweet]
  (filter #(and (not (or (empty? %) (= 1 (count %)) (stop-word? %))) ) (map #(.toLowerCase %) (split-tweet tweet))))

(defn create-cache
  [expiry]
  (.build (.expireAfterWrite (CacheBuilder/newBuilder) expiry (TimeUnit/MINUTES))))

(defn nil-get
  [collection key]
  (let [value (collection key)]
    (if (nil? value)
      0
      value)))

(defn sum-of-products
  [f1 f2]
  (reduce + (map #(* (nil-get f2  %) (f1 %)) (keys f1))))

(defn sum-of-squares
  [f]
  (reduce + (map (fn [val] (* val val)) (vals f))))

(defn product-of-roots-of-sums-of-squares
  [f1 f2]
  (let [sum-of-f1-squares (sum-of-squares f1)
        sum-of-f2-squares (sum-of-squares f2)]
    (* (Math/sqrt sum-of-f1-squares) (Math/sqrt sum-of-f2-squares)))
  )

(defn similarity
  [first second]
  (let [f1 (frequencies (clean-tweet first))
        f2 (frequencies (clean-tweet second))
        topline (sum-of-products f1 f2)
        bottomline (product-of-roots-of-sums-of-squares f1 f2)]
    (if (> bottomline 0) (/ topline bottomline) 0.0)
    ))

(defspout twitter-spout ["name" "tweet" "created"]
  [conf context collector]
  (spout
   (nextTuple []
     (emit-spout! collector (.next-tweet bridge)))))


(defbolt punctuation-stripper ["cleaned"]
  [tuple collector]
  (emit-bolt! collector [(clean-tweet (.getString tuple 1))] :anchor tuple)
  (ack! collector tuple))


(defbolt frequency-counter ["identifier" "count"]
  [tuple collector]
  (doseq [entry (frequencies (.getValue tuple 0))]
    (emit-bolt! collector [ (key entry) (val entry)] :anchor tuple))
  (ack! collector tuple))

(defbolt name-maker ["identifier" "count"]
  [tuple collector]
  (do (emit-bolt! collector [(str "@" (.getValue tuple 0)) 1] :anchor tuple)
      (ack! collector tuple)))

(defbolt printer [] {:params [text]}
  [tuple collector]
  (let [pos (atom 0)]
    (println text)
    (doseq [[identifier total] (.getValue tuple 0)]
      (swap! pos inc)
      (println (str "* " @pos ": " identifier " with " total)))))

(defbolt print-similar [] {:params [period]}
  [tuple collector]
  (let [similarity-score (.getDouble tuple 4)]
    (if (>= similarity-score 0.65)
      (println (str "Similarity in the last " period " minutes :"(.getValue tuple 2) ", " (.getString tuple 0) ", " (.getString tuple 1) ", " (.getDouble tuple 4) )))))

(defbolt running-total ["word" "count" "tweets"] {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (bolt
     (execute [tuple]
       (let [word (.getString tuple 0)
             count (.getLong tuple 1)]
         (swap! counts (partial merge-with (fn [[entries tweets] [i j]] [(+ entries i) (+ tweets j)])) {word [count 1]})
         (let [[counts tweets] (@counts word)]
           (emit-bolt! collector [word counts tweets] :anchor tuple))
         (ack! collector tuple)
         )))))


(defbolt timer ["time"] {:prepare true :params [time-period-in-millis]}
  [conf context collector]
  (let [last-time (atom (System/currentTimeMillis))]
    (bolt
     (execute [tuple]
              (let [current-time (System/currentTimeMillis)]
                (if (> (- current-time @last-time) time-period-in-millis)
                  (do (emit-bolt! collector [time-period-in-millis] :anchor tuple)
                      (reset! last-time current-time)))
                (ack! collector tuple)))
     )))

(defbolt acker ["name" "tweet" "created"]
  [tuple collector]
  (let [name (.getString tuple 0)
        tweet (.getString tuple 1)
        created (.getValue tuple 2)]
    (emit-bolt! collector [name tweet created])
    (ack! collector tuple)))

(defbolt aggregate ["first" "second" "time1" "time2" "score"] {:prepare true :params [window]}
  [conf context collector]
  (let [cache (create-cache window)
        id (atom 0)]
    (bolt
     (execute [tuple]
              (let [first (.getString tuple 0)
                    tweet (.getString tuple 1)
                    time1 (.getValue tuple 2)
                    asMap (.asMap cache)
                    ]
                (swap! id inc)
                (doseq [key (.keySet asMap)]
                  (let [entry (.get asMap key)
                        score (similarity tweet (:tweet entry))]
                    (emit-bolt! collector [first (:user entry) time1 (:created entry) score])))
                (.put cache @id (Tweet. time1 first tweet))
                )))))

(defbolt counter ["total"] {:prepare true}
  [conf context collector]
  (let [total (atom 0)]
    (bolt
     (execute [tuple]
              (if (.contains (.getFields tuple) "time")
                (do (emit-bolt! collector [@total] :anchor tuple)
                    (reset! total 0))
                (swap! total inc))
              (ack! collector tuple)))))

(defbolt top-N ["top"] {:prepare true :params [number-in-leaderboard]}
  [conf context collector]
  (let [top (atom {})]
    (bolt
     (execute [tuple]
              (if (.contains (.getFields tuple) "time")
                (do
                  (emit-bolt! collector [@top] :anchor tuple)
                  (reset! top {}))
                (let [identifier (.getString tuple 0)
                      counter (.getLong tuple 1)]
                  (if (< (count @top) number-in-leaderboard)
                    (swap! top (partial merge {identifier counter}))
                    (if (candidate? [identifier counter] @top)
                      (swap! top (partial add-and-sort-top number-in-leaderboard [identifier counter]))))
                  (ack! collector tuple)))))))

(defn create-topology []
  (topology
   {"1" (spout-spec twitter-spout)}

   {"2" (bolt-spec {"1" :shuffle}
                   punctuation-stripper
                   :p 3)
    "3" (bolt-spec {"2" :shuffle}
                   frequency-counter
                   :p 3)
    "4" (bolt-spec {"3" :shuffle}
                   running-total)
    "5" (bolt-spec {"4" :shuffle "7" :shuffle}
                   (top-N 20))
    ;; 5-minute timer
    "6" (bolt-spec {"1" :shuffle}
                   (timer 300000))
    ;; one hour timer
    "7" (bolt-spec {"1" :shuffle}
                   (timer 3600000))
    ;; five minute tweeter counter
    "8" (bolt-spec {"1" :shuffle}
                   name-maker
                   :p 3)
    "9" (bolt-spec {"8" :shuffle}
                   running-total)
    "10" (bolt-spec {"9" :shuffle "6" :shuffle}
                    (top-N 10))
    ;; one hour tweeter counter
    "11" (bolt-spec {"1" :shuffle}
                   name-maker
                   :p 3)
    "12" (bolt-spec {"11" :shuffle}
                   running-total)
    "13" (bolt-spec {"12" :shuffle "7" :shuffle}
                    (top-N 10))
    "14" (bolt-spec {"5" :shuffle}
                    (printer "Top words in the past hour"))
    "15" (bolt-spec {"10" :shuffle}
                    (printer "Top tweeters in the past five minutes"))
    "16" (bolt-spec {"13" :shuffle}
                    (printer "Top tweeters in the past hour"))
    "17" (bolt-spec {"1" :shuffle}
                    acker
                    :p 10)
    "18" (bolt-spec {"17" :shuffle}
                    (aggregate 5)
                    :p 10)
    "19" (bolt-spec {"18" :shuffle}
                    (print-similar 5)
                    :p 2)
    "20" (bolt-spec {"1" :shuffle}
                    acker
                    :p 10)
    "21" (bolt-spec {"1" :shuffle}
                    (aggregate 20)
                    :p 20)
    "22" (bolt-spec {"21" :shuffle}
                    (print-similar 20)
                    :p 2)
    }))


(defn run-local! [source]
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "tweetstorm" {TOPOLOGY-DEBUG false} (create-topology))
    (Thread/sleep 3660000)
    (.shutdown cluster)
    ))


(defn -main
  "Main method"
  [& args]
  (do (twitter/stream-tweets bridge)
      (run-local! bridge)))
