(ns tweetstorm.twitter)

(defprotocol TwitterClient
  (receive [this name text created]))

(defn connect-to-twitter
  "connects to twitter"
  [stream]
  (doto stream  (.setOAuthConsumer  "Wc4EBbrYhJSmq6HcB2fA" "4qltgQI2eMB9Oo2gK3JiAv8DeDq3XLkOMXUCxUYf2cg")
        (.setOAuthAccessToken (twitter4j.auth.AccessToken. "18787342-Bz6C3otv36lVnsVbgUmv5baDUezCkhTIiV8p8MYnY" "CPYa7sf3Ss4GezrYGvIc9xnuIvxfcsoXirDLZrhH4k"))))

(defn create-listener
  "Creates a status listener"
  [callback]
  (reify
    twitter4j.StatusListener
    (onStatus [_ status]  (if (= "en" (.toLowerCase (.getLang (.getUser status)))) (.receive callback (.getScreenName (.getUser status)) (.getText status) (.getCreatedAt status))))
      (onDeletionNotice [_ statusDeletionNotice])
      (onTrackLimitationNotice [_ numberOfLimitedStatuses])
      (onStallWarning [_ stallWarning])
      (onScrubGeo [_ userId upToStatusId])
      (onException [_ exception])))

(defn stream-tweets
  "Streams tweets"
  [callback]
  (let [stream (.getInstance (twitter4j.TwitterStreamFactory.))]
    (do
      (connect-to-twitter stream)
      (doto stream
        (.addListener (create-listener callback))
        (.sample)))))
