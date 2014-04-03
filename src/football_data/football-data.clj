(ns football-data.football-data
  (:require
   ;;[org.httpkit.client :as http]
   ;;[clojure.core.async :refer [<! >! put! <!! chan close! timeout go]]
   [clojure-csv.core :refer [parse-csv]]
   [clj-time.core :as t]
   [clj-time.format :as f]
   )
  )


;; football data utils
;; ============================================================================
(defn football-data-url [season league]
  (str "http://www.football-data.co.uk/mmz4281/" season "/" league ".csv"))



(defn default-parser [string]
  (try
    (read-string string)
    (catch Exception e nil)))


(def DATE-FORMATTER (f/formatter "dd/MM/YY"))

(defn date-parser [date-str]
  (let [clj-time (f/parse DATE-FORMATTER date-str)
        util-date (.toDate clj-time)]
    util-date))

(defn cast-nfo-fabric [default-parser custom-parsers]
  (fn [map-to-parse]
    (let [ks (keys map-to-parse)
          parsers-map-default (zipmap ks (repeat default-parser))
          relevant-custom-parsers (select-keys custom-parsers ks)
          parsers-map (merge parsers-map-default relevant-custom-parsers)]
      parsers-map
    )))


(def cast-nfo-maker (cast-nfo-fabric default-parser {:Date date-parser}))


(defn read-games [csv-data]
  (let [data (parse-csv csv-data)
        [headers & games] data
        key-headers (map keyword headers)
        make-game-map #(zipmap key-headers %)
        game-maps (map make-game-map games)]
    game-maps))


(defn cast-game [game]
  (let [
        cast-nfo (cast-nfo-maker game)
        make-pair (fn [[k func]]
                       [k (func (k game))])
        pairs (map make-pair cast-nfo)
        casted-game (into {} pairs)]
    casted-game))

(defn cast-games [raw-games]
  (let [game-caster #(cast-game %)]
    (map game-caster raw-games)))

(defn parse-season [csv]
  (let [raw-games (read-games csv)
        casted-games (cast-games raw-games)]
     casted-games))

(defn parse-football-data [csv]
  (parse-season csv))

