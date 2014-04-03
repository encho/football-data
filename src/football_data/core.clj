;; +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
;; Football Data Analysis
;; +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

(ns football-data.core
  (:require
   [clojure.edn :as edn]
   [football-data.football-data :as football]
   [org.httpkit.client :as http]
   [clojure.core.async :refer [<! >! put! <!! chan close! timeout go]]
   [clojure-csv.core :refer [parse-csv]]
   [clj-time.core :as t]
   [clj-time.format :as f]
   )
  )


;; Utils
;; ============================================================================
(defn GET [url]
  (let [ch (chan 1)]
    (http/get url #_options
          (fn [{:keys [status headers body error]}] ;; asynchronous handle response
            (if error
              (put! ch error)
              (put! ch body))
            (close! ch)))
    ch))

(defn save-data [filename data]
    (spit filename (prn-str data)))

(defn read-data [file-path]
  (edn/read-string (slurp file-path)))

(defn permute [seasons leagues]
  (for [s seasons l leagues] [s l]))

(defn football-filename [folder [season league]]
  (str folder "/" season "-" league ".csv"))

(defn fetch-and-save [csv-folder seasons leagues]
  (let [permutations (permute seasons leagues)
        filenames (map #(football-filename csv-folder %) permutations)
        urls (map #(apply football/football-data-url %) permutations)]
        (doseq [url urls filename filenames]
          (let [csv-data (<!! (GET url))]
            (save-data filename csv-data)))))



;; Example Usage
;; ============================================================================

;; Download and parse single season
;; ----------------------------------------------------------------------------

;; define league and season:
(def season "1314")
(def league "I2")

;; make url for the data at http://www.football-data.co.uk:
(def soccer-url (football/football-data-url season league))

;; fetch the csv data for the league and season:
(def csv-data (<!! (GET soccer-url)))

;; parse the data:
(def games (football/parse-football-data csv-data))

;; dump the parsed games data to a file:
(save-data "data/edn/test.edn" games)

;; read edn games from a file:
(read-data "data/edn/test.edn")


;; Bulk download and save csv data
;; ----------------------------------------------------------------------------

;; define folder and relevant seasons/leagues
(def csv-folder "data/csv")
;; (def seasons ["1011" "1112" "1213" "1314"])
(def seasons ["xxx" "eeee" "ss" "a"])
(def leagues ["xcf"])

(fetch-and-save csv-folder seasons leagues)




;; "http://www.football-data.co.uk/mmz4281/1112/E0"
;; "http://www.football-data.co.uk/mmz4281/1112/xxxx"

(defn years-pair-to-season [years-pair]
  (let [years-pair-as-strings (map str years-pair)
        years-pair-substrings (map #(subs % 2 4) years-pair-as-strings)
        season (apply str years-pair-substrings)]
    season))


(defn make-seasons [first-startyear last-startyear]
  (let [
        start-years (range first-startyear last-startyear)
        end-years (range (inc first-startyear) (inc last-startyear))
        years-pairs (map vector start-years end-years)
        seasons (map years-pair-to-season years-pairs)
        ]
    seasons))


(def start-years (range 1990 2015))
(def end-years (range 1991 2016))
(def years-pairs (map vector start-years end-years))
(def seasons (map years-pair-to-season years-pairs))
seasons


(def seasons (make-seasons 1990 1994))
seasons


(def leagues-nfo [
              {:ticker "E0"}
              {:ticker "E1"}
              {:ticker "E2"}
              {:ticker "E3"}
              {:ticker "EC"}

              {:ticker "D1"}
              {:ticker "D2"}

              {:ticker "I1"}
              {:ticker "I2"}

              {:ticker "SP1"}
              {:ticker "SP2"}

              {:ticker "F1"}
              {:ticker "F2"}

              ])

(def leagues (map :ticker leagues-nfo))

leagues


(fetch-and-save csv-folder seasons leagues)


;; mongolab football-database:
;; mongodb://<dbuser>:<dbpassword>@ds061797.mongolab.com:61797/football-data
;; user: encho
;; pwd.: footballdata
;; i.e.:
;; mongodb://encho:footballdata@ds061797.mongolab.com:61797/football-data







