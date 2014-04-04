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

   ;;[clojure.contrib.shell-out :as shell]
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

(defn years-pair-to-season [years-pair]
  (let [years-pair-as-strings (map str years-pair)
        years-pair-substrings (map #(subs % 2 4) years-pair-as-strings)
        season (apply str years-pair-substrings)]
    season))


(defn make-seasons [first-startyear last-startyear]
  (let [start-years (range first-startyear last-startyear)
        end-years (range (inc first-startyear) (inc last-startyear))
        years-pairs (map vector start-years end-years)
        seasons (map years-pair-to-season years-pairs)]
    seasons))


;; Utils for getting filepaths/names for given regex...
;; ----------------------------------------------------------------------------
(defn- wildcard-filter
  "Given a regex, return a FilenameFilter that matches."
  [re]
  (reify java.io.FilenameFilter
    (accept [_ dir name] (not (nil? (re-find re name))))))

(defn- nonhidden-filter
  "return a FilenameFilter that ignores files that begin with dot or end with ~."
  []
  (reify java.io.FilenameFilter
    (accept [_ dir name] (and (not (.startsWith name "."))
			      (not (.endsWith name "~"))))))

(defn directory-list
  "Given a directory and a regex, return a sorted seq of matching filenames.  To find something like *.txt you would pass in \".*\\\\.txt\""
  ([dir re]
     (sort (.list (clojure.java.io/file dir) (wildcard-filter (java.util.regex.Pattern/compile re)))))
  ([dir]
     (sort (.list (clojure.java.io/file dir) (nonhidden-filter))))
  )

(defn full-directory-list
  "Given a directory, return the full pathnames for the files it contains"
  [dir]
  (sort (map #(.getCanonicalPath %) (.listFiles (clojure.java.io/file dir))))
  )

(defn pwd []
  (.getCanonicalPath (clojure.java.io/file ".")))





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

;; ;; define folder and relevant seasons/leagues
;; ;; (def seasons ["1011" "1112" "1213" "1314"])
;; (def seasons ["xxx" "eeee" "ss" "a"])
;; (def leagues ["xcf"])

;; (fetch-and-save csv-folder seasons leagues)




;; "http://www.football-data.co.uk/mmz4281/1112/E0"
;; "http://www.football-data.co.uk/mmz4281/1112/xxxx"



;; (def start-years (range 1990 2015))
;; (def end-years (range 1991 2016))
;; (def years-pairs (map vector start-years end-years))
;; (def seasons (map years-pair-to-season years-pairs))
;; seasons

(def csv-folder "data/csv")

(def seasons (make-seasons 2000 2002))
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



;; fabrik for csv data paths

(defn data-path [season league]
  (football-filename csv-folder [season league]))

(data-path "1213" "E0")


;; fetch filenames in csv data folder
(def csv-folder-path (str (pwd) "/" csv-folder))
(def csv-filenames (directory-list csv-folder-path ".csv"))
(def csv-filepaths (map #(str csv-folder-path "/" %) csv-filenames))

(first csv-filepaths)







(def my-file (nth csv-filepaths 30))


(def my-file (data-path "9798" "D1"))
;; my-file

(def csv-data (slurp my-file))
;; csv-data

;; this is right now as its not edn??!!
(def aa (read-string csv-data))
aa

;; parse the data:

(football/default-parser "66")
(football/default-parser "")

(def ggg (football/read-games aa))
ggg

(= "" "")

(defn is-empy-date? [raw-game]
  (= "" (:Date raw-game)))



(map :Date ggg)



(filter is-empy-date? ggg)

(def not-empty-date-games (filter has-valid-date? ggg))
(def clean (map remove-empty-key not-empty-date-games))
clean


(def xxx (first not-empty-date-games))

xxx

(first (keys xxx))
(keyword "")

((keyword "") xxx)




(def games (football/parse-football-data aa))
games


;; mongolab football-database:
;; mongodb://<dbuser>:<dbpassword>@ds061797.mongolab.com:61797/football-data
;; user: encho
;; pwd.: footballdata
;; i.e.:
;; mongodb://encho:footballdata@ds061797.mongolab.com:61797/football-data

(football/date-parser "30/01/01")
(football/date-parser "30/01/99")

;; #inst "1999-01-01T00:00:00.000-00:00"



