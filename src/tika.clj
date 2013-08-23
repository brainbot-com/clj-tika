;; Clojure Interface to Apache Tika library
(ns tika
  (:import [java.io InputStream File FileInputStream]
           [java.net URL]
           [org.apache.tika.language LanguageIdentifier]
           [org.apache.tika.metadata Metadata]
           [org.apache.tika Tika]
           [org.apache.tika.io TikaInputStream]
           )
  (:use [clojure.java.io :only [input-stream]])
  )

;; TODO: add separate function to extract only meta-data

(def ^Tika ^{:private true} tika-class (Tika.))
(def ^{:dynamic true} *default-max-length* (.getMaxStringLength tika-class))

(defn conv-metadata [^Metadata mdata]
  (let [names (.names mdata)]
    (zipmap (map #(keyword (.toLowerCase %1)) names)
            (map #(seq (.getValues mdata %1)) names))))


(defn- parse-istream
  [^InputStream istream max-length]
  (let [metadata (Metadata.)
        text (.parseToString tika-class istream metadata (int max-length))]
    (assoc (conv-metadata metadata) :text text)))


(defn parse
  "Performs parsing of given object"
  ([in max-length]
     (parse-istream (input-stream in) max-length))
  ([in]
     (parse in *default-max-length*)))



(defprotocol TikaProtocol
  "Protocol for Tika library"
  (detect-mime-type [this] "Detects mime-type of given object")
  )

(extend-protocol TikaProtocol
  InputStream
  (detect-mime-type [^InputStream ifile]
    (with-open [in (TikaInputStream/get ifile)]
      (.detect tika-class in))))

(extend-protocol TikaProtocol
  java.io.File
  (detect-mime-type [^File file] (.detect tika-class file)))

(extend-protocol TikaProtocol
  String
  (detect-mime-type [^String filename] (.detect tika-class filename)))

(extend-protocol TikaProtocol
  URL
  (detect-mime-type [^URL url] (.detect tika-class url)))

(defn detect-language
  "Detects language of given text"
  [^String text]
  (.getLanguage (LanguageIdentifier. text)))
