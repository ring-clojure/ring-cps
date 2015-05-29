(ns ring.cps.adapter.undertow
  (:require [clojure.string :as str]
            [ring.cps.protocols :as p])
  (:import [io.undertow Undertow]
           [io.undertow.io Sender]
           [io.undertow.server HttpHandler HttpServerExchange]
           [io.undertow.util HeaderMap HeaderValues HttpString]
           [java.nio ByteBuffer]
           [org.xnio.channels StreamSourceChannel]))

(defn- header-key [^HeaderValues header]
  (-> header .getHeaderName .toString .toLowerCase))

(defn- header-val [^HeaderValues header]
  (if (> (.size header) 1)
    (str/join ", " (iterator-seq (.iterator header)))
    (.get header 0)))

(defn- get-headers [^HeaderMap header-map]
  (persistent! (reduce #(assoc! %1 (header-key %2) (header-val %2))
                       (transient {})
                       header-map)))

(extend-type StreamSourceChannel
  p/Closeable
  (close! [channel])
  p/Reader
  (read! [channel callback]))

(defn- get-request [^HttpServerExchange ex]
  {:server-port    (-> ex .getDestinationAddress .getPort)
   :server-name    (-> ex .getHostName)
   :remote-addr    (-> ex .getSourceAddress .getAddress .getHostAddress)
   :uri            (-> ex .getRequestURI)
   :query-string   (-> ex .getQueryString)
   :scheme         (-> ex .getRequestScheme .toString .toLowerCase keyword)
   :request-method (-> ex .getRequestMethod .toString .toLowerCase keyword)
   :protocol       (-> ex .getProtocol .toString)
   :headers        (-> ex .getRequestHeaders get-headers)
   :body           (-> ex .getRequestChannel)})

(extend-type Sender
  p/Writer
  (write! [sender data callback]
    (.send sender (ByteBuffer/wrap ^bytes data)))
  p/Closeable
  (close! [sender]
    (.close sender)))

(defn- add-header! [^HeaderMap header-map ^String key val]
  (if (string? val)
    (.put header-map (HttpString. key) ^String val)
    (.putAll header-map (HttpString. key) vals)))

(defn- set-headers! [header-map headers]
  (reduce-kv add-header! header-map headers))

(defn- set-response! [^HttpServerExchange exchange response]
  (.setResponseCode exchange (:status response))
  (set-headers! (.getResponseHeaders exchange) (:headers response))
  (p/send-body! (:body response) (.getResponseSender exchange)))

(defn- undertow-handler [handler]
  (reify HttpHandler
    (handleRequest [_ exchange]
      (handler (get-request exchange) #(set-response! exchange %)))))

(defn- ^Undertow undertow-builder
  [handler {:keys [port host] :or {host "0.0.0.0", port 80}}]
  (doto (Undertow/builder)
    (.addHttpListener port host)
    (.setHandler (undertow-handler handler))))

(defn run-undertow [handler options]
  (doto (.build (undertow-builder handler options))
    (.start)))
