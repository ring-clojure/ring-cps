(ns ring.cps.adapter.undertow
  (:require [clojure.string :as str]
            [ring.cps.protocols :as p])
  (:import [io.undertow Undertow]
           [io.undertow.io Sender]
           [io.undertow.server HttpHandler HttpServerExchange]
           [io.undertow.util HeaderMap HeaderValues HttpString]
           [java.nio ByteBuffer]
           [java.util.concurrent ConcurrentLinkedQueue]
           [org.xnio ChannelListener IoUtils Pool Pooled]
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

(defmacro ^:private with-pool [[sym pool] & body]
  `(let [pooled# (.allocate ~pool)]
     (try
       (let [~sym (.getResource pooled#)] ~@body)
       (finally
         (.free pooled#)))))

(defn- read-channel [^StreamSourceChannel channel ^Pool buffer-pool callback]
  (with-pool [^ByteBuffer buffer buffer-pool]
    (let [res (.read channel buffer)]
      (when (pos? res)
        (.flip buffer)
        (callback buffer))
      res)))

(defn- read-listener [^ConcurrentLinkedQueue callbacks buffer-pool]
  (reify ChannelListener
    (handleEvent [_ channel]
      (loop []
        (when-let [callback (.poll callbacks)]
          (read-channel channel buffer-pool callback)
          (recur)))
      (.resumeReads channel))))

(defn- channel-reader [^StreamSourceChannel channel buffer-pool]
  (let [callbacks (ConcurrentLinkedQueue.)
        listener  (read-listener callbacks buffer-pool)]
    (-> channel .getReadSetter (.set listener))
    (reify
      p/Closeable
      (close! [_]
        (IoUtils/safeClose channel))
      p/Reader
      (read! [reader callback]
        (let [res (read-channel channel buffer-pool callback)]
          (cond
            (neg? res)  (p/close! reader)
            (zero? res) (.add callbacks callback)))))))

(defn- get-request [^HttpServerExchange ex]
  (let [buffer-pool (-> ex .getConnection .getBufferPool)]
    {:server-port    (-> ex .getDestinationAddress .getPort)
     :server-name    (-> ex .getHostName)
     :remote-addr    (-> ex .getSourceAddress .getAddress .getHostAddress)
     :uri            (-> ex .getRequestURI)
     :query-string   (-> ex .getQueryString)
     :scheme         (-> ex .getRequestScheme .toString .toLowerCase keyword)
     :request-method (-> ex .getRequestMethod .toString .toLowerCase keyword)
     :protocol       (-> ex .getProtocol .toString)
     :headers        (-> ex .getRequestHeaders get-headers)
     :body           (-> ex .getRequestChannel (channel-reader buffer-pool))}))

(defn- channel-writer [^Sender sender]
  (reify
    p/Closeable
    (close! [_]
      (.close sender))
    p/Writer
    (write! [_ data callback]
      (.send sender (ByteBuffer/wrap ^bytes data)))))

(defn- add-header! [^HeaderMap header-map ^String key val]
  (if (string? val)
    (.put header-map (HttpString. key) ^String val)
    (.putAll header-map (HttpString. key) vals)))

(defn- set-headers! [header-map headers]
  (reduce-kv add-header! header-map headers))

(defn- set-response! [^HttpServerExchange exchange response]
  (.setResponseCode exchange (:status response))
  (set-headers! (.getResponseHeaders exchange) (:headers response))
  (p/send-body! (:body response) (channel-writer (.getResponseSender exchange))))

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
