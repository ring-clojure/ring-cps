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
    (let [result (.read channel buffer)]
      (when (pos? result)
        (.flip buffer)
        (callback buffer))
      result)))

(defn- read-listener [^ConcurrentLinkedQueue pending buffer-pool]
  (reify ChannelListener
    (handleEvent [_ channel]
      (loop []
        (when-let [callback (.poll pending)]
          (read-channel channel buffer-pool callback)
          (recur)))
      (.resumeReads channel))))

(defn- request-reader [^HttpServerExchange exchange]
  (let [channel     (-> exchange .getRequestChannel)
        buffer-pool (-> exchange .getConnection .getBufferPool)
        pending     (ConcurrentLinkedQueue.)
        listener    (read-listener pending buffer-pool)]
    (-> channel .getReadSetter (.set listener))
    (reify
      p/Closeable
      (close! [_]
        (IoUtils/safeClose channel))
      p/Reader
      (read! [reader callback]
        (let [result (read-channel channel buffer-pool callback)]
          (cond
            (neg? result)  (p/close! reader)
            (zero? result) (.add pending callback)))))))

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
   :body           (-> ex request-reader)})

(defn- write-channel [^StreamSourceChannel channel ^ByteBuffer buffer callback]
  (let [result (.write channel buffer)]
     (when (pos? result)
       (callback result))
     result))

(defn- write-listener [^ConcurrentLinkedQueue pending]
  (reify ChannelListener
    (handleEvent [_ channel]
      (loop []
        (when-let [[data callback] (.poll pending)]
          (write-channel channel data callback)
          (recur)))
      (.resumeWrites channel))))

(defn- response-writer [^HttpServerExchange exchange]
  (let [channel  (-> exchange .getResponseChannel)
        pending  (ConcurrentLinkedQueue.)
        listener (write-listener pending)]
    (-> channel .getWriteSetter (.set listener))
    (reify
      p/Closeable
      (close! [_]
        (doto channel
          (.shutdownWrites)
          (.flush)
          (IoUtils/safeClose)))
      p/Writer
      (write! [_ data callback]
        (let [buffer (ByteBuffer/wrap ^bytes data)
              result (write-channel channel buffer callback)]
          (when (zero? result)
            (.add pending [buffer callback])))))))

(defn- add-header! [^HeaderMap header-map ^String key val]
  (if (string? val)
    (.put header-map (HttpString. key) ^String val)
    (.putAll header-map (HttpString. key) vals)))

(defn- set-headers! [header-map headers]
  (reduce-kv add-header! header-map headers))

(defn- set-response! [^HttpServerExchange exchange response]
  (.setResponseCode exchange (:status response))
  (set-headers! (.getResponseHeaders exchange) (:headers response))
  (p/send-body! (:body response) (response-writer exchange)))

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
