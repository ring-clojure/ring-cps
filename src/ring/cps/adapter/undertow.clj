(ns ring.cps.adapter.undertow
  (:require [clojure.string :as str]
            [ring.cps.protocols :as p])
  (:import [io.undertow Undertow Undertow$Builder]
           [io.undertow.io Sender]
           [io.undertow.server HttpHandler HttpServerExchange]
           [io.undertow.util HeaderMap HeaderValues HttpString]
           [java.nio ByteBuffer]
           [java.nio.channels ReadableByteChannel WritableByteChannel]
           [java.util.concurrent ConcurrentLinkedQueue]
           [org.xnio ChannelListener IoUtils Pool Pooled]
           [org.xnio.channels StreamSinkChannel StreamSourceChannel]))

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

(defmacro ^:private with-channel [[sym channel] & body]
  `(let [ch# ~channel]
     (locking ch#
       (try
         (let [~sym ch#] ~@body)
         (catch Exception ex#
           (IoUtils/safeClose ch#)
           (throw ex#))))))

(defn- read-channel [channel ^Pool buffer-pool ^ConcurrentLinkedQueue pending]
  (with-channel [^ReadableByteChannel ch channel]
    (with-pool [^ByteBuffer buffer buffer-pool]
      (loop []
        (when-let [callback (.peek pending)]
          (let [result (.read ch buffer)]
            (cond
              (neg? result)
              (do (.clear pending)
                  (IoUtils/safeClose ch))
              (pos? result)
              (do (.poll pending)
                  (doto buffer .flip callback .clear)
                  (recur)))))))))

(defn- read-listener [buffer-pool pending]
  (reify ChannelListener
    (handleEvent [_ channel]
      (read-channel channel buffer-pool pending)
      (.resumeReads ^StreamSourceChannel channel))))

(defn- request-reader [^HttpServerExchange exchange]
  (let [channel     (-> exchange .getRequestChannel)
        buffer-pool (-> exchange .getConnection .getBufferPool)
        pending     (ConcurrentLinkedQueue.)
        listener    (read-listener buffer-pool pending)]
    (-> channel .getReadSetter (.set listener))
    (reify
      p/Closeable
      (close! [_]
        (IoUtils/safeClose channel))
      p/Reader
      (read! [reader callback]
        (.add pending callback)
        (.wakeupReads channel)))))

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

(defn- write-channel [channel ^ConcurrentLinkedQueue pending]
  (with-channel [^WritableByteChannel ch channel]
    (loop []
      (when-let [[^ByteBuffer buffer callback] (.peek pending)]
        (while (and (.remaining buffer)
                    (pos? (.write channel buffer))))
        (when (zero? (.remaining buffer))
          (.poll pending)
          (callback)
          (recur))))))

(defn- write-listener [pending]
  (reify ChannelListener
    (handleEvent [_ channel]
      (write-channel channel pending)
      (.resumeWrites ^StreamSinkChannel channel))))

(def flush-listener
  (reify ChannelListener
    (handleEvent [_ channel]
      (let [^StreamSinkChannel ch channel]
        (if (.flush channel)
          (IoUtils/safeClose channel)
          (.resumeWrites channel))))))

(defn- response-writer [^HttpServerExchange exchange]
  (let [channel  (-> exchange .getResponseChannel)
        pending  (ConcurrentLinkedQueue.)
        listener (write-listener pending)]
    (-> channel .getWriteSetter (.set listener))
    (reify
      p/Closeable
      (close! [_]
        (-> channel .getWriteSetter (.set flush-listener))
        (.shutdownWrites channel)
        (if (.flush channel)
          (IoUtils/safeClose channel)))
      p/Writer
      (write! [_ buffer callback]
        (.add pending [buffer callback])
        (.wakeupWrites channel)))))

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

(defn- undertow-builder
  [handler {:keys [port host] :or {host "0.0.0.0", port 80}}]
  (doto (Undertow/builder)
    (.addHttpListener port host)
    (.setHandler (undertow-handler handler))))

(defn run-undertow [handler options]
  (let [builder ^Undertow$Builder (undertow-builder handler options)
        server  (.build builder)]
    (.start server)
    server))
