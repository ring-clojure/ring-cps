(ns ring.cps.adapter.undertow
  (:require [ring.cps.protocols :as p])
  (:import [java.nio ByteBuffer]
           [io.undertow Undertow]
           [io.undertow.server HttpHandler HttpServerExchange]
           [io.undertow.util HeaderMap HttpString]
           [org.xnio.channels StreamSinkChannel]))

(extend-type HttpServerExchange
  p/Writer
  (write! [exchange data callback]
    (.send (.getResponseSender exchange) (ByteBuffer/wrap ^bytes data)))
  p/Closeable
  (close! [exchange]
    (.endExchange exchange)))

(defn- add-header! [^HeaderMap header-map ^String key val]
  (if (string? val)
    (.put header-map (HttpString. key) ^String val)
    (.putAll header-map (HttpString. key) vals)))

(defn- set-headers! [header-map headers]
  (reduce-kv add-header! header-map headers))

(defn- set-response! [^HttpServerExchange exchange response]
  (.setResponseCode exchange (:status response))
  (set-headers! (.getResponseHeaders exchange) (:headers response))
  (p/send-body! (:body response) exchange))

(defn- undertow-handler [handler]
  (reify HttpHandler
    (handleRequest [_ exchange]
      (handler {} #(set-response! exchange %)))))

(defn- ^Undertow undertow-builder
  [handler {:keys [port host] :or {host "0.0.0.0", port 80}}]
  (doto (Undertow/builder)
    (.addHttpListener port host)
    (.setHandler (undertow-handler handler))))

(defn run-undertow [handler options]
  (doto (.build (undertow-builder handler options))
    (.start)))
