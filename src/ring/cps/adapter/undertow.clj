(ns ring.cps.adapter.undertow
  (:import [io.undertow Undertow]
           [io.undertow.server HttpHandler HttpServerExchange]
           [io.undertow.util HttpString]))

(defn- undertow-handler [handler]
  (reify HttpHandler
    (handleRequest [_ exchange]
      (doto exchange
        (.. getResponseHeaders (put (HttpString. "Content-Type") "text/plain"))
        (.. getResponseSender  (send "Hello World"))))))

(defn- ^Undertow undertow-builder
  [handler {:keys [port host] :or {host "0.0.0.0", port 80}}]
  (doto (Undertow/builder)
    (.addHttpListener port host)
    (.setHandler (undertow-handler handler))))

(defn run-undertow [handler options]
  (-> (undertow-builder handler options)
      (.build)
      (.start)))
