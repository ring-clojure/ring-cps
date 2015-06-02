(ns ring.cps.protocols
  (:import [java.nio ByteBuffer]))

(defprotocol Closeable
  (close! [x]))

(defprotocol Reader
  (read! [x callback]))

(defprotocol Writer
  (write! [x data callback]))

(defprotocol ResponseBody
  (send-body! [x writer]))

(extend-protocol ResponseBody
  nil
  (send-body! [_ writer]
    (close! writer))
  String
  (send-body! [string writer]
    (let [buffer (ByteBuffer/wrap (.getBytes string "UTF-8"))]
      (write! writer buffer (fn [_ _] (close! writer))))))
