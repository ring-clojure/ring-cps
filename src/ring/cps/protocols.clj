(ns ring.cps.protocols)

(defprotocol Closeable
  (close! [x]))

(defprotocol Reader
  (read! [x callback]))

(defprotocol Writer
  (write! [x data callback]))

(defprotocol ResponseBody
  (send-body! [x writer]))

(defn- noop [])

(extend-protocol ResponseBody
  nil
  (send-body! [_ writer]
    (close! writer))
  String
  (send-body! [string writer]
    (write! writer (.getBytes string "UTF-8") noop)
    (close! writer)))
