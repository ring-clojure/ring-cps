(ns ring.cps.protocols)

(defprotocol Closeable
  (close! [x]))

(defprotocol Readable
  (read! [x callback]))

(defprotocol Writable
  (write! [x data callback]))

(defprotocol ResponseBody
  (send-body! [x writer]))

(extend-protocol ResponseBody
  String
  (send-body! [string writer]
    (doto writer
      (write! (.getBytes string "UTF-8"))
      (close!))))
