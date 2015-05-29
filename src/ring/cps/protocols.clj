(ns ring.cps.protocols)

(defprotocol Closeable
  (close! [x]))

(defprotocol Readable
  (read! [x callback]))

(defprotocol Writable
  (write! [x data callback]))
