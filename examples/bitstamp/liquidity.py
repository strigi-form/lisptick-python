"""Some liquidity indicators, asked from 1 minute in past until next minute"""
import lisptick

HOST = "uat.lisptick.org"
PORT = 12006

class Liquidity():
  """Keep and print liquidity indicators from request"""

  def __init__(self):
    self.t = None
    self.spread = None
    self.usdvolume = None

  def update(self, _, uid, point):
    """Update liquidity from request"""
    if not isinstance(point, lisptick.Point):
      #should not happen
      return
    self.time = point.time
    if uid == 1:
      #id is given sequentially so 1 is spread
      self.spread = point.i
    else:
      self.usdvolume = point.i
    #print status
    print(self.time, "spread: $", self.spread, "volume: $", self.usdvolume)

def main():
    """Show bid/ask spread and mininum available volume at 1st limit"""
    conn = lisptick.Socket(HOST, PORT)
    liquidity = Liquidity()

    # $ spread
    # mininum buy or sell volume in $, rounded to cent
    request = """
(def
  code "BTC"
  dt   1m
)

(defn spread[name start stop]
  (-
    (timeserie @ask-price "bitstamp" code start stop)
    (timeserie @bid-price "bitstamp" code start stop)))

(defn avusd[name start stop]
  (round
    (min
      (*
        (timeserie @ask-volume "bitstamp" code start stop)
        (timeserie @ask-price "bitstamp" code start stop))
      (*
        (timeserie @bid-volume "bitstamp" code start stop)
        (timeserie @bid-price "bitstamp" code start stop)))
   -2))

[
  (spread code (- (now) dt) (+ (now) dt))
  (avusd code (- (now) dt) (+ (now) dt))
]
"""
    conn.walk_result(request, liquidity.update)

if __name__ == "__main__":
    main()
