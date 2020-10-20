"""Walk maximums temperature in Celsius"""
import lisptick

HOST = "uat.lisptick.org"
PORT = 12006

def main():
    """Ask for max temperatures at Poitiers airport"""
    conn = lisptick.Socket(HOST, PORT)
    # maxium T° progression since start date
    # change T° form Kelvin to Celsius 
    print("Maximum T° progression")
    print_request(conn,
"""
(def
  zero-kc -273.15    ;0 Kelvin in Celsius
  start 2016-01-01
  stop  2018-12-31
  station "86027001" ;Poitiers airport
)

(max
  (+
    zero-kc
    (timeserie @"t" "meteonet" station start stop)))
""")

    print("Time and value of maximum T°")
    print_request(conn,
"""
(def
  zero-kc -273.15    ;0 Kelvin in Celsius
  start 2016-01-01
  stop  2018-12-31
  station "86027001" ;Poitiers airport
)

(last 
  (max
    (+
      zero-kc
      (timeserie @"t" "meteonet" station start stop))))
""")

def print_request(conn, request):
    """send request to conn and print result on the fly"""
    conn.walk_result(request, print_value)

def print_value(_, __, value):
    """reader and uid are useless as result is a single timeserie"""
    print(value)

if __name__ == "__main__":
    main()
