"""Get result example for a timeserie"""
import lisptick

HOST = "lisptick.org"
PORT = 12006

def main():
    """Ask for temperature at Poitiers airport"""
    conn = lisptick.Socket(HOST, PORT)
    request = """(timeserie @"t" "meteonet" "86027001" 2017-07-06)"""
    timeserie = conn.get_result(request)
    for point in timeserie:
        print(point.time, point.i)

if __name__ == "__main__":
    main()
