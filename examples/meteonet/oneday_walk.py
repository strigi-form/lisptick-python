"""Walk result example for a single timeserie"""
import lisptick

HOST = "lisptick.org"
PORT = 12006

def main():
    """Ask for temperature at Poitiers airport"""
    conn = lisptick.Socket(HOST, PORT)
    request = """(timeserie @"t" "meteonet" "86027001" 2017-07-06)"""
    # call show_value for each value one by one, as soon as it arrives
    conn.walk_result(request, show_value)

def show_value(_, __, value):
    """reader and uid are useless as result is a single timeserie"""
    print(value.time, value.i)

if __name__ == "__main__":
    main()
