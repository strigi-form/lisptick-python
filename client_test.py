"""Client of LispTick TimeSerie Streaming Server
https://docs.python-guide.org/writing/tests/
https://docs.python.org/3/library/unittest.html"""

import unittest
import datetime
import re
import lisptick

HOST = "lisptick.org"
PORT = 12006


class ClientTest(unittest.TestCase):
    """Class Test LispTick"""

    def test_int(self):
        """Test Integer (+ 3 4)"""
        self.assertEqual(test_get("""(+ 3 4)"""), 7)

    def test_bool(self):
        """Test boolean"""
        self.assertEqual(test_get("""(= 3 4)"""), False)
        self.assertEqual(test_get("""(= 4 4)"""), True)

    def test_float(self):
        """Test Float"""
        self.assertEqual(test_get("""(+ 3.04 0.1)"""), 3.14)
        self.assertEqual(test_get("""(/ 10 4)"""), 2.5)

    def test_dec64(self):
        """Test Dec64 as Float"""
        self.assertEqual(test_get("""2.5"""), 2.5)

    def test_time_duration(self):
        """Test time & duration"""
        #NTP should ensure lag is rarely more than 128ms
        delta = abs(datetime.datetime.now() - test_get("""(now)"""))
        self.assertLess(delta.total_seconds(), 0.128)
        epoch = 1508322600
        self.assertEqual(str(test_get("""2017-10-18T10:30""")),
                         str(datetime.datetime.fromtimestamp(epoch)))
        self.assertEqual(str(test_get("""1Y1M10D10s""")), str(
            lisptick.Duration(1, 1, 10, 1000000000 * 10)))
        self.assertEqual(str(test_get("""10h""")), str(
            lisptick.Duration(0, 0, 0, 1000000000 * 60 * 60 * 10)))

    def test_string(self):
        """Test String (version)"""
        self.assertTrue(re.compile(
            r"(?:LispTick)\s[v][0-9]+\.[0-9]+\.[0-9]+").match(test_get("""(version)""")))
        self.assertEqual(test_get('"toto+&"'), "toto+&")

    def test_array(self):
        """Test Array"""
        self.assertEqual(test_get("""[1 2 3 4]"""), [1, 2, 3, 4])

    def test_error_array(self):
        """Test Error on Array (+ "a" 3)"""
        with self.assertRaises(lisptick.LispTickException):
            test_get("""(+ "a" 3)""")

    def test_timeserie(self):
        """Test timeserie"""
        tserie = test_get(
            """(timeserie 2017-10-26T09:19 48.6 2017-10-26T10:30 49 2017-10-26T11:51 49.27)""")
        self.assertTrue(isinstance(tserie, list))
        self.assertEqual(str(tserie[0]), str(lisptick.Point(
            datetime.datetime(2017, 10, 26, 11, 19), 48.6)))
        self.assertEqual(str(tserie[1]), str(lisptick.Point(
            datetime.datetime(2017, 10, 26, 12, 30), 49)))
        self.assertEqual(str(tserie[2]), str(lisptick.Point(
            datetime.datetime(2017, 10, 26, 13, 51), 49.27)))

    def test_array_timeserie(self):
        """Test mutliplexed timeseries"""
        test = """[
            (timeserie 2017-10-26T09:19 48.6 2017-10-26T10:30 49 2017-10-26T11:51 49.27)
            (timeserie 2017-10-26T09:19 48.6 2017-10-26T10:30 49 2017-10-26T11:51 49.27)
        ]"""
        tserie = test_get(test)
        self.assertTrue(isinstance(tserie, list))
        self.assertEqual(str(tserie[0][0]), str(lisptick.Point(
            datetime.datetime(2017, 10, 26, 11, 19), 48.6)))
        self.assertEqual(str(tserie[0][1]), str(lisptick.Point(
            datetime.datetime(2017, 10, 26, 12, 30), 49)))
        self.assertEqual(str(tserie[0][2]), str(lisptick.Point(
            datetime.datetime(2017, 10, 26, 13, 51), 49.27)))
        self.assertEqual(str(tserie[1][0]), str(lisptick.Point(
            datetime.datetime(2017, 10, 26, 11, 19), 48.6)))
        self.assertEqual(str(tserie[1][1]), str(lisptick.Point(
            datetime.datetime(2017, 10, 26, 12, 30), 49)))
        self.assertEqual(str(tserie[1][2]), str(lisptick.Point(
            datetime.datetime(2017, 10, 26, 13, 51), 49.27)))

    def test_timeserie_array(self):
        """Test timeserie of arrays"""
        test = """
            (def d 2017-10-26)(timeserie (+ d 9h19m) [1 "a"] (+ d 11h51m) [3 "c"])
        """
        tserie = test_get(test)
        self.assertTrue(isinstance(tserie, list))
        self.assertEqual(str(tserie[0]), str(lisptick.Point(
            datetime.datetime(2017, 10, 26, 11, 19), [1, "a"])))
        self.assertEqual(str(tserie[1]), str(lisptick.Point(
            datetime.datetime(2017, 10, 26, 13, 51), [3, "c"])))

    def test_pair(self):
        """Test Pair (3.5 . "toto")"""
        self.assertEqual(test_get("""(3.5 . "toto")"""), (3.5, "toto"))

    def test_hist(self):
        """Test array of Pairs from histogram"""
        self.assertEqual(test_get("""(hist 3 4 5 4 3 3)"""),
                         [(3, 3), (4, 2), (5, 1)])

    def test_tensor(self):
        """Test Pair (3.5 . "toto")"""
        tensor = lisptick.Tensor(
            [3, 3], [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0])
        self.assertEqual(
            str(test_get("""(tensor (shape 3 3) [1 2 3 4 5 6 7 8 9])""")), str(tensor))

    def test_sentinel(self):
        """Test SexpNull"""
        self.assertEqual(test_get("""()"""), lisptick.Sentinel.Null)


def test_get(code):
    """Call LispTick server and get raw result"""
    conn = lisptick.Socket(HOST, PORT)
    return conn.get_result(code)


if __name__ == "__main__":
    unittest.main()
