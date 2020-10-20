"""
LispTick TimeSerie Streaming Server module
To work on an Onion Omega2 needs:
opkg update
opkg install python-codecs
"""
import datetime
import json
import struct
import socket

# Types byte definition
TNULL = b'\x00'
TINT = b'\x01'
TFLOAT = b'\x02'
TTIME = b'\x03'
TDURATION = b'\x04'
TERROR = b'\x05'
TSTRING = b'\x06'
TARRAY = b'\x07'
TARRAYSERIAL = b'\x08'
TTIMESERIE = b'\x09'
TSENTINEL = b'\x0A'
TBOOL = b'\x0B'
TDEC64 = b'\x0C'
TPAIR = b'\x0D'
THEARTBEAT = b'\x0E'
TTENSOR = b'\x0F'


class LispTickException(Exception):
    """Simple LispTick error message"""

    def __init__(self, msg):
        super(LispTickException, self).__init__(msg)
        self._msg = msg

    def _str_(self):
        return self._msg


class Sentinel(int):
    """Sentinel object indicating end of a grid flow"""
    Null = 0
    End = 1
    Marker = 2


class InArray():
    """UID and position in an array"""

    def __init__(self, init_uid, init_pos):
        self.uid = init_uid
        self.pos = init_pos

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    def get_uid(self):
        """Element uniq id"""
        return self.uid

    def get_pos(self):
        """Element position in array"""
        return self.pos


class Duration():
    """LispTick duration time handling Year, Month, Day and microseconds (from nano)"""

    def __init__(self, init_year=0, init_month=0, init_day=0, init_epoch=0):
        self.year = init_year
        self.month = init_month
        self.timedelta = datetime.timedelta(init_day, 0, init_epoch / 1000)

    def __str__(self):
        return str(self.year) + "Y" + str(self.month) + "M" + str(self.timedelta)

    def get_year(self):
        """Number of years duration part"""
        return self.year

    def get_month(self):
        """Number of month duration part"""
        return self.month

    def get_timedelta(self):
        """micro seconds and days duration part as a timedelta"""
        return self.timedelta


class Point():
    """A point is a value at a time"""

    def __init__(self, init_time, init_value):
        self.time = init_time
        self.i = init_value

    def __str__(self):
        return str(self.time) + " " + str(self.i)

    def __len__(self):
        return 2


class HeartBeat():
    """An HeartBeat is an information value that can be forgotten"""

    def __init__(self, init_value):
        self.value = init_value

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    def get_value(self):
        """HeartBeat value"""
        return self.value

class Tensor():
    """Tensor  n-dimensional arrays"""

    def __init__(self, shape, values=None):
        self.shape = shape
        if values is None:
            values = [0] * self.get_size()
        self.values = values

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    def get_size(self):
        """tensor size in number of values"""
        size = 1
        for i in self.shape:
            size *= i
        return size


class Socket():
    """Request LispTick by socket"""

    def __init__(self, host, port):
        self.__host = host
        self.__port = port

    def get_result(self, request):
        """Send resquest to server and return result"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.__host, self.__port))

        # Send request
        send_message(sock, request)
        res = LisptickReader(sock).get_result(-1)

        sock.close()
        return res

    def walk_result(self, request, func):
        """Call func on each part of result"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.__host, self.__port))

        # Send request
        send_message(sock, request)
        err_msg = LisptickReader(sock).walk_result(func)

        sock.close()
        if err_msg != "":
            raise LispTickException(err_msg)

#dec64 float factor
factors = [1.0]*129
for e in range (0, 128):
    factors[e] = pow(10.0, e)

class ReaderContext():
    """internaly used by get_result to read full result"""

    def __init__(self, limit):
        self.arrays = {}
        self.timeseries = {}
        self.res = {}
        self.limit = 0
        self.limit_reached = False
        self.size_limit = limit

    def update_limit_and_check(self):
        """increase limit and check if max is reached"""
        self.limit += 1
        if self.size_limit > -1 & self.limit >= self.size_limit:
            self.limit_reached = True
        return self.limit_reached

    def get_timeserie(self, uid):
        "Return timeserie from uid"
        return self.timeseries.get(uid)

    def set_timeserie(self, uid, tserie):
        "Set timeserie at uid"
        self.timeseries[uid] = tserie

    def get_array(self, uid):
        "Return array from uid"
        return self.arrays.get(uid)

    def set_array(self, uid, array):
        "Set array at uid"
        self.arrays[uid] = array


class LisptickReader():
    """Reader dedicated to LispTick communication and Sexp Serialization"""

    def __init__(self, init_con):
        self.con = init_con
        self.tserie = {}
        self.sizes = {}
        self.where = {}

    def __str__(self):
        res = "[ ts: "+str(self.tserie) + ", sizes: " + str(self.sizes)
        res += ", where: "+str(self.where)+" ]"
        return res

    def walk_result(self, func):
        """Walk LispTick received result message, callinf func for each element"""
        err = ""
        while True:
            # do not call fix received as end is in fact nothing received...
            idt = self.con.recv(1)
            if idt == b'':
                # nothing received, this is the end
                return err
            uid_bin = struct.unpack('>BBB', self._fix_size_recv(3))
            uid = int((uid_bin[2]*256 + uid_bin[1])*256 + uid_bin[0])
            if idt == TERROR:
                err = self._get_string()
            elif idt == TINT:
                res = self._get_int()
            elif idt == TFLOAT:
                res = self._get_float()
            elif idt == TTIME:
                res = self._get_time()
            elif idt == TDURATION:
                res = self._get_duration()
            elif idt == TSTRING:
                res = self._get_string()
            elif idt == TARRAY:
                # parallel array, retreive array header
                self._get_array_header(uid)
                # read next info
                continue
            elif idt == TARRAYSERIAL:
                # serialized array
                res = self._serial_get(idt)
            elif idt == TTIMESERIE:
                self.tserie[uid] = self._get_timeserie_label()
                continue
            elif idt == TSENTINEL:
                res = self._get_sentinel()
                if res == Sentinel.End:
                    # this is the end
                    return err
            elif idt == TBOOL:
                res = self._get_bool()
            elif idt == TDEC64:
                res = self._get_dec64()
            elif idt == TPAIR:
                res = self._get_pair()
            elif idt == THEARTBEAT:
                res = self._get_heartbeat()
            elif idt == TTENSOR:
                res = self._get_tensor()
            else:
                err = "Unhandled type %d" % idt
            if err != "":
                return err
            # Is it from a timeserie ?
            if self._is_in_timeserie(uid):
                # always time after timeserie element
                time = self._get_time()
                point = Point(time, res)
                func(self, uid, point)
            else:
                func(self, uid, res)
        return err

    def get_result(self, limit=-1):
        """Retrieve complet result by calling walk_result internally"""
        context = ReaderContext(limit)

        def closure(_, uid, value):
            """fill result, called by walk"""
            if isinstance(value, HeartBeat):
                # heartbeat nothing to do, just read it
                return
            if context.update_limit_and_check():
                return
            # are we in an array ???
            pos, is_in_array = self._get_array_where(uid)
            # are we in timeserie
            is_in_ts = self._is_in_timeserie(uid)

            if is_in_ts:
                # this is a point in a timeserie
                tserie = context.get_timeserie(uid)
                if tserie is None:
                    # 1st create it
                    tserie = []
                # this is a timeserie so recive a Point
                tserie.append(value)
                context.set_timeserie(uid, tserie)
            if is_in_array:
                # this is part of array pos.uid
                array = context.get_array(pos.uid)
                if array is None:
                    # 1st creat it
                    array = [None] * self._get_array_size_by_id(pos.uid)
                    context.set_array(pos.uid, array)
                if is_in_ts:
                    array[pos.pos] = context.get_timeserie(uid)
                else:
                    array[pos.pos] = value
                return
            # simple single result
            if not is_in_ts:
                context.res = value
            return

        err = self.walk_result(closure)

        if (err == "") & context.limit_reached:
            self.con.close()
            raise LispTickException(
                "Points limit reached, use streaming or (graphsample)")
        if err != "":
            self.con.close()
            raise LispTickException(err)

        root_array = context.get_array(0)
        if root_array is not None:
            # is array
            # replace timeserie by timeseries them selfs
            # to do store timeserie uid in array ??
            i = 0
            for val in root_array:
                if isinstance(val, list):
                    context.arrays[0][i] = context.get_timeserie(i+1)
                    i = i + 1
            context.res = root_array
        else:
            # only one timeserie
            if len(context.timeseries) == 1:
                for _, tserie in context.timeseries.items():
                    context.res = tserie
        return context.res

    def _serial_get(self, idt):
        """Element has been serialized as it is a point of a timeserie"""
        # Retreive result type
        if idt == TERROR:
            err = self._get_string()
            self.con.close()
            raise LispTickException(err)
        elif idt == TNULL:
            return None
        elif idt == TINT:
            return self._get_int()
        elif idt == TFLOAT:
            return self._get_float()
        elif idt == TTIME:
            return self._get_time()
        elif idt == TDURATION:
            return self._get_duration()
        elif idt == TSTRING:
            return self._get_string()
        elif idt == TARRAYSERIAL:
            # serialized array
            # read size
            size = struct.unpack("<q", self._fix_size_recv(8))[0]
            res = [None] * size
            for i in range(0, size):
                serial_type = self._fix_size_recv(1)
                if serial_type == b'':
                    return None
                # consume unused id
                struct.unpack('>BBB', self._fix_size_recv(3))
                res[i] = self._serial_get(serial_type)
            return res
        elif idt == TSENTINEL:
            return self._get_sentinel()
        elif idt == TBOOL:
            return self._get_bool()
        elif idt == TDEC64:
            return self._get_dec64()
        elif idt == TPAIR:
            return self._get_pair()
        elif idt == THEARTBEAT:
            return self._get_heartbeat()
        elif idt == TTENSOR:
            return self._get_tensor()
        else:
            error = "Unhandled type %d" % idt
            self.con.close()
            raise LispTickException(error)

    def _get_array_where(self, uid):
        # tell if it is in an Array and where
        if self.where.get(uid) is None:
            return None, False
        return self.where.get(uid), True

    def _get_array_size_by_id(self, uid):
        # return size of Array with uid as ID
        return self.sizes.get(uid)

    def _is_in_timeserie(self, uid):
        # tell if it is in a timeserie
        if self.tserie.get(uid) is not None:
            return True
        return False

    def _get_array_header(self, uid):
        # get array size return read size
        self.sizes[uid] = struct.unpack("<q", self._fix_size_recv(8))[0]
        for i in range(0, self.sizes.get(uid)):
            header_type = self._fix_size_recv(1)
            if header_type == b'':
                return
            header_uid = self._fix_size_recv(3)
            if header_uid == b'':
                return
            header_uid = struct.unpack('>BBB', header_uid)
            header_uid = int(
                (header_uid[2]*256 + header_uid[1])*256 + header_uid[0])
            self.where[header_uid] = InArray(uid, i)

    def _get_timeserie_label(self):
        # size is number of points -> x8 to have bytes...
        # sizeLimit is max number of points, -1 means no limit
        return self._get_string()
        # some kind of Label sent first

    def _get_int(self):
        """Int64 LittleEndian"""
        return struct.unpack('<q', self._fix_size_recv(8))[0]

    def _get_sentinel(self):
        """Int64 LittleEndian"""
        return Sentinel(struct.unpack('<q', self._fix_size_recv(8))[0])

    def _get_dec64(self):
        """Dec64 special encoding see https://www.crockford.com/dec64.html"""
        d64 = struct.unpack('<q', self._fix_size_recv(8))[0]
        if (d64 % 256) > 127:
            return (d64 >> 8) / factors[256 - (d64 % 256)]
        else:
            return (d64 >> 8) * factors[d64 % 256]

    def _get_float(self):
        """Float64 LittleEndian"""
        return struct.unpack('<d', self._fix_size_recv(8))[0]

    def _get_time(self):
        """Nano second since epoch as a Int64"""
        # UnixNano time
        epoch = struct.unpack('<q', self._fix_size_recv(8))[0]
        # Python only handles microsecond
        return epoch_datetime(epoch)

    def _get_duration(self):
        """Nano seconds duration as a Int64"""
        # UnixNano time
        year = struct.unpack('<q', self._fix_size_recv(8))[0]
        month = struct.unpack('<q', self._fix_size_recv(8))[0]
        day = struct.unpack('<q', self._fix_size_recv(8))[0]
        epoch = struct.unpack('<q', self._fix_size_recv(8))[0]
        # Python only handles microsecond
        # round year and month to days
        return Duration(year, month, day, epoch)

    def _get_string(self):
        """Simple string from socket, first size then string"""
        size = struct.unpack("<q", self._fix_size_recv(8))[0]
        return str(self._fix_size_recv(size).decode())

    def _get_bool(self):
        if struct.unpack('<q', self._fix_size_recv(8))[0] == 0:
            return False
        return True

    def _get_pair(self):
        serial_type = self._fix_size_recv(1)
        if serial_type == b'':
            return (None, None)
        # consume unused ID
        struct.unpack('>BBB', self._fix_size_recv(3))
        head = self._serial_get(serial_type)
        serial_type = self._fix_size_recv(1)
        if serial_type == b'':
            return (head, None)
        # consume unused ID
        struct.unpack('>BBB', self._fix_size_recv(3))
        return (head, self._serial_get(serial_type))

    def _get_heartbeat(self):
        """HeartBeat gives progression and ensure client is still listening"""
        serial_type = self._fix_size_recv(1)
        if serial_type == b'':
            return None
        # consume unused ID
        struct.unpack('>BBB', self._fix_size_recv(3))
        value = self._serial_get(serial_type)
        return HeartBeat(value)

    def _get_tensor(self):
        serial_type = self._fix_size_recv(1)
        if serial_type == b'':
            return None
        # consume unused ID
        struct.unpack('>BBB', self._fix_size_recv(3))
        shape = self._serial_get(serial_type)
        tensor = Tensor(shape)

        # too verbose experimental implementation
        # in future will be gzip list of same type
        for i in range(tensor.get_size()):
            serial_type = self._fix_size_recv(1)
            if serial_type == b'':
                return None
            # consume unused ID
            struct.unpack('>BBB', self._fix_size_recv(3))
            value = self._serial_get(serial_type)
            tensor.values[i] = value
        return tensor

    def _fix_size_recv(self, size):
        """Ensure size is received and not less"""
        res = self.con.recv(size)
        while len(res) < size:
            res += self.con.recv(size - len(res))
        return res


def epoch_datetime(epoch):
    """Transform 64bits epoch to datetime"""
    # Empty ?
    if epoch == -6795364578871345152:
        return datetime.time()
    return datetime.datetime.fromtimestamp(epoch / 1e9)


def send_message(sock, request):
    """Send request to LispTick"""
    msg = json.dumps({"code": request}).encode()
    if len(msg) > 65536:
        raise RuntimeError("message for LispTick is >64KB")
    bsize = bytearray()
    bsize.append(len(msg) % 256)
    bsize.append(int(len(msg)/256))
    sock.send(bsize)
    totalsent = 0
    while totalsent < len(msg):
        sent = sock.send(msg[totalsent:])
        if sent == 0:
            raise RuntimeError("socket connection broken")
        totalsent = totalsent + sent
