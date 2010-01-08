#!/usr/bin/env python
# coding: utf-8

"""
Protocol implementation for Tyrant 
<http://tokyocabinet.sourceforge.net/tyrantdoc/>
tx version coded by Mikhail Krivushin aka Deepwalker
"""

import math
import socket
import struct

from twisted.internet import defer, protocol, reactor

class TyrantError(Exception):
    """
    Tyrant error, socket and communication errors are not included here.
    """

# pyrant constants
MAGIC_NUMBER = 0xc8
ENCODING = 'UTF-8'


def _ulen(expr):
    return len(expr.encode(ENCODING)) \
            if isinstance(expr, unicode) else len(expr)


def _pack(code, *args):
    # Craft string that we'll use to send data based on args type and content
    buf = ''
    fmt = '>BB'
    largs = []
    for arg in args:
        if isinstance(arg, int):
            fmt += 'I'
            largs.append(arg)

        elif isinstance(arg, str):
            buf += arg

        elif isinstance(arg, unicode):
            buf += arg.encode(ENCODING)
        
        elif isinstance(arg, long):
            fmt += 'Q'
            largs.append(arg)

        elif isinstance(arg, (list, tuple)):
            for v in arg:
                v = str(v)
                buf += "%s%s" % (struct.pack(">I", len(v)), v)

    return "%s%s" % (struct.pack(fmt, MAGIC_NUMBER, code, *largs), buf)

# Здесь будет город-сад, точнее twisted протокол.
class TyrantProtocol(protocol.Protocol):
    """Tyrant protocol raw implementation. There are all low level constants
    and operations. You can use it if you need that atomicity in your requests
    """

    # Protocol commands
    PUT = 0x10
    PUTKEEP = 0x11
    PUTCAT = 0x12
    PUTSHL = 0x13
    PUTNR = 0x18
    OUT = 0x20
    GET = 0x30
    MGET = 0x31
    VSIZ = 0x38
    ITERINIT = 0x50
    ITERNEXT = 0x51
    FWMKEYS = 0x58
    ADDINT = 0x60
    ADDDOUBLE = 0x61
    EXT = 0x68
    SYNC = 0x70
    VANISH = 0x72
    COPY = 0x73
    RESTORE = 0x74
    SETMST = 0x78
    RNUM = 0x80
    SIZE = 0x81
    STAT = 0x88
    MISC = 0x90

    # Query conditions
    RDBQCSTREQ = 0    # string is equal to 
    RDBQCSTRINC = 1   # string is included in 
    RDBQCSTRBW = 2    # string begins with 
    RDBQCSTREW = 3    # string ends with 
    RDBQCSTRAND = 4   # string includes all tokens in 
    RDBQCSTROR = 5    # string includes at least one token in 
    RDBQCSTROREQ = 6  # string is equal to at least one token in 
    RDBQCSTRRX = 7    # string matches regular expressions of 
    RDBQCNUMEQ = 8    # number is equal to 
    RDBQCNUMGT = 9    # number is greater than 
    RDBQCNUMGE = 10   # number is greater than or equal to 
    RDBQCNUMLT = 11   # number is less than 
    RDBQCNUMLE = 12   # number is less than or equal to 
    RDBQCNUMBT = 13   # number is between two tokens of 
    RDBQCNUMOREQ = 14 # number is equal to at least one token in 
    RDBQCNEGATE = 15  # negation flag 
    RDBQCNOIDX = 16   # no index flag 

    # Order
    RDBQOSTRASC = 0   # string ascending 
    RDBQOSTRDESC = 1  # string descending 
    RDBQONUMASC = 2   # number ascending 
    RDBQONUMDESC = 3  # number descending 

    # Opts
    RDBMONOULOG = 1
    RDBXOLCKREC = 1
    RDBXOLCKGLB = 2

    conditionsmap = {
        # String conditions
        'seq': RDBQCSTREQ,
        'scontains': RDBQCSTRINC,
        'sstartswith': RDBQCSTRBW,
        'sendswith': RDBQCSTREW,
        'smatchregex': RDBQCSTRRX,
        
        # Numbers conditions
        'neq': RDBQCNUMEQ,
        'ngt': RDBQCNUMGT,
        'nge': RDBQCNUMGE,
        'nlt': RDBQCNUMLT,
        'nle': RDBQCNUMLE,

        # Multiple conditions
        'scontains_or': RDBQCSTROR,
        'seq_or': RDBQCSTROREQ,
        'neq_or': RDBQCNUMOREQ
    }
    ########
    def __init__(self):
        self.bufer = ''
        self.recv_fifo=[]

    def dataReceived(self, data):
        #print "Data recieved", repr(data)
        self.bufer+=data
        while self.recv_fifo:
            d,bytes=self.recv_fifo[0]
            if bytes <= len(self.bufer):
                res = self.bufer[:bytes]
                self.bufer = self.bufer[bytes:]
                self.recv_fifo.pop(0)
                d.callback(res)
            else:
                break

    @defer.inlineCallbacks
    def sock_send(self, *args,**kwargs):
        """Pack arguments and send the buffer to the socket"""
        #print "Посылка -", args, kwargs
        sync = kwargs.pop('sync', True)
        # Send message to socket, then check for errors as needed.
        self.transport.write(_pack(*args))

        #fail_code = yield self.get_byte()
        fail_code = yield self.recv(1)
        fail_code = ord(fail_code)
        if fail_code:
            raise TyrantError(fail_code)
        defer.returnValue(True)

    @defer.inlineCallbacks
    def recv(self, bytes):
        """Get given bytes from socket"""
        #print "Try to get bytes",bytes,repr(self.bufer)
        if bytes < len(self.bufer):
            res = self.bufer[:bytes]
            self.bufer = self.bufer[bytes:]
            defer.returnValue(res)
        else:
            d = defer.Deferred()
            self.recv_fifo.append((d,bytes))
            #print self.recv_fifo
            res = yield d
            defer.returnValue(res)

    def get_byte(self):
        """Get 1 byte from socket."""
        return self.recv(1)

    @defer.inlineCallbacks
    def get_int(self):
        """Get an integer (4 bytes) from socket."""
        res = yield self.recv(4)
        defer.returnValue(struct.unpack('>I', res)[0])

    @defer.inlineCallbacks
    def get_long(self):
        """Get a long (8 bytes) from socket."""
        res = yield self.recv(8)
        defer.returnValue(struct.unpack('>Q', res)[0])

    @defer.inlineCallbacks
    def get_str(self):
        """Get a string (n bytes, which is an integer just before string)."""
        data_len = yield self.get_int()
        string = yield self.recv(data_len)
        defer.returnValue(string)

    @defer.inlineCallbacks
    def get_unicode(self):
        """Get a unicode."""
        string = yield self.get_str()
        defer.returnValue(string.decode(ENCODING))

    @defer.inlineCallbacks
    def get_double(self):
        """Get 2 long numbers (16 bytes) from socket"""
        data = yield self.recv(16)
        intpart, fracpart = struct.unpack('>QQ', data)
        defer.returnValue(intpart + (fracpart * 1e-12))

    @defer.inlineCallbacks
    def get_strpair(self):
        """Get string pair (n bytes, n bytes which are 2 integers just 
        before pair)"""
        klen = yield self.get_int()
        vlen = yield self.get_int()
        kstr = yield self.recv(klen)
        vstr = yield self.recv(vlen)
        defer.returnValue(kstr,vstr)
    ########

    def put(self, key, value):
        """Unconditionally set key to value
        """
        return self.sock_send(self.PUT, _ulen(key), _ulen(value), key, value)

    def putkeep(self, key, value):
        """Set key to value if key does not already exist
        """
        return self.sock_send(self.PUTKEEP, _ulen(key), _ulen(value), key, value)

    def putcat(self, key, value):
        """Append value to the existing value for key, or set key to
        value if it does not already exist
        """
        return self.sock_send(self.PUTCAT, _ulen(key), _ulen(value), key, value)

    def putshl(self, key, value, width):
        """Equivalent to::

            self.putcat(key, value)
            self.put(key, self.get(key)[-width:])
        """
        return self.sock_send(self.PUTSHL, _ulen(key), _ulen(value), width, key, value)

    def putnr(self, key, value):
        """Set key to value without waiting for a server response
        """
        return self.sock_send(self.PUTNR, _ulen(key), _ulen(value), key, value)

    def out(self, key):
        """Remove key from server
        """
        return self.sock_send(self.OUT, _ulen(key), key)

    @defer.inlineCallbacks
    def get(self, key, literal=False):
        """Get the value of a key from the server
        """
        yield self.sock_send(self.GET, _ulen(key), key)
        if literal:
            data = yield self.get_str()
        else:
            data = yield self.get_unicode()
        defer.returnValue(data)

    @defer.inlineCallbacks
    def getint(self, key):
        """Get an integer for given key. Must been added by addint"""
        yield self.sock_send(self.GET, _ulen(key), key)
        val = yield self.get_str()
        defer.returnValue(struct.unpack('I', val)[0])

    @defer.inlineCallbacks
    def getdouble(self, key):
        """Get a double for given key. Must been added by adddouble"""
        yield self.sock_send(self.GET, _ulen(key), key)
        val = yield self.get_str()
        intpart, fracpart = struct.unpack('>QQ', val)
        defer.returnValue(intpart + (fracpart * 1e-12))

    @defer.inlineCallbacks
    def mget(self, klst):
        """Get key,value pairs from the server for the given list of keys
        """
        yield self.sock_send(self.MGET, len(klst), klst)
        numrecs = yield self.get_int()
        res=[]
        for i  in xrange(numrecs):
            data = yield self.get_strpair()
            res.append(data)
        defer.returnValue(res)

    @defer.inlineCallbacks
    def vsiz(self, key):
        """Get the size of a value for key
        """
        yield self.sock_send(self.VSIZ, _ulen(key), key)
        data=yield self.get_int()
        defer.returnValue(data)

    def iterinit(self):
        """Begin iteration over all keys of the database
        """
        return self.sock_send(self.ITERINIT)

    @defer.inlineCallbacks
    def iternext(self):
        """Get the next key after iterinit
        """
        yield self.sock_send(self.ITERNEXT)
        defer.returnValue(self.get_unicode())

    @defer.inlineCallbacks
    def fwmkeys(self, prefix, maxkeys):
        """Get up to the first maxkeys starting with prefix
        """
        yield self.sock_send(self.FWMKEYS, _ulen(prefix), maxkeys, prefix)
        numkeys = yield self.get_int()
        res = []
        for i in xrange(numkeys):
            data = yield self.get_unicode()
            res.append(data)
        defer.returnValue(res)

    @defer.inlineCallbacks
    def addint(self, key, num):
        """Sum given integer to existing one
        """
        yield self.sock_send(self.ADDINT, _ulen(key), num, key)
        res = yield self.get_int()
        defer.returnValue(res)

    @defer.inlineCallbacks
    def adddouble(self, key, num):
        """Sum given double to existing one
        """
        fracpart, intpart = math.modf(num)
        fracpart, intpart = int(fracpart * 1e12), int(intpart)
        yield self.sock_send(self.ADDDOUBLE, _ulen(key), long(intpart), 
                        long(fracpart), key)
        res = yield self.get_double()
        defer.returnValue(res)

    def ext(self, func, opts, key, value):
        """Call func(key, value) with opts

        opts is a bitflag that can be RDBXOLCKREC for record locking
        and/or RDBXOLCKGLB for global locking"""
        yield self.sock_send(self.EXT, len(func), opts, _ulen(key), _ulen(value),
                        func, key, value)
        res = yield self.get_unicode()
        defer.returnValue(res)

    def sync(self):
        """Synchronize the database
        """
        return self.sock_send(self.SYNC)

    def vanish(self):
        """Remove all records
        """
        return self.sock_send(self.VANISH)

    def copy(self, path):
        """Hot-copy the database to path
        """
        return self.sock_send(self.COPY, _ulen(path), path)

    def restore(self, path, msec):
        """Restore the database from path at timestamp (in msec)
        """
        return self.sock_send(self.RESTORE, _ulen(path), msec, path)

    def setmst(self, host, port):
        """Set master to host:port
        """
        return self.sock_send(self.SETMST, len(host), port, host)

    @defer.inlineCallbacks
    def rnum(self):
        """Get the number of records in the database
        """
        yield self.sock_send(self.RNUM)
        res = yield self.get_long()
        defer.returnValue(res)

    @defer.inlineCallbacks
    def size(self):
        """Get the size of the database
        """
        yield self.sock_send(self.SIZE)
        res = yield self.get_long()
        defer.returnValue(res)

    @defer.inlineCallbacks
    def stat(self):
        """Get some statistics about the database
        """
        yield self.sock_send(self.STAT)
        res = yield self.get_unicode()
        defer.returnValue(res)

    def search(self, conditions, limit=10, offset=0, 
               order_type=0, order_field=None, opts=0):
        """Search table elements. args should be (field, opt, expr) tuple
        """
        args = ["addcond\x00%s\x00%d\x00%s" % cond for cond in conditions]

        # Set order in query
        if order_field:
            args += ['setorder\x00%s\x00%d' % (order_field, order_type)]

        # Set limit and offset
        if limit > 0 and offset >= 0:
            args += ['setlimit\x00%d\x00%d' % (limit, offset)]

        return self.misc('search', args, opts)

    @defer.inlineCallbacks
    def misc(self, func, args, opts=0):
        """All databases support "putlist", "outlist", and "getlist".
        "putlist" is to store records. It receives keys and values one after
        the other, and returns an empty list.
        "outlist" is to remove records. It receives keys, and returns an empty
        list.
        "getlist" is to retrieve records. It receives keys, and returns values

        Table database supports "setindex", "search", "genuid".
        opts is a bitflag that can be:
            RDBMONOULOG to prevent writing to the update log
        """
        try:
            yield self.sock_send(self.MISC, len(func), opts, len(args), func, args)
        finally:
            numrecs = yield self.get_int()
        
        res = []
        for i in xrange(numrecs):
            data = yield self.get_unicode()
            res.append(data)
        defer.returnValue(res)

###
# test
##

@defer.inlineCallbacks
def test_proto():
    cc = yield protocol.ClientCreator(reactor, TyrantProtocol).connectTCP("127.0.0.1",1978)
    yield cc.put('test','tutu123123123123123213')
    res = yield cc.get('test')
    print res
    reactor.stop()

if __name__=='__main__':
    test_proto()
    reactor.run()
