#!/usr/bin/env python
# coding: utf-8

"""
A pythonic python-only implementation of Tokyo Tyrant protocol. 
Python 2.4+ is needed.

This library takes a "pythonic" approach to make it more clear and easy
to implement.

More information about Tokyo Cabinet: 
    http://tokyocabinet.sourceforge.net/

More information about Tokyo Tyrant: 
    http://tokyocabinet.sourceforge.net/tyrantdoc/

This is an usage example:

    >>> import pyrant
    >>> t = pyrant.Tyrant(host='127.0.0.1', port=1978)
    >>> if t.dbtype != pyrant.DBTYPETABLE:
    ...     t['key'] = 'foo'
    ...     print t['key']
    ... else:
    ...     t['key'] = {'name': 'foo'}
    ...     print t['key']['name']
    foo
    >>> del t['key']
    >>> print t['key']
    Traceback (most recent call last):
        ...
    KeyError: 'key'

"""

import itertools as _itertools
from protocol import TyrantProtocol, TyrantError

__version__ = '0.0.2'

# Constants
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 1978
MAX_RESULTS = 1000

# Table Types
DBTYPEBTREE = 'B+ tree'
DBTYPETABLE = 'table'
DBTYPEMEMORY = 'on-memory hash'
DBTYPEHASH = 'hash'


def _parse_elem(elem, dbtype, sep=None):
    if dbtype == DBTYPETABLE:
        # Split element by \x00 which is the column separator
        elems = elem.split('\x00')
        if not elems[0]:
            return None

        return dict((elems[i], elems[i + 1]) \
                        for i in xrange(0, len(elems), 2))
    elif sep and sep in elem:
        return elem.split(sep)

    return elem

class Tyrant(dict,TyrantProtocol):
    """Main class of Tyrant implementation. Acts like a python dictionary,
    so you can query object using normal subscript operations.
    """

    def __init__(self, separator=None, literal=False):
        """
        Acts like a python dictionary. Params are:
            
        host: Tyrant Host address
        port: Tyrant port number
        separator: If this parameter is set, you can put and get lists as
        values.
        literal: If is set string is returned instead of unicode
        """
        # We want to make protocol public just in case anyone need any
        # specific option
        self.separator = separator
        self.literal = literal
        self._get_db_type()

    @defer.inlineCallbacks
    def _get_db_type(self):
        self.dbtype = yield self.get_stats()['type']

    @defer.inlineCallbacks
    def __contains__(self, key):
        try:
            self.vsiz(key)
        except TyrantError:
            return False
        else:
            return True

    @defer.inlineCallbacks
    def __delitem__(self, key):
        try:
            return self.proto.out(key)
        except TyrantError:
            raise KeyError(key)

    @defer.inlineCallbacks
    def __getitem__(self, key):
        try:
            return _parse_elem(self.proto.get(key, self.literal), self.dbtype,
                               self.separator)
        except TyrantError:
            raise KeyError(key)

    @defer.inlineCallbacks
    def get(self, key, default=None):
        """Allow for getting with a default.
          
           >>> t = Tyrant()
           >>> t['foo'] = {'a': 'z', 'b': 'y'}
           >>> print t.get('foo', {})
           {u'a': u'z', u'b': u'y'}
           >>> print t.get('bar', {})
           {}

        """
        try:
            return self[key]
        except KeyError:
            return default

    @defer.inlineCallbacks
    def __len__(self):
        return self.proto.rnum()

    @defer.inlineCallbacks
    def __repr__(self):
        return object.__repr__(self)

    @defer.inlineCallbacks
    def __setitem__(self, key, value):
        if isinstance(value, dict):
            flat = _itertools.chain([key], *value.iteritems())
            self.proto.misc('put', list(flat))
            
        elif isinstance(value, (list, tuple)):
            assert self.separator, "Separator is not set"

            flat = self.separator.join(value)
            self.proto.put(key, flat)

        else:
            self.proto.put(key, value)


    @defer.inlineCallbacks
    def call_func(self, func, key, value, record_locking=False, 
                  global_locking=False):
        """Call specific function.
        """
        opts = ((record_locking and TyrantProtocol.RDBXOLCKREC) |
                (global_locking and TyrantProtocol.RDBXOLCKGLB))
        return self.proto.ext(func, opts, key, value)

    @defer.inlineCallbacks
    def clear(self):
        """Used in order to remove all records of a remote database object"""
        self.proto.vanish()

    @defer.inlineCallbacks
    def concat(self, key, value, width=None):
        """Concatenate columns of the existing record"""
        if width is None:
            self.proto.putcat(key, value)
        else:
            self.proto.putshl(key, value, width)

    @defer.inlineCallbacks
    def get_size(self, key):
        """Get the size of the value of a record"""
        try:
            return self.proto.vsiz(key)
        except TyrantError:
            raise KeyError(key)

    @defer.inlineCallbacks
    def get_stats(self):
        """Get the status string of the database.
        The return value is the status message of the database.The message 
        format is a dictionary. 
        """ 
        return dict(l.split('\t', 1) \
                        for l in self.proto.stat().splitlines() if l)

    @defer.inlineCallbacks
    def iterkeys(self):
        """Iterate keys using remote operations"""
        self.proto.iterinit()
        try:
            while True:
                yield self.proto.iternext()
        except TyrantError:
            pass

    @defer.inlineCallbacks
    def keys(self):
        """Return the list of keys in database"""
        return list(self.iterkeys())

    @defer.inlineCallbacks
    def update(self, other, **kwargs):
        """Update/Add given objects into database"""
        self.multi_set(other.iteritems())
        if kwargs:
            self.update(kwargs)

    @defer.inlineCallbacks
    def multi_del(self, keys, no_update_log=False):
        """Remove given records from database"""
        opts = (no_update_log and TyrantProtocol.RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)

        self.proto.misc("outlist", keys, opts)

    @defer.inlineCallbacks
    def multi_get(self, keys, no_update_log=False):
        """Returns a list of records that match given keys"""
        opts = (no_update_log and TyrantProtocol.RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)

        rval = self.proto.misc("getlist", keys, opts)
        
        if len(rval) <= len(keys):
            # 1.1.10 protocol, may return invalid results
            if len(rval) < len(keys):
                raise KeyError("Missing a result, unusable response in 1.1.10")

            return rval

        # 1.1.11 protocol returns interleaved key, value list
        d = dict((rval[i], _parse_elem(rval[i + 1], self.dbtype, 
                                       self.separator)) \
                    for i in xrange(0, len(rval), 2))
        return d

    @defer.inlineCallbacks
    def multi_set(self, items, no_update_log=False):
        """Store given records into database"""
        opts = (no_update_log and TyrantProtocol.RDBMONOULOG or 0)
        lst = []
        for k, v in items.iteritems():
            if isinstance(v, (list, tuple)):
                assert self.separator, "Separator is not set"

                v = self.separator.join(v)
            lst.extend((k, v))

        self.proto.misc("putlist", lst, opts)

    @defer.inlineCallbacks
    def get_int(self, key):
        """Get an integer for given key. Must been added by addint"""
        return self.proto.getint(key)
    
    @defer.inlineCallbacks
    def get_double(self, key):
        """Get a double for given key. Must been added by adddouble"""
        return self.proto.getdouble(key)

    @defer.inlineCallbacks
    def prefix_keys(self, prefix, maxkeys=None):
        """Get forward matching keys in a database.
        The return value is a list object of the corresponding keys.
        """
        if maxkeys is None:
            maxkeys = len(self)

        return self.proto.fwmkeys(prefix, maxkeys)

    @defer.inlineCallbacks
    def sync(self):
        """Synchronize updated content into database"""
        self.proto.sync()

    @defer.inlineCallbacks
    def _get_query(self):
        return Query(self, self.dbtype, self.literal)

    query = property(_get_query)


class Q(object):
    """Condition object. You can | this type to ORs conditions,
    but you cannot use operand "&", to do this just add more Q to your filter
    Example:
        
        >>> t = Tyrant()
        >>> t.clear()
        >>> t.dbtype == pyrant.DBTYPETABLE
        True
        >>> t['i'] = {'name': 'Reflejo', 'test': 0}
        >>> t['you'] = {'name': 'Fulano', 'test': 1}
        >>> res = t.query.filter(Q(name='Reflejo'), Q(test=0))
        >>> res[0]['i']['name']
        u'Reflejo'
        >>> res = t.query.filter(Q(name='Reflejo') | Q(name='Fulano'))
        >>> len(res)
        2

    """

    def __init__(self, **kwargs):
        assert kwargs, "You need to specify at least one condition"

        for kw, val in kwargs.iteritems():
            nameop = kw.split('__')
            self._op = 's' if isinstance(val, (str, unicode)) else 'n'
            self._op += nameop[1] if len(nameop) > 1 else 'eq'
            self.name = nameop[0]
            self.expr = val

    def __or__(self, q):
        import copy
        assert isinstance(q, Q), "Unsupported operand type(s) for |"
        
        op = '%s_or' % q._op
        if q._op == self._op and op in TyrantProtocol.conditionsmap:
            qcopy = copy.copy(q)
            qcopy._op = op
            qcopy.expr = "%s,%s" % (q.expr , self.expr)
            
            return qcopy
        else:
            raise TypeError("Unsoported operand for |. You can only do this "\
                            "on contains or eq")

    def _getop(self):
        return TyrantProtocol.conditionsmap[self._op]

    op = property(_getop)

    def __repr__(self):
        return "%s [%s] %s" % (self.name, self.op, self.expr)


class Query(object):
    """Query table operations. This is a lazy object
    that abstract all queries for tyrant protocol.

    Usage:

    # Insert object into database
    >>> t = Tyrant()
    >>> t.dbtype == pyrant.DBTYPETABLE
    True
    >>> t['i'] = {'name': 'Reflejo', 'test': 0}

    # Now let's see if we can reach it querying tyrant
    >>> res = t.query.filter(name='Reflejo', test=0).order('name')
    >>> print res[0]['i']['name']
    Reflejo

    """

    def __init__(self, proto, dbtype, literal=False):
        self._conditions = []
        self._order = None
        self._order_t = 0
        self._cache = {}
        self._proto = proto
        self._dbtype = dbtype
        self.literal = literal

    def order(self, name):
        """Define result order. name parameter is the column name. 
        You can prefix "-" to order desc.
        If "#" is added just before column name, column are ordered as numbers

        Examples:
            order('-name')
            order('-#ranking')
            order('name')

        """
        if name.startswith('-'):
            if name.startswith('-#'):
                order = (name[2:], TyrantProtocol.RDBQONUMDESC)
            else:
                order = (name[1:], TyrantProtocol.RDBQOSTRDESC)
        elif name.startswith('#'):
            order = (name[1:], TyrantProtocol.RDBQONUMASC)
        else:
            order = (name, TyrantProtocol.RDBQOSTRASC)

        if self._order != order[0] or self._order_t != order[1]:
            # Add another rule means that our naive cache should be empty'ed
            self._cache = {}
            self._order = order[0]
            self._order_t = order[1]

        return self

    def filter(self, *args, **kwargs):
        """Add condition to query. This could be done by Q object or by keyword
        arguments. Keys are: 
            __eq: Equals (default) to expression
            __lt: Less than expression
            __le: Less or equal to expression
            __gt: Greater than expression
            __ge: Greater or equal to expression

        Example:
            >>> t = Tyrant()
            >>> t.get_stats()['type']
            u'table'
            >>> t['i'] = {'name': 'Reflejo', 'test': 4}
            >>> t['i2'] = {'name': 'Reflejo', 'test': 4}
            >>> t['i3'] = {'name': 'Reflejo', 'test': 4}

            >>> res = t.query.filter(test__gt=0)[:1]
            >>> len(res) == 1
            True
            >>> print res[0]['i']['name']
            Reflejo

        """
        # Add another rule means that our naive cache should be empty'ed
        self._cache = {}

        # Iterate arguments. Should be instances of Q
        for q in args:
            assert isinstance(q, Q), "Arguments must be instances of Q"
            self._conditions.append(q)

        # Generate Q with arguments as needed
        for name, expr in kwargs.iteritems():
            self._conditions.append(Q(**{name: expr}))

        return self

    def __len__(self):
        return len(self[:])

    def __repr__(self):
        # Do the query using getitem
        return str(self[:])

    def __getitem__(self, k):
        # Retrieve an item or slice from the set of results.
        if not isinstance(k, (slice, int, long)):
            raise TypeError("ResultSet indices must be integers")

        # Check slice integrity
        assert (not isinstance(k, slice) and (k >= 0)) \
            or (isinstance(k, slice) and (k.start is None or k.start >= 0) \
            and (k.stop is None or k.stop >= 0)), \
            "Negative indexing is not supported."

        if isinstance(k, slice):
            offset = k.start or 0
            limit = (k.stop - offset) if k.stop is not None else MAX_RESULTS
        else:
            offset = k
            limit = 1

        cache_key = "%s_%s" % (offset, limit)
        if cache_key in self._cache:
            return self._cache[cache_key]

        conditions = [(c.name, c.op, c.expr) for c in self._conditions]

        # Do the search.
        keys = self._proto.search(conditions, limit, offset,
                                  order_type=self._order_t,
                                  order_field=self._order)

        # Since results are keys, we need to query for actual values
        if isinstance(k, slice):
            ret = [{key: _parse_elem(self._proto.get(key, self.literal),
                                     self._dbtype)} \
                        for key in keys]
        else:
            ret = {
                keys[0]: _parse_elem(self._proto.get(keys[0], self.literal), 
                                     self._dbtype)
            }

        self._cache[cache_key] = ret
        return ret
