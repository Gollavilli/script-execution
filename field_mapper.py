import sys
import datetime
PY2 = sys.version_info[0] == 2

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class ProgrammingError(Exception):
    """Exception for errors programming errors"""
    pass


class MySQLConverterBase(object):
    """Base class for conversion classes
    All class dealing with converting to and from MySQL data types must
    be a subclass of this class.
    """

    def __init__(self, charset='utf8', use_unicode=True):
        self.python_types = None
        self.mysql_types = None
        self.charset = None
        self.charset_id = 0
        self.use_unicode = None
        self._cache_field_types = {}



    def to_mysql(self, value):
        """Convert Python data type to MySQL"""
        type_name = value.__class__.__name__.lower()
        try:
            return getattr(self, "_{0}_to_mysql".format(type_name))(value)
        except AttributeError:
            return value

    def to_python(self, vtype, value):
        """Convert MySQL data type to Python"""

        if (value == b'\x00' or value is None) and vtype[1] != FieldType.BIT:
            # Don't go further when we hit a NULL value
            return None

        if not self._cache_field_types:
            self._cache_field_types = {}
            for name, info in FieldType.desc.items():
                try:
                    self._cache_field_types[info[0]] = getattr(
                        self, '_{0}_to_python'.format(name))
                except AttributeError:
                    # We ignore field types which has no method
                    pass

        try:
            return self._cache_field_types[vtype[1]](value, vtype)
        except KeyError:
            return value

    def escape(self, buf):
        """Escape buffer for sending to MySQL"""
        return buf

    def quote(self, buf):
        """Quote buffer for sending to MySQL"""
        return str(buf)


class MySQLConverter(MySQLConverterBase):
    """Default conversion class for MySQL Connector/Python.
     o escape method: for escaping values send to MySQL
     o quoting method: for quoting values send to MySQL in statements
     o conversion mapping: maps Python and MySQL data types to
       function for converting them.
    Whenever one needs to convert values differently, a converter_class
    argument can be given while instantiating a new connection like
    cnx.connect(converter_class=CustomMySQLConverterClass).
    """

    __metaclass__ = Singleton

    def __init__(self, charset=None, use_unicode=True):
        MySQLConverterBase.__init__(self, charset, use_unicode)
        self._cache_field_types = {}

    def escape(self, value):
        """
        Escapes special characters as they are expected to by when MySQL
        receives them.
        As found in MySQL source mysys/charset.c
        Returns the value if not a string, or the escaped string.
        """
        if value is None:
            return value
        elif isinstance(value, NUMERIC_TYPES):
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.replace(b'\\', b'\\\\')
            value = value.replace(b'\n', b'\\n')
            value = value.replace(b'\r', b'\\r')
            value = value.replace(b'\047', b'\134\047')  # single quotes
            value = value.replace(b'\042', b'\134\042')  # double quotes
            value = value.replace(b'\032', b'\134\032')  # for Win32
        else:
            value = value.replace('\\', '\\\\')
            value = value.replace('\n', '\\n')
            value = value.replace('\r', '\\r')
            value = value.replace('\047', '\134\047')  # single quotes
            value = value.replace('\042', '\134\042')  # double quotes
            value = value.replace('\032', '\134\032')  # for Win32
        return value

    def quote(self, buf):
        """
        Quote the parameters for commands. General rules:
          o numbers are returns as bytes using ascii codec
          o None is returned as bytearray(b'NULL')
          o Everything else is single quoted '<buf>'
        Returns a bytearray object.
        """
        if isinstance(buf, NUMERIC_TYPES):
            if PY2:
                if isinstance(buf, float):
                    return repr(buf)
                else:
                    return str(buf)
            else:
                return str(buf).encode('ascii')
        elif isinstance(buf, type(None)):
            return bytearray(b"NULL")
        else:
            return bytearray(b"'" + buf + b"'")

    def to_mysql(self, value):
        """Convert Python data type to MySQL"""
        type_name = value.__class__.__name__.lower()
        try:
            return getattr(self, "_{0}_to_mysql".format(type_name))(value)
        except AttributeError:
            raise TypeError("Python '{0}' cannot be converted to a "
                            "MySQL type".format(type_name))

    def to_python(self, vtype, value):
        """Convert MySQL data type to Python"""
        if value == 0 and vtype[1] != FieldType.BIT:  # \x00
            # Don't go further when we hit a NULL value
            return None
        if value is None:
            return None

        if not self._cache_field_types:
            self._cache_field_types = {}
            for name, info in FieldType.desc.items():
                try:
                    self._cache_field_types[info[0]] = getattr(
                        self, '_{0}_to_python'.format(name))
                except AttributeError:
                    # We ignore field types which has no method
                    pass

        try:
            return self._cache_field_types[vtype[1]](value, vtype)
        except KeyError:
            # If one type is not defined, we just return the value as str
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError:
                return value
        except ValueError as err:
            raise ValueError("%s (field %s)" % (err, vtype[0]))
        except TypeError as err:
            raise TypeError("%s (field %s)" % (err, vtype[0]))
        except:
            raise

    def _int_to_mysql(self, value):
        """Convert value to int"""
        return int(value)

    def _long_to_mysql(self, value):
        """Convert value to int"""
        return int(value)

    def _float_to_mysql(self, value):
        """Convert value to float"""
        return float(value)

    def _str_to_mysql(self, value):
        """Convert value to string"""
        if PY2:
            return str(value)
        return self._unicode_to_mysql(value)


    def _bytes_to_mysql(self, value):
        """Convert value to bytes"""
        return value

    def _bytearray_to_mysql(self, value):
        """Convert value to bytes"""
        return bytes(value)

    def _bool_to_mysql(self, value):
        """Convert value to boolean"""
        if value:
            return 1
        else:
            return 0

    def _nonetype_to_mysql(self, value):
        """
        This would return what None would be in MySQL, but instead we
        leave it None and return it right away. The actual conversion
        from None to NULL happens in the quoting functionality.
        Return None.
        """
        return None

    def _datetime_to_mysql(self, value):
        """
        Converts a datetime instance to a string suitable for MySQL.
        The returned string has format: %Y-%m-%d %H:%M:%S[.%f]
        If the instance isn't a datetime.datetime type, it return None.
        Returns a bytes.
        """
        if value.microsecond:
            fmt = '{0:d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}.{6:06d}'
            return fmt.format(
                value.year, value.month, value.day,
                value.hour, value.minute, value.second,
                value.microsecond).encode('ascii')

        fmt = '{0:d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}'
        return fmt.format(
            value.year, value.month, value.day,
            value.hour, value.minute, value.second).encode('ascii')

    def _date_to_mysql(self, value):
        """
        Converts a date instance to a string suitable for MySQL.
        The returned string has format: %Y-%m-%d
        If the instance isn't a datetime.date type, it return None.
        Returns a bytes.
        """
        return '{0:d}-{1:02d}-{2:02d}'.format(value.year, value.month,
                                              value.day).encode('ascii')

    def _time_to_mysql(self, value):
        """
        Converts a time instance to a string suitable for MySQL.
        The returned string has format: %H:%M:%S[.%f]
        If the instance isn't a datetime.time type, it return None.
        Returns a bytes.
        """
        if value.microsecond:
            return value.strftime('%H:%M:%S.%f').encode('ascii')
        return value.strftime('%H:%M:%S').encode('ascii')

    def _struct_time_to_mysql(self, value):
        """
        Converts a time.struct_time sequence to a string suitable
        for MySQL.
        The returned string has format: %Y-%m-%d %H:%M:%S
        Returns a bytes or None when not valid.
        """
        return time.strftime('%Y-%m-%d %H:%M:%S', value).encode('ascii')

    def _timedelta_to_mysql(self, value):
        """
        Converts a timedelta instance to a string suitable for MySQL.
        The returned string has format: %H:%M:%S
        Returns a bytes.
        """
        seconds = abs(value.days * 86400 + value.seconds)

        if value.microseconds:
            fmt = '{0:02d}:{1:02d}:{2:02d}.{3:06d}'
            if value.days < 0:
                mcs = 1000000 - value.microseconds
                seconds -= 1
            else:
                mcs = value.microseconds
        else:
            fmt = '{0:02d}:{1:02d}:{2:02d}'

        if value.days < 0:
            fmt = '-' + fmt

        (hours, remainder) = divmod(seconds, 3600)
        (mins, secs) = divmod(remainder, 60)

        if value.microseconds:
            result = fmt.format(hours, mins, secs, mcs)
        else:
            result = fmt.format(hours, mins, secs)

        if PY2:
            return result
        else:
            return result.encode('ascii')

    def _decimal_to_mysql(self, value):
        """
        Converts a decimal.Decimal instance to a string suitable for
        MySQL.
        Returns a bytes or None when not valid.
        """
        if isinstance(value, Decimal):
            return str(value).encode('ascii')

        return None

    def row_to_python(self, row, fields):
        """Convert a MySQL text result row to Python types
        The row argument is a sequence containing text result returned
        by a MySQL server. Each value of the row is converted to the
        using the field type information in the fields argument.
        Returns a tuple.
        """
        i = 0
        result = [None]*len(fields)

        if not self._cache_field_types:
            self._cache_field_types = {}
            for name, info in FieldType.desc.items():
                try:
                    self._cache_field_types[info[0]] = getattr(
                        self, '_{0}_to_python'.format(name))
                except AttributeError:
                    # We ignore field types which has no method
                    pass

        for field in fields:
            field_type = field[1]

            if (row[i] == 0 and field_type != FieldType.BIT) or row[i] is None:
                # Don't convert NULL value
                i += 1
                continue

            try:
                result[i] = self._cache_field_types[field_type](row[i], field)
            except KeyError:
                # If one type is not defined, we just return the value as str
                try:
                    result[i] = row[i].decode('utf-8')
                except UnicodeDecodeError:
                    result[i] = row[i]
            except (ValueError, TypeError) as err:
                err.message = "{0} (field {1})".format(str(err), field[0])
                raise

            i += 1

        return tuple(result)

mysql_converter = MySQLConverter()