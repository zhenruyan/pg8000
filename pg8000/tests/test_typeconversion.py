import unittest
import pg8000
import datetime
import decimal
import struct
from .connection_settings import db_connect
from pg8000.six import b, IS_JYTHON, text_type
import uuid


if not IS_JYTHON:
    import pytz


# Type conversion tests
class Tests(unittest.TestCase):
    def setUp(self):
        self.db = pg8000.connect(**db_connect)

    def tearDown(self):
        self.db.close()

    def testTimeRoundtrip(self):
        ps = self.db.execute("SELECT :1 as f1", (datetime.time(4, 5, 6),))
        retval = ps.fetchall()
        self.assertEqual(retval[0][0], datetime.time(4, 5, 6))

    def testDateRoundtrip(self):
        ps = self.db.execute("SELECT :1 as f1", (datetime.date(2001, 2, 3),))
        retval = ps.fetchall()
        self.assertEqual(retval[0][0], datetime.date(2001, 2, 3))

    def testBoolRoundtrip(self):
        ps = self.db.execute("SELECT :1 as f1", (True,))
        retval = ps.fetchall()
        self.assertEqual(retval[0][0], True)

    def testNullRoundtrip(self):
        # We can't just "SELECT %s" and set None as the parameter, since it has
        # no type.  That would result in a PG error, "could not determine data
        # type of parameter %s".  So we create a temporary table, insert null
        # values, and read them back.
        self.db.execute(
            "CREATE TEMPORARY TABLE TestNullWrite "
            "(f1 int4, f2 timestamp, f3 varchar)")
        self.db.execute(
            "INSERT INTO TestNullWrite VALUES (:1, :2, :3)",
            (None, None, None,))
        ps = self.db.execute("SELECT * FROM TestNullWrite")
        retval = ps.fetchone()
        self.assertEqual(retval, [None, None, None])

    def testNullSelectFailure(self):
        # See comment in TestNullRoundtrip.  This test is here to ensure that
        # this behaviour is documented and doesn't mysteriously change.
        self.assertRaises(
            pg8000.errors.ProgrammingError, self.db.execute,
            "SELECT :1 as f1", (None,))
        self.db.rollback()

    def testDecimalRoundtrip(self):
        values = (
            "1.1", "-1.1", "10000", "20000", "-1000000000.123456789", "1.0",
            "12.44")
        for v in values:
            ps = self.db.execute("SELECT :1 as f1", (decimal.Decimal(v),))
            retval = ps.fetchall()
            self.assertEqual(str(retval[0][0]), v)

    def testFloatRoundtrip(self):
        # This test ensures that the binary float value doesn't change in a
        # roundtrip to the server.  That could happen if the value was
        # converted to text and got rounded by a decimal place somewhere.
        val = 1.756e-12
        bin_orig = struct.pack("!d", val)
        ps = self.db.execute("SELECT :1 as f1", (val,))
        retval = ps.fetchall()
        bin_new = struct.pack("!d", retval[0][0])
        self.assertEqual(bin_new, bin_orig)

    def testStrRoundtrip(self):
        v = "hello world"
        self.db.execute(
            "create temporary table test_str (f character varying(255))")
        self.db.execute("INSERT INTO test_str VALUES (:1)", (v,))
        ps = self.db.execute("SELECT * from test_str")
        retval = ps.fetchall()
        self.assertEqual(retval[0][0], v)

    def testUnicodeRoundtrip(self):
        ps = self.db.execute(
            "SELECT cast(:1 as character varying) as f1",
            ("hello \u0173 world",))
        retval = ps.fetchall()
        self.assertEqual(retval[0][0], "hello \u0173 world")

        v = text_type("hello \u0173 world")
        ps = self.db.execute("SELECT cast(:1 as varchar) as f1", (v,))
        retval = ps.fetchall()
        self.assertEqual(retval[0][0], v)

    def testLongRoundtrip(self):
        ps = self.db.execute("SELECT :1 as f1", (50000000000000,))
        retval = ps.fetchall()
        self.assertEqual(retval[0][0], 50000000000000)

    def testIntRoundtrip(self):
        int2 = 21
        int4 = 23
        int8 = 20
        #numeric = 1700
        test_values = [
            (0, int2),
            (-32767, int2),
            (-32768, int4),
            (+32767, int2),
            (+32768, int4),
            (-2147483647, int4),
            (-2147483648, int8),
            (+2147483647, int4),
            (+2147483648, int8),
            (-9223372036854775807, int8),
            (+9223372036854775807, int8), ]
            #(-9223372036854775808, numeric),
            #(+9223372036854775808, numeric),
        for value, typoid in test_values:
            ps = self.db.execute("SELECT :1 as f1", (value,))
            retval = ps.fetchall()
            self.assertEqual(retval[0][0], value)
            column_name, column_typeoid = ps.description[0][0:2]
            self.assertEqual(column_typeoid, typoid, "type should be INT2[]")

    def testByteaRoundtrip(self):
        ps = self.db.execute(
            "SELECT :1 as f1",
            (pg8000.Binary(b("\x00\x01\x02\x03\x02\x01\x00")),))
        self.assertEqual(
            ps.fetchall()[0][0], b("\x00\x01\x02\x03\x02\x01\x00"))

    def testTimestampRoundtrip(self):
        v = datetime.datetime(2001, 2, 3, 4, 5, 6, 170000)
        ps = self.db.execute("SELECT :1 as f1", (v,))
        self.assertEqual(ps.fetchall()[0][0], v)

    def testIntervalRoundtrip(self):
        v = pg8000.Interval(microseconds=123456789, days=2, months=24)
        ps = self.db.execute("SELECT :1 as f1", (v,))
        self.assertEqual(ps.fetchall()[0][0], v)

    def testEnumRoundtrip(self):
        try:
            ps = self.db.execute(
                "create type lepton as enum ('electron', 'muon', 'tau')")
        except pg8000.ProgrammingError:
            self.db.rollback()

        v = 'muon'
        ps = self.db.execute("SELECT cast(:1 as lepton) as f1", (v,))
        self.assertEqual(ps.fetchall()[0][0], v)
        ps = self.db.execute(
            "CREATE TEMPORARY TABLE testenum "
            "(f1 lepton)")
        self.db.execute("INSERT INTO testenum VALUES (:1)", ('electron',))
        self.db.execute("drop table testenum")
        self.db.execute("drop type lepton")
        self.db.commit()

    def testXmlRoundtrip(self):
        v = '<genome>gatccgagtac</genome>'
        ps = self.db.execute("select xmlparse(content :1) as f1", (v,))
        self.assertEqual(ps.fetchall()[0][0], v)

    def testUuidRoundtrip(self):
        v = uuid.UUID('911460f2-1f43-fea2-3e2c-e01fd5b5069d')
        ps = self.db.execute("select :1 as f1", (v,))
        self.assertEqual(ps.fetchall()[0][0], v)

    def testTimestampTzOut(self):
        ps = self.db.execute(
            "SELECT cast('2001-02-03 04:05:06.17 America/Edmonton'"
            " as timestamp with time zone)")
        dt = ps.fetchall()[0][0]
        self.assertEqual(dt.tzinfo is not None, True, "no tzinfo returned")
        self.assertEqual(
            dt.astimezone(pg8000.utc),
            datetime.datetime(2001, 2, 3, 11, 5, 6, 170000, pg8000.utc),
            "retrieved value match failed")

    def testTimestampTzRoundtrip(self):
        if not IS_JYTHON:
            mst = pytz.timezone("America/Edmonton")
            v1 = mst.localize(datetime.datetime(2001, 2, 3, 4, 5, 6, 170000))
            ps = self.db.execute("SELECT :1 as f1", (v1,))
            v2 = ps.fetchall()[0][0]
            self.assertNotEqual(v2.tzinfo, None)
            self.assertEqual(v1, v2)

    def testTimestampMismatch(self):
        if not IS_JYTHON:
            mst = pytz.timezone("America/Edmonton")
            self.db.execute("SET SESSION TIME ZONE 'America/Edmonton'")
            try:
                self.db.execute(
                    "CREATE TEMPORARY TABLE TestTz "
                    "(f1 timestamp with time zone, "
                    "f2 timestamp without time zone)")
                self.db.execute(
                    "INSERT INTO TestTz (f1, f2) VALUES (:1, :2)", (
                        # insert timestamp into timestamptz field (v1)
                        datetime.datetime(2001, 2, 3, 4, 5, 6, 170000),
                        # insert timestamptz into timestamp field (v2)
                        mst.localize(datetime.datetime(
                            2001, 2, 3, 4, 5, 6, 170000))))
                ps = self.db.execute("SELECT f1, f2 FROM TestTz")

                # when inserting a timestamp into a timestamptz field,
                # postgresql assumes that it is in local time. So the value
                # that comes out will be the server's local time interpretation
                # of v1. We've set the server's TZ to MST, the time should
                # be...
                retval = ps.fetchall()
                f1 = retval[0][0]
                self.assertEqual(
                    f1, datetime.datetime(
                        2001, 2, 3, 11, 5, 6, 170000, pytz.utc))

                # inserting the timestamptz into a timestamp field, pg8000
                # converts the value into UTC, and then the PG server converts
                # it into local time for insertion into the field. When we
                # query for it, we get the same time back, like the tz was
                # dropped.
                f2 = retval[0][1]
                self.assertEqual(
                    f2, datetime.datetime(2001, 2, 3, 4, 5, 6, 170000))
            finally:
                self.db.execute("SET SESSION TIME ZONE DEFAULT")

    def testNameOut(self):
        # select a field that is of "name" type:
        ps = self.db.execute("SELECT usename FROM pg_user")
        ps.fetchall()
        # It is sufficient that no errors were encountered.

    def testOidOut(self):
        ps = self.db.execute("SELECT oid FROM pg_type")
        ps.fetchall()
        # It is sufficient that no errors were encountered.

    def testBooleanOut(self):
        ps = self.db.execute("SELECT cast('t' as bool)")
        self.assertTrue(ps.fetchall()[0][0])

    def testNumericOut(self):
        for num in ('5000', '50.34'):
            ps = self.db.execute("SELECT cast(" + num + " as numeric)")
            self.assertEqual(str(ps.fetchall()[0][0]), num)

    def testInt2Out(self):
        ps = self.db.execute("SELECT cast(5000 as smallint)")
        self.assertEqual(ps.fetchall()[0][0], 5000)

    def testInt4Out(self):
        ps = self.db.execute("SELECT cast(5000 as integer)")
        self.assertEqual(ps.fetchall()[0][0], 5000)

    def testInt8Out(self):
        ps = self.db.execute("SELECT cast(50000000000000 as bigint)")
        self.assertEqual(ps.fetchall()[0][0], 50000000000000)

    def testFloat4Out(self):
        ps = self.db.execute("SELECT cast(1.1 as real)")
        self.assertEqual(ps.fetchall()[0][0], 1.1000000238418579)

    def testFloat8Out(self):
        ps = self.db.execute("SELECT cast(1.1 as double precision)")
        self.assertEqual(ps.fetchall()[0][0], 1.1000000000000001)

    def testVarcharOut(self):
        ps = self.db.execute("SELECT cast('hello' as varchar(20))")
        self.assertEqual(ps.fetchall()[0][0], "hello")

    def testCharOut(self):
        ps = self.db.execute("SELECT cast('hello' as char(20))")
        self.assertEqual(ps.fetchall()[0][0], "hello               ")

    def testTextOut(self):
        ps = self.db.execute("SELECT cast('hello' as text)")
        self.assertEqual(ps.fetchall()[0][0], "hello")

    def testIntervalOut(self):
        ps = self.db.execute(
            "SELECT cast('1 month 16 days 12 hours 32 minutes 64 seconds'"
            " as interval)")
        expected_value = pg8000.Interval(
            microseconds=(12 * 60 * 60 * 1000 * 1000) +
            (32 * 60 * 1000 * 1000) + (64 * 1000 * 1000),
            days=16, months=1)
        self.assertEqual(ps.fetchall()[0][0], expected_value)

    def testTimestampOut(self):
        ps = self.db.execute(
            "SELECT cast('2001-02-03 04:05:06.17' as timestamp)")
        self.assertEqual(
            ps.fetchall()[0][0],
            datetime.datetime(2001, 2, 3, 4, 5, 6, 170000))

    # confirms that pg8000's binary output methods have the same output for
    # a data type as the PG server
    def testBinaryOutputMethods(self):
        methods = (
            ("float8send", 22.2),
            ("timestamp_send", datetime.datetime(2001, 2, 3, 4, 5, 6, 789)),
            ("byteasend", pg8000.Binary(b("\x01\x02"))),
            ("interval_send", pg8000.Interval(1234567, 123, 123)),)
        for method_out, value in methods:
            ps = self.db.execute("SELECT %s(:1) as f1" % method_out, (value,))
            self.assertEqual(
                ps.fetchall()[0][0],
                self.db.make_params((value,))[0][2](value))

    def testInt4ArrayOut(self):
        ps = self.db.execute(
            "SELECT cast('{1,2,3,4}' as INT[]) AS f1, "
            "cast('{{1,2,3},{4,5,6}}' as INT[][]) AS f2, "
            "cast('{{{1,2},{3,4}},{{NULL,6},{7,8}}}' as INT[][][]) AS f3")
        f1, f2, f3 = ps.fetchone()
        self.assertEqual(f1, [1, 2, 3, 4])
        self.assertEqual(f2, [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(f3, [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testInt2ArrayOut(self):
        ps = self.db.execute(
            "SELECT cast('{1,2,3,4}' as INT2[]) AS f1, "
            "cast('{{1,2,3},{4,5,6}}' as INT2[][]) AS f2, "
            "cast('{{{1,2},{3,4}},{{NULL,6},{7,8}}}' as INT2[][][]) AS f3")
        f1, f2, f3 = ps.fetchone()
        self.assertEqual(f1, [1, 2, 3, 4])
        self.assertEqual(f2, [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(f3, [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testInt8ArrayOut(self):
        ps = self.db.execute(
            "SELECT cast('{1,2,3,4}' as INT8[]) AS f1, "
            "cast('{{1,2,3},{4,5,6}}' as INT8[][]) AS f2, "
            "cast('{{{1,2},{3,4}},{{NULL,6},{7,8}}}' as INT8[][][]) AS f3")
        f1, f2, f3 = ps.fetchone()
        self.assertEqual(f1, [1, 2, 3, 4])
        self.assertEqual(f2, [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(f3, [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testBoolArrayOut(self):
        ps = self.db.execute(
            "SELECT cast('{TRUE,FALSE,FALSE,TRUE}' as BOOL[]) AS f1, "
            "cast('{{TRUE,FALSE,TRUE},{FALSE,TRUE,FALSE}}' as BOOL[][]) AS f2,"
            "cast('{{{TRUE,FALSE},{FALSE,TRUE}},{{NULL,TRUE},{FALSE,FALSE}}}'"
            "as BOOL[][][]) AS f3")
        f1, f2, f3 = ps.fetchone()
        self.assertEqual(f1, [True, False, False, True])
        self.assertEqual(f2, [[True, False, True], [False, True, False]])
        self.assertEqual(
            f3,
            [[[True, False], [False, True]], [[None, True], [False, False]]])

    def testFloat4ArrayOut(self):
        ps = self.db.execute(
            "SELECT cast('{1,2,3,4}' as FLOAT4[]) AS f1, "
            "cast('{{1,2,3},{4,5,6}}' as FLOAT4[][]) AS f2, "
            "cast('{{{1,2},{3,4}},{{NULL,6},{7,8}}}' as FLOAT4[][][]) AS f3")
        f1, f2, f3 = ps.fetchone()
        self.assertEqual(f1, [1, 2, 3, 4])
        self.assertEqual(f2, [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(f3, [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testFloat8ArrayOut(self):
        ps = self.db.execute(
            "SELECT cast('{1,2,3,4}' as FLOAT8[]) AS f1, "
            "cast('{{1,2,3},{4,5,6}}' as FLOAT8[][]) AS f2, "
            "cast('{{{1,2},{3,4}},{{NULL,6},{7,8}}}' as FLOAT8[][][]) AS f3")
        f1, f2, f3 = ps.fetchone()
        self.assertEqual(f1, [1, 2, 3, 4])
        self.assertEqual(f2, [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(f3, [[[1, 2], [3, 4]], [[None, 6], [7, 8]]])

    def testIntArrayRoundtrip(self):
        # send small int array, should be sent as INT2[]
        ps = self.db.execute("SELECT :1 as f1", ([1, 2, 3],))
        self.assertEqual(ps.fetchall()[0][0], [1, 2, 3])
        column_name, column_typeoid = ps.description[0][0:2]
        self.assertEqual(column_typeoid, 1005, "type should be INT2[]")

        # test multi-dimensional array, should be sent as INT2[]
        ps = self.db.execute("SELECT :1 as f1", ([[1, 2], [3, 4]],))
        self.assertEqual(ps.fetchall()[0][0], [[1, 2], [3, 4]])

        column_name, column_typeoid = ps.description[0][0:2]
        self.assertEqual(column_typeoid, 1005, "type should be INT2[]")

        # a larger value should kick it up to INT4[]...
        ps = self.db.execute("SELECT :1 as f1", ([70000, 2, 3],))
        self.assertEqual(ps.fetchall()[0][0], [70000, 2, 3])
        column_name, column_typeoid = ps.description[0][0:2]
        self.assertEqual(column_typeoid, 1007, "type should be INT4[]")

        # a much larger value should kick it up to INT8[]...
        ps = self.db.execute("SELECT :1 as f1", ([7000000000, 2, 3],))
        self.assertEqual(
            ps.fetchall()[0][0], [7000000000, 2, 3],
            "retrieved value match failed")
        column_name, column_typeoid = ps.description[0][0:2]
        self.assertEqual(column_typeoid, 1016, "type should be INT8[]")

    def testIntArrayWithNullRoundtrip(self):
        ps = self.db.execute("SELECT :1 as f1", ([1, None, 3],))
        self.assertEqual(ps.fetchall()[0][0], [1, None, 3])

    def testFloatArrayRoundtrip(self):
        ps = self.db.execute("SELECT :1 as f1", ([1.1, 2.2, 3.3],))
        self.assertEqual(ps.fetchall()[0][0], [1.1, 2.2, 3.3])

    def testBoolArrayRoundtrip(self):
        ps = self.db.execute("SELECT :1 as f1", ([True, False, None],))
        self.assertEqual(ps.fetchall()[0][0], [True, False, None])

    def testStringArrayOut(self):
        ps = self.db.execute("SELECT cast('{a,b,c}' as TEXT[]) AS f1")
        self.assertEqual(ps.fetchone()[0], ["a", "b", "c"])
        ps = self.db.execute("SELECT cast('{a,b,c}' as CHAR[]) AS f1")
        self.assertEqual(ps.fetchone()[0], ["a", "b", "c"])
        ps = self.db.execute("SELECT cast('{a,b,c}' as VARCHAR[]) AS f1")
        self.assertEqual(ps.fetchone()[0], ["a", "b", "c"])
        ps = self.db.execute("SELECT cast('{a,b,c}' as CSTRING[]) AS f1")
        self.assertEqual(ps.fetchone()[0], ["a", "b", "c"])
        ps = self.db.execute("SELECT cast('{a,b,c}' as NAME[]) AS f1")
        self.assertEqual(ps.fetchone()[0], ["a", "b", "c"])
        ps = self.db.execute("SELECT cast('{}' as text[]);")
        self.assertEqual(ps.fetchone()[0], [])

    def testNumericArrayOut(self):
        ps = self.db.execute("SELECT cast('{1.1,2.2,3.3}' as numeric[]) AS f1")
        self.assertEqual(
            ps.fetchone()[0], [
                decimal.Decimal("1.1"), decimal.Decimal("2.2"),
                decimal.Decimal("3.3")])

    def testNumericArrayRoundtrip(self):
        v = [decimal.Decimal("1.1"), None, decimal.Decimal("3.3")]
        ps = self.db.execute("SELECT :1 as f1", (v,))
        self.assertEqual(ps.fetchall()[0][0], v)

    def testStringArrayRoundtrip(self):
        ps = self.db.execute("SELECT :1 as f1", (["Hello!", "World!", None],))
        self.assertEqual(ps.fetchall()[0][0], ["Hello!", "World!", None])

        ps = self.db.execute("SELECT :1 as f1", (["Hello!", "World!", None],))
        self.assertEqual(ps.fetchall()[0][0], ["Hello!", "World!", None])

    def testArrayHasValue(self):
        self.assertRaises(
            pg8000.ArrayContentEmptyError,
            self.db.array_inspect, [[None], [None], [None]])
        self.db.rollback()

    def testArrayContentNotSupported(self):
        class Kajigger(object):
            pass
        self.assertRaises(
            pg8000.ArrayContentNotSupportedError,
            self.db.array_inspect, [[Kajigger()], [None], [None]])
        self.db.rollback()

    def testArrayDimensions(self):
        for arr in (
                [1, [2]], [[1], [2], [3, 4]],
                [[[1]], [[2]], [[3, 4]]],
                [[[1]], [[2]], [[3, 4]]],
                [[[[1]]], [[[2]]], [[[3, 4]]]],
                [[1, 2, 3], [4, [5], 6]]):

            arr_send = self.db.array_inspect(arr)[2]
            self.assertRaises(
                pg8000.ArrayDimensionsNotConsistentError, arr_send, arr)
            self.db.rollback()

    def testArrayHomogenous(self):
        arr = [[[1]], [[2]], [[3.1]]]
        arr_send = self.db.array_inspect(arr)[2]
        self.assertRaises(
            pg8000.ArrayContentNotHomogenousError, arr_send, arr)
        self.db.rollback()

    def testArrayInspect(self):
        self.db.array_inspect([1, 2, 3])
        self.db.array_inspect([[1], [2], [3]])
        self.db.array_inspect([[[1]], [[2]], [[3]]])

    def testMacaddr(self):
        ps = self.db.execute("SELECT macaddr '08002b:010203'")
        self.assertEqual(ps.fetchall()[0][0], "08:00:2b:01:02:03")

if __name__ == "__main__":
    unittest.main()
