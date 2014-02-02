import unittest
import os
import time
import pg8000
import datetime
from .connection_settings import db_connect
from sys import exc_info
from pg8000.six import b, IS_JYTHON


# DBAPI compatible interface tests
class Tests(unittest.TestCase):
    def setUp(self):
        self.db = pg8000.connect(**db_connect)
        # Jython 2.5.3 doesn't have a time.tzset() so skip
        if not IS_JYTHON:
            os.environ['TZ'] = "UTC"
            time.tzset()

        try:
            self.db.execute("DROP TABLE t1")
        except pg8000.DatabaseError:
            e = exc_info()[1]
            # the only acceptable error is:
            self.assertEqual(e.args[1], b('42P01'))  # table does not exist
            self.db.rollback()
        self.db.execute(
            "CREATE TEMPORARY TABLE t1 "
            "(f1 int primary key, f2 int not null, f3 varchar(50) null)")
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
            (1, 1, None))
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
            (2, 10, None))
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
            (3, 100, None))
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
            (4, 1000, None))
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
            (5, 10000, None))
        self.db.commit()

    def tearDown(self):
        self.db.close()

    def testParallelQueries(self):
        ps1 = self.db.execute("SELECT f1, f2, f3 FROM t1")
        while 1:
            row = ps1.fetchone()
            if row is None:
                break
            f1, f2, f3 = row
            ps2 = self.db.execute(
                "SELECT f1, f2, f3 FROM t1 WHERE f1 > :1", (f1,))
            while 1:
                row = ps2.fetchone()
                if row is None:
                    break
                f1, f2, f3 = row

        self.db.rollback()

    def testNumeric(self):
        ps = self.db.execute("SELECT f1, f2, f3 FROM t1 WHERE f1 > :1", (3,))
        while 1:
            row = ps.fetchone()
            if row is None:
                break
            f1, f2, f3 = row
        self.db.rollback()

    def testNamed(self):
        ps = self.db.execute(
            "SELECT f1, f2, f3 FROM t1 WHERE f1 > :f1", {"f1": 3})
        while 1:
            row = ps.fetchone()
            if row is None:
                break
            f1, f2, f3 = row
        self.db.rollback()

    def testDate(self):
        val = pg8000.Date(2001, 2, 3)
        self.assertEqual(val, datetime.date(2001, 2, 3))

    def testTime(self):
        val = pg8000.Time(4, 5, 6)
        self.assertEqual(val, datetime.time(4, 5, 6))

    def testTimestamp(self):
        val = pg8000.Timestamp(2001, 2, 3, 4, 5, 6)
        self.assertEqual(val, datetime.datetime(2001, 2, 3, 4, 5, 6))

    def testDateFromTicks(self):
        if IS_JYTHON:
            return

        val = pg8000.DateFromTicks(1173804319)
        self.assertEqual(val, datetime.date(2007, 3, 13))

    def testTimeFromTicks(self):
        if IS_JYTHON:
            return

        val = pg8000.TimeFromTicks(1173804319)
        self.assertEqual(val, datetime.time(16, 45, 19))

    def testTimestampFromTicks(self):
        if IS_JYTHON:
            return

        val = pg8000.TimestampFromTicks(1173804319)
        self.assertEqual(val, datetime.datetime(2007, 3, 13, 16, 45, 19))

    def testBinary(self):
        v = pg8000.Binary(b("\x00\x01\x02\x03\x02\x01\x00"))
        self.assertEqual(v, b("\x00\x01\x02\x03\x02\x01\x00"))
        self.assertTrue(isinstance(v, pg8000.BINARY))

    def testRowCount(self):
        ps = self.db.execute("SELECT * FROM t1")

        # In PostgreSQL 8.4 we don't know the row count for a select
        if not self.db._server_version.startswith("8.4"):
            self.assertEqual(5, ps.rowcount)

        ps = self.db.execute("UPDATE t1 SET f3 = :1 WHERE f2 > 101", "Hello!")
        self.assertEquals(2, ps.rowcount)

        ps = self.db.execute("DELETE FROM t1")
        self.assertEqual(5, ps.rowcount)
        self.db.commit()

    def testFetchMany(self):
        ps = self.db.execute("SELECT * FROM t1")
        self.assertEqual(2, len(ps.fetchmany(2)))
        self.assertEqual(2, len(ps.fetchmany(2)))
        self.assertEqual(1, len(ps.fetchmany(2)))
        self.assertEqual(0, len(ps.fetchmany(2)))
        self.db.commit()

    def testIterator(self):
        from warnings import filterwarnings
        filterwarnings("ignore", "DB-API extension cursor.next()")
        filterwarnings("ignore", "DB-API extension cursor.__iter__()")

        ps = self.db.execute("SELECT * FROM t1 ORDER BY f1")
        f1 = 0
        for row in ps:
            next_f1 = row[0]
            assert next_f1 > f1
            f1 = next_f1

        self.db.commit()

    # Vacuum can't be run inside a transaction, so we need to turn
    # autocommit on.
    def testVacuum(self):
        self.db.autocommit = True
        self.db.execute("vacuum")

if __name__ == "__main__":
    unittest.main()
