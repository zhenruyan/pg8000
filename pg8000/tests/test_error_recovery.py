import unittest
import pg8000
from .connection_settings import db_connect
import warnings
import datetime
from sys import exc_info
from pg8000.six import b


class TestException(Exception):
    pass


class Tests(unittest.TestCase):
    def setUp(self):
        self.db = pg8000.connect(**db_connect)

    def tearDown(self):
        self.db.close()

    def raiseException(self, value):
        raise TestException("oh noes!")

    def testPyValueFail(self):
        # Ensure that if types.py_value throws an exception, the original
        # exception is raised (TestException), and the connection is
        # still usable after the error.
        orig = self.db.py_types[datetime.time]
        self.db.py_types[datetime.time] = (
            orig[0], orig[1], self.raiseException)

        try:
            try:
                ps = self.db.execute(
                    "SELECT :1 as f1", (datetime.time(10, 30),))
                ps.fetchall()
                # shouldn't get here, exception should be thrown
                self.fail()
            except TestException:
                # should be TestException type, this is OK!
                self.db.rollback()
        finally:
            self.db.py_types[datetime.time] = orig

        # ensure that the connection is still usable for a new query
        ps = self.db.execute("VALUES (cast('hw3' as text))")
        self.assertEqual(ps.fetchone()[0], "hw3")

    def testNoDataErrorRecovery(self):
        for i in range(1, 4):
            try:
                self.db.execute("DROP TABLE t1")
            except pg8000.DatabaseError:
                e = exc_info()[1]
                # the only acceptable error is:
                self.assertEqual(e.args[1], b('42P01'))  # table does not exist
                self.db.rollback()

    def testClosedConnection(self):
        warnings.simplefilter("ignore")
        my_db = pg8000.connect(**db_connect)
        my_db.close()
        self.assertRaises(
            self.db.InterfaceError, my_db.execute,
            "VALUES (cast('hw1' as text)")
        warnings.resetwarnings()

if __name__ == "__main__":
    unittest.main()
