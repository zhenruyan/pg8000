#!/usr/bin/env python
''' Python DB API 2.0 driver compliance unit test suite.

    This software is Public Domain and may be used without restrictions.

 "Now we have booze and barflies entering the discussion, plus rumours of
  DBAs on drugs... and I won't tell you what flashes through my mind each
  time I read the subject line with 'Anal Compliance' in it.  All around
  this is turning out to be a thoroughly unwholesome unit test."

    -- Ian Bicking
'''

__rcs_id__ = '$Id: dbapi20.py,v 1.10 2003/10/09 03:14:14 zenzen Exp $'
__version__ = '$Revision: 1.10 $'[11:-2]
__author__ = 'Stuart Bishop <zen@shangri-la.dropbear.id.au>'

import unittest
import time
import warnings
from pg8000.six import b

# $Log: dbapi20.py,v $
# Revision 1.10  2003/10/09 03:14:14  zenzen
# Add test for DB API 2.0 optional extension, where database exceptions
# are exposed as attributes on the Connection object.
#
# Revision 1.9  2003/08/13 01:16:36  zenzen
# Minor tweak from Stefan Fleiter
#
# Revision 1.8  2003/04/10 00:13:25  zenzen
# Changes, as per suggestions by M.-A. Lemburg
# - Add a table prefix, to ensure namespace collisions can always be avoided
#
# Revision 1.7  2003/02/26 23:33:37  zenzen
# Break out DDL into helper functions, as per request by David Rushby
#
# Revision 1.6  2003/02/21 03:04:33  zenzen
# Stuff from Henrik Ekelund:
#     added test_None
#     added test_nextset & hooks
#
# Revision 1.5  2003/02/17 22:08:43  zenzen
# Implement suggestions and code from Henrik Eklund - test that
# cursor.arraysize defaults to 1 & generic cursor.callproc test added
#
# Revision 1.4  2003/02/15 00:16:33  zenzen
# Changes, as per suggestions and bug reports by M.-A. Lemburg,
# Matthew T. Kromer, Federico Di Gregorio and Daniel Dittmar
# - Class renamed
# - Now a subclass of TestCase, to avoid requiring the driver stub
#   to use multiple inheritance
# - Reversed the polarity of buggy test in test_description
# - Test exception heirarchy correctly
# - self.populate is now self._populate(), so if a driver stub
#   overrides self.ddl1 this change propogates
# - VARCHAR columns now have a width, which will hopefully make the
#   DDL even more portible (this will be reversed if it causes more problems)
# - cursor.rowcount being checked after various execute and fetchXXX methods
# - Check for fetchall and fetchmany returning empty lists after results
#   are exhausted (already checking for empty lists if select retrieved
#   nothing
# - Fix bugs in test_setoutputsize_basic and test_setinputsizes
#


class DatabaseAPI20Test(unittest.TestCase):
    ''' Test a database self.driver for DB API 2.0 compatibility.
        This implementation tests Gadfly, but the TestCase
        is structured so that other self.drivers can subclass this
        test case to ensure compiliance with the DB-API. It is
        expected that this TestCase may be expanded in the future
        if ambiguities or edge conditions are discovered.

        The 'Optional Extensions' are not yet being tested.

        self.drivers should subclass this test, overriding setUp, tearDown,
        self.driver, connect_args and connect_kw_args. Class specification
        should be as follows:

        import dbapi20
        class mytest(dbapi20.DatabaseAPI20Test):
           [...]

        Don't 'import DatabaseAPI20Test from dbapi20', or you will
        confuse the unit tester - just 'import dbapi20'.
    '''

    # The self.driver module. This should be the module where the 'connect'
    # method is to be found
    driver = None
    connect_args = ()  # List of arguments to pass to connect
    connect_kw_args = {}  # Keyword arguments for connect
    table_prefix = 'dbapi20test_'  # If you need to specify a prefix for tables

    ddl1 = 'create table %sbooze (name varchar(20))' % table_prefix
    ddl2 = 'create table %sbarflys (name varchar(20))' % table_prefix
    xddl1 = 'drop table %sbooze' % table_prefix
    xddl2 = 'drop table %sbarflys' % table_prefix

    lowerfunc = 'lower'  # Name of stored procedure to convert
                         # string->lowercase

    # Some drivers may need to override these helpers, for example adding
    # a 'commit' after the execute.
    def executeDDL1(self, con):
        return con.execute(self.ddl1)

    def executeDDL2(self, con):
        return con.execute(self.ddl2)

    def setUp(self):
        ''' self.drivers should override this method to perform required setup
            if any is necessary, such as creating the database.
        '''
        pass

    def tearDown(self):
        ''' self.drivers should override this method to perform required
            cleanup if any is necessary, such as deleting the test database.
            The default drops the tables that may be created.
        '''
        con = self._connect()
        try:
            for ddl in (self.xddl1, self.xddl2):
                try:
                    con.execute(ddl)
                    con.commit()
                except self.driver.Error:
                    # Assume table didn't exist. Other tests will check if
                    # execute is busted.
                    pass
        finally:
            con.close()

    def _connect(self):
        try:
            return self.driver.connect(
                *self.connect_args, **self.connect_kw_args)
        except AttributeError:
            self.fail("No connect method found in self.driver module")

    def test_connect(self):
        con = self._connect()
        con.close()

    def test_apilevel(self):
        try:
            # Must exist
            apilevel = self.driver.apilevel
            # Must equal 2.0
            self.assertEqual(apilevel, '3.0')
        except AttributeError:
            self.fail("Driver doesn't define apilevel")

    def test_Exceptions(self):
        # Make sure required exceptions exist, and are in the
        # defined heirarchy.
        self.assertEqual(issubclass(self.driver.Warning, Exception), True)
        self.assertEqual(issubclass(self.driver.Error, Exception), True)
        self.assertEqual(
            issubclass(self.driver.InterfaceError, self.driver.Error), True)
        self.assertEqual(
            issubclass(self.driver.DatabaseError, self.driver.Error), True)
        self.assertEqual(
            issubclass(self.driver.OperationalError, self.driver.Error), True)
        self.assertEqual(
            issubclass(self.driver.IntegrityError, self.driver.Error), True)
        self.assertEqual(
            issubclass(self.driver.InternalError, self.driver.Error), True)
        self.assertEqual(
            issubclass(self.driver.ProgrammingError, self.driver.Error), True)
        self.assertEqual(
            issubclass(self.driver.NotSupportedError, self.driver.Error), True)

    def test_ExceptionsAsConnectionAttributes(self):
        # OPTIONAL EXTENSION
        # Test for the optional DB API 2.0 extension, where the exceptions
        # are exposed as attributes on the Connection object
        # I figure this optional extension will be implemented by any
        # driver author who is using this test suite, so it is enabled
        # by default.
        warnings.simplefilter("ignore")
        con = self._connect()
        drv = self.driver
        self.assertEqual(con.Warning is drv.Warning, True)
        self.assertEqual(con.Error is drv.Error, True)
        self.assertEqual(con.InterfaceError is drv.InterfaceError, True)
        self.assertEqual(con.DatabaseError is drv.DatabaseError, True)
        self.assertEqual(con.OperationalError is drv.OperationalError, True)
        self.assertEqual(con.IntegrityError is drv.IntegrityError, True)
        self.assertEqual(con.InternalError is drv.InternalError, True)
        self.assertEqual(con.ProgrammingError is drv.ProgrammingError, True)
        self.assertEqual(con.NotSupportedError is drv.NotSupportedError, True)
        warnings.resetwarnings()
        con.close()

    def test_commit(self):
        con = self._connect()
        try:
            # Commit must work, even if it doesn't do anything
            con.commit()
        finally:
            con.close()

    def test_rollback(self):
        con = self._connect()
        # If rollback is defined, it should either work or throw
        # the documented exception
        if hasattr(con, 'rollback'):
            try:
                con.rollback()
            except self.driver.NotSupportedError:
                pass
        con.close()

    def test_cursor(self):
        con = self._connect()
        con.close()

    def test_cursor_isolation(self):
        con = self._connect()
        try:
            # Make sure cursors created from the same connection have
            # the documented transaction isolation level
            self.executeDDL1(con)
            con.execute(
                "insert into %sbooze values ('Victoria Bitter')" % (
                    self.table_prefix))
            ps2 = con.execute("select name from %sbooze" % self.table_prefix)
            booze = ps2.fetchall()
            self.assertEqual(len(booze), 1)
            self.assertEqual(len(booze[0]), 1)
            self.assertEqual(booze[0][0], 'Victoria Bitter')
        finally:
            con.close()

    def test_description(self):
        con = self._connect()
        try:
            ps = self.executeDDL1(con)
            self.assertEqual(
                ps.description, None,
                'cursor.description should be none after executing a '
                'statement that can return no rows (such as DDL)')
            ps = con.execute('select name from %sbooze' % self.table_prefix)
            self.assertEqual(
                len(ps.description), 1,
                'cursor.description describes too many columns')
            self.assertEqual(
                len(ps.description[0]), 7,
                'cursor.description[x] tuples must have 7 elements')
            self.assertEqual(
                ps.description[0][0].lower(), b('name'),
                'cursor.description[x][0] must return column name')
            self.assertEqual(
                ps.description[0][1], self.driver.STRING,
                'cursor.description[x][1] must return column type. Got %r'
                % ps.description[0][1])

            # Make sure self.description gets reset
            ps = self.executeDDL2(con)
            self.assertEqual(
                ps.description, None,
                'cursor.description not being set to None when executing '
                'no-result statements (eg. DDL)')
        finally:
            con.close()

    def test_rowcount(self):
        con = self._connect()
        try:
            ps = self.executeDDL1(con)
            self.assertEqual(
                ps.rowcount, -1,
                'ps.rowcount should be -1 after executing no-result '
                'statements')
            ps = con.execute(
                "insert into %sbooze values ('Victoria Bitter')" % (
                    self.table_prefix))
            self.assertEqual(
                ps.rowcount in (-1, 1), True,
                'ps.rowcount should == number or rows inserted, or '
                'set to -1 after executing an insert statement')
            ps = con.execute("select name from %sbooze" % self.table_prefix)
            self.assertEqual(
                ps.rowcount in (-1, 1), True,
                'ps.rowcount should == number of rows returned, or '
                'set to -1 after executing a select statement')
            ps = self.executeDDL2(con)
            self.assertEqual(
                ps.rowcount, -1,
                'ps.rowcount not being reset to -1 after executing '
                'no-result statements')
        finally:
            con.close()

    lower_func = 'lower'

    def test_callproc(self):
        con = self._connect()
        try:
            if self.lower_func and hasattr(con, 'callproc'):
                r = con.callproc(self.lower_func, ('FOO',))
                self.assertEqual(len(r), 1)
                self.assertEqual(r[0], 'FOO')
                r = r.fetchall()
                self.assertEqual(len(r), 1, 'callproc produced no result set')
                self.assertEqual(
                    len(r[0]), 1, 'callproc produced invalid result set')
                self.assertEqual(
                    r[0][0], 'foo', 'callproc produced invalid results')
        finally:
            con.close()

    def test_close(self):
        con = self._connect()
        con.close()

        # cursor.execute should raise an Error if called after connection
        # closed
        self.assertRaises(self.driver.Error, self.executeDDL1, con)

        # connection.commit should raise an Error if called after connection'
        # closed.'
        self.assertRaises(self.driver.Error, con.commit)

        # connection.close should raise an Error if called more than once
        self.assertRaises(self.driver.Error, con.close)

    def test_execute(self):
        con = self._connect()
        try:
            self._paraminsert(con)
        finally:
            con.close()

    def _paraminsert(self, con):
        self.executeDDL1(con)
        ps = con.execute("insert into %sbooze values ('Victoria Bitter')" % (
            self.table_prefix))
        self.assertEqual(ps.rowcount in (-1, 1), True)

        ps = con.execute(
            'insert into %sbooze values (:beer)' % self.table_prefix,
            {'beer': "Cooper's"})
        self.assertEqual(ps.rowcount in (-1, 1), True)

        ps = con.execute('select name from %sbooze' % self.table_prefix)
        res = ps.fetchall()
        self.assertEqual(
            len(res), 2, 'cursor.fetchall returned too few rows')
        beers = [res[0][0], res[1][0]]
        beers.sort()
        self.assertEqual(
            beers[0], "Cooper's",
            'cursor.fetchall retrieved incorrect data, or data inserted '
            'incorrectly')
        self.assertEqual(
            beers[1], "Victoria Bitter",
            'cursor.fetchall retrieved incorrect data, or data inserted '
            'incorrectly')

    def test_executemany(self):
        con = self._connect()
        try:
            self.executeDDL1(con)
            largs = [("Cooper's",), ("Boag's",)]
            ps = con.executemany(
                'insert into %sbooze values (:1)' % self.table_prefix, largs)
            self.assertEqual(
                ps.rowcount in (-1, 2), True,
                'insert using con.executemany set ps.rowcount to '
                'incorrect value %r' % ps.rowcount)
            ps = con.execute('select name from %sbooze' % self.table_prefix)
            res = ps.fetchall()
            self.assertEqual(
                len(res), 2,
                'cursor.fetchall retrieved incorrect number of rows')
            beers = [res[0][0], res[1][0]]
            beers.sort()
            self.assertEqual(beers[0], "Boag's", 'incorrect data retrieved')
            self.assertEqual(beers[1], "Cooper's", 'incorrect data retrieved')
        finally:
            con.close()

    def test_fetchone(self):
        con = self._connect()
        try:
            # cursor.fetchone should raise an Error if called after
            # executing a query that cannnot return rows
            ps = self.executeDDL1(con)
            self.assertRaises(self.driver.Error, ps.fetchone)

            ps = con.execute('select name from %sbooze' % self.table_prefix)
            self.assertEqual(
                ps.fetchone(), None,
                'cursor.fetchone should return None if a query retrieves '
                'no rows')
            self.assertEqual(ps.rowcount in (-1, 0), True)

            # cursor.fetchone should raise an Error if called after
            # executing a query that cannnot return rows
            ps = con.execute(
                "insert into %sbooze values ('Victoria Bitter')" % (
                    self.table_prefix))
            self.assertRaises(self.driver.Error, ps.fetchone)

            ps = con.execute('select name from %sbooze' % self.table_prefix)
            r = ps.fetchone()
            self.assertEqual(
                len(r), 1,
                'cursor.fetchone should have retrieved a single row')
            self.assertEqual(
                r[0], 'Victoria Bitter',
                'cursor.fetchone retrieved incorrect data')
            self.assertEqual(
                ps.fetchone(), None,
                'cursor.fetchone should return None if no more rows available')
            self.assertEqual(ps.rowcount in (-1, 1), True)
        finally:
            con.close()

    samples = [
        'Carlton Cold',
        'Carlton Draft',
        'Mountain Goat',
        'Redback',
        'Victoria Bitter',
        'XXXX'
        ]

    def _populate(self):
        ''' Return a list of sql commands to setup the DB for the fetch
            tests.
        '''
        populate = [
            "insert into %sbooze values ('%s')" % (self.table_prefix, s)
            for s in self.samples]
        return populate

    def test_fetchmany(self):
        con = self._connect()
        try:
            ps = self.executeDDL1(con)
            for sql in self._populate():
                ps = con.execute(sql)

            ps = con.execute('select name from %sbooze' % self.table_prefix)
            r = ps.fetchmany()
            self.assertEqual(
                len(r), 1,
                'ps.fetchmany retrieved incorrect number of rows, '
                'default of arraysize is one.')
            ps.arraysize = 10
            r = ps.fetchmany(3)  # Should get 3 rows
            self.assertEqual(
                len(r), 3,
                'ps.fetchmany retrieved incorrect number of rows')
            r = ps.fetchmany(4)  # Should get 2 more
            self.assertEqual(
                len(r), 2,
                'ps.fetchmany retrieved incorrect number of rows')
            r = ps.fetchmany(4)  # Should be an empty sequence
            self.assertEqual(
                len(r), 0,
                'ps.fetchmany should return an empty sequence after '
                'results are exhausted')
            self.assertEqual(ps.rowcount in (-1, 6), True)

            # Same as above, using cursor.arraysize
            ps = con.execute('select name from %sbooze' % self.table_prefix)
            r = ps.fetchmany(4)  # Should get 4 rows
            self.assertEqual(
                len(r), 4,
                'cursor.arraysize not being honoured by fetchmany')
            r = ps.fetchmany(4)  # Should get 2 more
            self.assertEqual(len(r), 2)
            r = ps.fetchmany()  # Should be an empty sequence
            self.assertEqual(len(r), 0)
            self.assertEqual(ps.rowcount in (-1, 6), True)

            ps = con.execute('select name from %sbooze' % self.table_prefix)
            rows = ps.fetchmany(6)  # Should get all rows
            self.assertEqual(ps.rowcount in (-1, 6), True)
            self.assertEqual(len(rows), 6)
            self.assertEqual(len(rows), 6)
            rows = [row[0] for row in rows]
            rows.sort()

            # Make sure we get the right data back out
            for i in range(0, 6):
                self.assertEqual(
                    rows[i], self.samples[i],
                    'incorrect data retrieved by cursor.fetchmany')

            rows = ps.fetchmany()  # Should return an empty list
            self.assertEqual(
                len(rows), 0,
                'ps.fetchmany should return an empty sequence if '
                'called after the whole result set has been fetched')
            self.assertEqual(ps.rowcount in (-1, 6), True)

            ps = self.executeDDL2(con)
            ps = con.execute('select name from %sbarflys' % self.table_prefix)
            r = ps.fetchmany()  # Should get empty sequence
            self.assertEqual(
                len(r), 0,
                'ps.fetchmany should return an empty sequence if '
                'query retrieved no rows')
            self.assertEqual(ps.rowcount in (-1, 0), True)

        finally:
            con.close()

    def test_fetchall(self):
        con = self._connect()
        try:
            ps = self.executeDDL1(con)
            for sql in self._populate():
                ps = con.execute(sql)

            # cursor.fetchall should raise an Error if called
            # after executing a a statement that cannot return rows
            self.assertRaises(self.driver.Error, ps.fetchall)

            ps = con.execute('select name from %sbooze' % self.table_prefix)
            rows = ps.fetchall()
            self.assertEqual(ps.rowcount in (-1, len(self.samples)), True)
            self.assertEqual(
                len(rows), len(self.samples),
                'cursor.fetchall did not retrieve all rows')
            rows = [r[0] for r in rows]
            rows.sort()
            for i in range(0, len(self.samples)):
                self.assertEqual(
                    rows[i], self.samples[i],
                    'cursor.fetchall retrieved incorrect rows')
            rows = ps.fetchall()
            self.assertEqual(
                len(rows), 0,
                'cursor.fetchall should return an empty list if called '
                'after the whole result set has been fetched')
            self.assertEqual(ps.rowcount in (-1, len(self.samples)), True)

            self.executeDDL2(con)
            ps = con.execute('select name from %sbarflys' % self.table_prefix)
            rows = ps.fetchall()
            self.assertEqual(ps.rowcount in (-1, 0), True)
            self.assertEqual(
                len(rows), 0,
                'cursor.fetchall should return an empty list if '
                'a select query returns no rows')

        finally:
            con.close()

    def test_mixedfetch(self):
        con = self._connect()
        try:
            ps = self.executeDDL1(con)
            for sql in self._populate():
                ps = con.execute(sql)

            ps = con.execute('select name from %sbooze' % self.table_prefix)
            rows1 = ps.fetchone()
            rows23 = ps.fetchmany(2)
            rows4 = ps.fetchone()
            rows56 = ps.fetchall()
            self.assertEqual(ps.rowcount in (-1, 6), True)
            self.assertEqual(
                len(rows23), 2, 'fetchmany returned incorrect number of rows')
            self.assertEqual(
                len(rows56), 2, 'fetchall returned incorrect number of rows')

            rows = [rows1[0]]
            rows.extend([rows23[0][0], rows23[1][0]])
            rows.append(rows4[0])
            rows.extend([rows56[0][0], rows56[1][0]])
            rows.sort()
            for i in range(0, len(self.samples)):
                self.assertEqual(
                    rows[i], self.samples[i],
                    'incorrect data retrieved or inserted')
        finally:
            con.close()

    def help_nextset_setUp(self, con):
        ''' Should create a procedure called deleteme
            that returns two result sets, first the
            number of rows in booze then "name from booze"
        '''
        raise NotImplementedError('Helper not implemented')
        #sql="""
        #    create procedure deleteme as
        #    begin
        #        select count(*) from booze
        #        select name from booze
        #    end
        #"""
        #cur.execute(sql)

    def help_nextset_tearDown(self, cur):
        'If cleaning up is needed after nextSetTest'
        raise NotImplementedError('Helper not implemented')
        #cur.execute("drop procedure deleteme")

    def test_nextset(self):
        con = self._connect()
        try:
            cur = con.cursor()
            if not hasattr(cur, 'nextset'):
                return

            try:
                self.executeDDL1(cur)
                sql = self._populate()
                for sql in self._populate():
                    cur.execute(sql)

                self.help_nextset_setUp(cur)

                cur.callproc('deleteme')
                numberofrows = cur.fetchone()
                assert numberofrows[0] == len(self.samples)
                assert cur.nextset()
                names = cur.fetchall()
                assert len(names) == len(self.samples)
                s = cur.nextset()
                assert s is None, 'No more return sets, should return None'
            finally:
                self.help_nextset_tearDown(cur)

        finally:
            con.close()

    '''
    def test_nextset(self):
        raise NotImplementedError('Drivers need to override this test')
    '''

    def test_arraysize(self):
        # Not much here - rest of the tests for this are in test_fetchmany
        con = self._connect()
        try:
            self.assertEqual(
                hasattr(con, 'arraysize'), True,
                'cursor.arraysize must be defined')
        finally:
            con.close()

    def test_None(self):
        con = self._connect()
        try:
            self.executeDDL1(con)
            con.execute(
                'insert into %sbooze values (NULL)' % self.table_prefix)
            ps = con.execute('select name from %sbooze' % self.table_prefix)
            r = ps.fetchall()
            self.assertEqual(len(r), 1)
            self.assertEqual(len(r[0]), 1)
            self.assertEqual(r[0][0], None, 'NULL value not returned as None')
        finally:
            con.close()

    def test_Date(self):
        self.driver.Date(2002, 12, 25)
        self.driver.DateFromTicks(
            time.mktime((2002, 12, 25, 0, 0, 0, 0, 0, 0)))
        # Can we assume this? API doesn't specify, but it seems implied
        # self.assertEqual(str(d1),str(d2))

    def test_Time(self):
        self.driver.Time(13, 45, 30)
        self.driver.TimeFromTicks(
            time.mktime((2001, 1, 1, 13, 45, 30, 0, 0, 0)))
        # Can we assume this? API doesn't specify, but it seems implied
        # self.assertEqual(str(t1),str(t2))

    def test_Timestamp(self):
        self.driver.Timestamp(2002, 12, 25, 13, 45, 30)
        self.driver.TimestampFromTicks(
            time.mktime((2002, 12, 25, 13, 45, 30, 0, 0, 0)))
        # Can we assume this? API doesn't specify, but it seems implied
        # self.assertEqual(str(t1),str(t2))

    def test_Binary(self):
        self.driver.Binary(b('Something'))
        self.driver.Binary(b(''))

    def test_STRING(self):
        self.assertEqual(
            hasattr(self.driver, 'STRING'), True,
            'module.STRING must be defined')

    def test_BINARY(self):
        self.assertEqual(
            hasattr(self.driver, 'BINARY'), True,
            'module.BINARY must be defined.')

    def test_NUMBER(self):
        self.assertTrue(
            hasattr(self.driver, 'NUMBER'), 'module.NUMBER must be defined.')

    def test_DATETIME(self):
        self.assertEqual(
            hasattr(self.driver, 'DATETIME'), True,
            'module.DATETIME must be defined.')

    def test_ROWID(self):
        self.assertEqual(
            hasattr(self.driver, 'ROWID'), True,
            'module.ROWID must be defined.')
