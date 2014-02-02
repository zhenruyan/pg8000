Quick Start
===========

Installation
------------
pg8000 is available for Python 2.5, 2.6, 2.7, 3.2 and 3.3 (and has been tested
on CPython, Jython and PyPy).

To install pg8000 using `pip <https://pypi.python.org/pypi/pip>`_ type:

``pip install pg8000``

Interactive Example
-------------------


.. code-block:: python

    >>> import pg8000

    >>> conn = pg8000.connect(user="postgres", password="C.P.Snow")

    >>> ps = conn.execute("CREATE TEMPORARY TABLE book (id SERIAL, title TEXT)")

    >>> ps = conn.execute(
    ...     "INSERT INTO book (title) VALUES (:1), (:2) RETURNING id, title",
    ...     ("Ender's Game", "Speaker for the Dead"))
    >>> for row in ps:
    ...     id, title = row
    ...     print("id = %s, title = %s" % (id, title))
    id = 1, title = Ender's Game
    id = 2, title = Speaker for the Dead
    >>> conn.commit()

    >>> conn.execute("SELECT extract(millennium from now())").fetchone()
    [3.0]

    >>> import datetime
    >>> conn.execute("SELECT timestamp '2013-12-01 16:06' - :1",
    ... (datetime.date(1980, 4, 27),)).fetchone()
    [<Interval 0 months 12271 days 57960000000 microseconds>]

    >>> conn.execute("SELECT array_prepend(:1, :2)",
    ... ( 500, [1, 2, 3, 4], )).fetchone()
    [[500, 1, 2, 3, 4]]
    >>> conn.rollback()

    Following the DB-API specification, autocommit is off by default. It can be
    turned on by using the autocommit property of the connection.

    >>> conn.autocommit = True
    >>> ps = conn.execute("vacuum")
    >>> conn.autocommit = False
    
    >>> conn.close()
