import unittest
from collections import OrderedDict

from mongosql.statements import *
from mongosql.exc import InvalidColumnError, InvalidQueryError, InvalidRelationError
from .models import *


class StatementsTest(unittest.TestCase):
    """ Test individual statements """

    def test_projection(self):
        def test_by_full_projection(p, **expected_full_projection):
            """ Test:
                * get_full_projection()
                * __contains__() of a projection using its full projection
                * compile_columns()
            """
            self.assertEqual(p.get_full_projection(), expected_full_projection)

            # Test properties: __contains__()
            for name, include in expected_full_projection.items():
                self.assertEqual(name in p, True if include else False)

            # Test: compile_columns() only returns column properties
            columns = p.compile_columns()
            self.assertEqual(
                set(col.key for col in columns),
                set(col_name
                    for col_name in p.bags.columns.names
                    if expected_full_projection.get(col_name, 0))
            )

        # === Test: No input
        p = MongoProjection(Article).input(None)
        self.assertEqual(p.mode, p.MODE_EXCLUDE)
        self.assertEqual(p.projection, dict())

        # === Test: Valid projection, array
        p = MongoProjection(Article).input(['id', 'uid', 'title'])
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, uid=1, title=1))

        test_by_full_projection(p,
                                # Explicitly included
                                id=1, uid=1, title=1,
                                # Implicitly excluded
                                theme=0, data=0,
                                # Properties excluded
                                calculated=0, hybrid=0,
                                )

        # === Test: Valid projection, dict, include mode
        p = MongoProjection(Article).input(dict(id=1, uid=1, title=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, uid=1, title=1))

        test_by_full_projection(p, # basically, the same thing
                                id=1, uid=1, title=1,
                                theme=0, data=0,
                                calculated=0, hybrid=0,
                                )

        # === Test: Valid projection, dict, exclude mode
        p = MongoProjection(Article).input(dict(theme=0, data=0))
        self.assertEqual(p.mode, p.MODE_EXCLUDE)
        self.assertEqual(p.projection, dict(theme=0, data=0))

        test_by_full_projection(p,
                                id=1, uid=1, title=1,
                                theme=0, data=0,
                                calculated=1, hybrid=1,
                                )

        # === Test: `default_exclude` in exclude mode
        p = MongoProjection(Article, default_exclude=('calculated', 'hybrid'))\
            .input(dict(theme=0, data=0))
        self.assertEqual(p.mode, p.MODE_EXCLUDE)
        self.assertEqual(p.projection, dict(theme=0, data=0,
                                            # Extra stuff
                                            calculated=0, hybrid=0))

        test_by_full_projection(p,
                                id=1, uid=1, title=1,
                                theme=0, data=0,
                                calculated=0, hybrid=0,  # now excluded
                                )

        # === Test: `default_exclude` in include mode (no effect)
        p = MongoProjection(Article, default_exclude=('calculated', 'hybrid')) \
            .input(dict(id=1, calculated=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, calculated=1))

        test_by_full_projection(p,
                                id=1, uid=0, title=0,
                                theme=0, data=0,
                                calculated=1, hybrid=0,  # one included, one excluded
                                )

        # === Test: default_projection
        pr = Reusable(MongoProjection(Article, default_projection=dict(id=1, title=1)))

        p = pr.input(None)
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, title=1))

        p = pr.input(None)
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, title=1))

        # === Test: force_include
        pr = Reusable(MongoProjection(Article, force_include=('id',)))

        # Include mode
        p = pr.input(dict(title=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, title=1))  # id force included
        # Exclude mode
        p = pr.input(dict(data=0))
        self.assertEqual(p.mode, p.MODE_MIXED)
        self.assertEqual(p.projection, dict(id=1,  # force included
                                            uid=1, title=1, theme=1,
                                            data=0,  # excluded by request
                                            calculated=1, hybrid=1))

        # === Test: force_exclude
        pr = Reusable(MongoProjection(Article, force_exclude=('data',)))
        # Include mode
        p = pr.input(dict(id=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1))  # no `data`
        # Include mode: same property
        p = pr.input(dict(id=1, data=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1))  # No more data, even though requested
        # Exclude mode
        p = pr.input(dict(theme=0))
        self.assertEqual(p.mode, p.MODE_EXCLUDE)
        self.assertEqual(p.projection, dict(theme=0,  # excluded by request
                                            data=0,  # force excluded
                                            ))

        # === Test: Invalid projection, dict, problem: invalid arguments passed to __init__()
        with self.assertRaises(InvalidColumnError):
            MongoProjection(Article, default_projection=dict(id=1, INVALID=1))
        with self.assertRaises(InvalidQueryError):
            MongoProjection(Article, default_projection=dict(id=1, title=0))

        with self.assertRaises(InvalidColumnError):
            MongoProjection(Article, default_exclude='id')
        with self.assertRaises(InvalidColumnError):
            MongoProjection(Article, default_exclude=('INVALID',))

        with self.assertRaises(InvalidColumnError):
            MongoProjection(Article, force_exclude='id')
        with self.assertRaises(InvalidColumnError):
            MongoProjection(Article, force_exclude=('INVALID',))

        with self.assertRaises(InvalidColumnError):
            MongoProjection(Article, force_include='id')
        with self.assertRaises(InvalidColumnError):
            MongoProjection(Article, force_include=('INVALID',))

        # === Test: Invalid projection, dict, problem: 1s and 0s
        pr = Reusable(MongoProjection(Article))

        with self.assertRaises(InvalidQueryError):
            pr.input(dict(id=1, title=0))

        # === Test: Invalid projection, dict, problem: invalid column
        with self.assertRaises(InvalidColumnError):
            pr.input(dict(INVALID=1))

        # === Test: A mixed object is only acceptable when it mentions EVERY column
        # No error
        MongoProjection(Article).input(dict(id=1, uid=1, title=1, theme=1, data=0,
                                            calculated=1, hybrid=1))

    def test_sort(self):
        sr = Reusable(MongoSort(Article))

        # === Test: no input
        s = sr.input(None)
        self.assertEqual(s.sort_spec, OrderedDict())

        # === Test: list
        s = sr.input(['id', 'uid+', 'title-'])
        self.assertEqual(s.sort_spec, OrderedDict(id=+1, uid=+1, title=-1))

        # === Test: OrderedDict
        s = sr.input(OrderedDict(id=+1, uid=+1, title=-1))
        self.assertEqual(s.sort_spec, OrderedDict(id=+1, uid=+1, title=-1))

        # === Test: dict
        # One item allowed
        s = sr.input(dict(id=-1))
        # Two items disallowed
        with self.assertRaises(InvalidQueryError):
            s = sr.input(dict(id=-1, uid=+1))

        # === Test: invalid columns
        with self.assertRaises(InvalidColumnError):
            # Invalid column
            sr.input(dict(INVALID=+1))

        with self.assertRaises(InvalidColumnError):
            # Properties not supported
            sr.input(dict(calculated=+1))

        # Hybrid properties are ok
        sr.input(dict(hybrid=+1))

        # === Test: JSON column fields
        sr.input({'data.rating': -1})

    def test_group(self):
        # === Test: list
        g = MongoGroup(Article).input(['uid'])
        self.assertEqual(g.group_spec, OrderedDict(uid=+1))
