
import unittest

from flask import Flask, g
from flask_jsontools import FlaskJsonClient, DynamicJSONEncoder
from sqlalchemy.orm.exc import NoResultFound

from . import models
from .crud_view import ArticlesView


class CrudTest(unittest.TestCase):
    def setUp(self):
        # Init db
        self.engine, self.Session = models.get_working_db_for_tests()
        self.db = self.Session()
        self.db.begin()

        # Flask
        self.app = app = Flask(__name__)
        app.debug = app.testing = True
        app.json_encoder = DynamicJSONEncoder
        app.test_client_class = FlaskJsonClient

        ArticlesView.route_as_view(app, 'articles', ('/article/', '/article/<int:id>'))

        @app.before_request
        def db():
            g.db = self.db

    def tearDown(self):
        self.db.close()  # Reset session

    def test_list(self):
        """ Test list() """

        # Simple list
        # maxitems:2, sort:id- should apply
        with self.app.test_client() as c:
            rv = c.get('/article/', json=None)
            self.assertEqual(rv['articles'], [
                # 2 items
                # sort: id-
                {'id': 30, 'uid': 3, 'theme': None, 'title': '30', 'data': {'o': {'z': False}}, 'calculated': 5, 'hybrid': False},
                {'id': 21, 'uid': 2, 'theme': None, 'title': '21', 'data': {'rating': 4, 'o': {'z': True}}, 'calculated': 4, 'hybrid': False}
            ])

        # Query list
        # Try to override sort, limit
        with self.app.test_client() as c:
            rv = c.get('/article/', json={
                'query': {
                    'limit': 3, # Cannnot exceed
                    'sort': ['id+'],  # Sort changed
                    'project': ['id', 'uid']
                }})
            self.assertEqual(rv['articles'], [
                # Still 2 items: cannot exceed maxitems
                # sort: id+ (overridden)
                # Projection worked
                {'id': 10, 'uid': 1},
                {'id': 11, 'uid': 1},
            ])

        # Query list, aggregate
        with self.app.test_client() as c:
            rv = c.get('/article/', json={
                'query': {
                    'filter': {
                        'id': {'$gte': '10'},
                    },
                    'aggregate': {
                        'n': {'$sum': 1},
                        'sum_ids': {'$sum': 'id'},
                        'max_rating': {'$max': 'data.rating'},
                        'avg_rating': {'$avg': 'data.rating'},
                    },
                    'sort': None,  # Unset initial sorting. Otherwise, PostgreSQL wants this column in GROUP BY
                }})
            self.assertEqual(rv['articles'], [
                {
                    'n': 6,
                    'sum_ids': 10+11+12+20+21+30,
                    'max_rating': 6.0,
                    'avg_rating': (5+5.5+6+4.5+4  +0)/5,
                }
            ])

        # Test count
        with self.app.test_client() as c:
            rv = c.get('/article/', json={
                'query': {
                    'count': 1
                }})
            self.assertEqual(rv['articles'], 6)  # `maxitems` shouldnt apply here; therefore, we don't get a '2'

    def test_create(self):
        """ Test create() """

        # Create
        # 'ro' field should be set manually
        with self.app.test_client() as c:
            rv = c.post('/article/', json={
                'article': {
                    'id': 999, 'uid': 999,
                    'title': '999',
                    'theme': None,
                    'data': {'wow': True}
                }
                })
            self.assertEqual(rv['article'], {
                'id': 1,  # Auto-set
                'uid': 3,  # Set manually
                'title': '999',
                'theme': None,
                'data': {'wow': True},
            })

    def test_get(self):
        """ Test get() """

        # Simple get
        with self.app.test_client() as c:
            rv = c.get('/article/30', json={
                'query': {
                    'project': ['id', 'uid'],
                }
            })
            self.assertEqual(rv['article'], {
                'id': 30, 'uid': 3
            })

        self.db.close()  # Reset session and its cache

        # Query get: relations
        with self.app.test_client() as c:
            rv = c.get('/article/30', json={
                'query': {
                    'project': ['id', 'uid'],
                    'join': ['user',],
                }
            })
            self.assertEqual(rv['article'], {
                'id': 30, 'uid': 3,
                'user': {
                    'id': 3,
                    'name': 'c',
                    'age': 16,
                    'tags': ['3', 'a', 'b', 'c'],
                }
            })

        self.db.close()  # Reset session and its cache

        # Query get: relations with filtering, projection and further joins
        with self.app.test_client() as c:
            rv = c.get('/article/30', json={
                'query': {
                    'project': ['id', 'uid'],
                    'join': {
                        'user': {
                            'project': ['name'],
                            'join': {
                                'comments': {
                                    'filter': {
                                        'uid': '3'
                                    }
                                }
                            },
                        }
                    }
                }
            })

            from pprint import pprint
            self.assertEqual(rv['article'], {
                'id': 30,
                'uid': 3,
                'user': {
                    'name': 'c',
                    'comments': [{'id': 102, 'uid': 3, 'aid': 10, 'text': '10-c', }]
                }
            })

        self.db.close()  # Reset session and its cache

    def test_update(self):
        """ Test update() """

        # Update
        # `uid` should be copied over
        # JSON `data` should be merged
        with self.app.test_client() as c:
            rv = c.post('/article/10', json={
                'article': {
                    'id': 999, 'uid': 999, # 'ro': ignored
                    'data': {'?': ':)'}
                }
            })
            self.assertEqual(rv['article'], {
                'id': 10,  # ro
                'uid': 1,  # ro
                'title': '10',  # Unchanged
                'theme': None,
                'data': {'?': ':)', 'o': {'a': True}, 'rating': 5},  # merged
            })

    def test_delete(self):
        """ Test delete() """

        # Delete
        with self.app.test_client() as c:
            rv = c.delete('/article/10', json=None)
            art = rv['article']
            art.pop('comments', None)
            self.assertEqual(rv['article'], {
                'id': 10, 'uid': 1,
                'title': '10',
                'theme': None,
                'data': {'o': {'a': True}, 'rating': 5},
            })

            self.db.close()

            self.assertRaises(NoResultFound, c.get, '/article/10')  # really removed

    def test_404(self):
        """ Try accessing entities that do not exist """

    def test_property_project(self):
        """ Test project of @property """

        # Simple get
        with self.app.test_client() as c:
            rv = c.get('/article/30', json={
                'query': {
                    'project': ['uid', 'calculated'],
                }
            })
            self.assertEqual(rv['article'], {
                'uid': 3, 'calculated': 5
            })
            rv = c.get('/article/', json={
                'query': {
                    'project': ['uid', 'calculated'],
                }
            })
            self.assertEqual(rv['articles'], [
                # 2 items
                # sort: id-
                {'uid': 3, 'calculated': 5},
                {'uid': 2, 'calculated': 4}
            ])
            # Propjection for join
            rv = c.get('/article/20', json={
                'query': {
                    'project': ['id'],
                    'join': {'comments': {
                        'project': ['id', 'comment_calc'],
                    }}}
            })
            self.assertEqual(rv['article'], {
                'id': 20,
                'comments': [
                    {'comment_calc': u'ONE', 'id': 106},
                    {'comment_calc': u'TWO', 'id': 107}]
            })

            try:
                rv = c.get('/article/', json={
                    'query': {
                        'project': ['uid', 'no_such_property'],
                    }
                })
                assert False, 'Should throw an exception'
            except:
                pass
