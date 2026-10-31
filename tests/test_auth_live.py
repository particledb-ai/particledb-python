import os
import unittest
from particledb.connection import Connection
from particledb.errors import ConnectionError


@unittest.skipUnless(os.environ.get('PDB_AUTH_PORT'), 'PDB_AUTH_PORT required')
class NativeAuthentication(unittest.TestCase):
    def test_valid_credentials_and_wrong_password(self):
        options = dict(host='127.0.0.1', port=int(os.environ['PDB_AUTH_PORT']),
                       user=os.environ['PDB_AUTH_USER'], password=os.environ['PDB_AUTH_PASS'])
        with Connection(**options) as db:
            self.assertEqual(db.scalar('SELECT 42'), 42)
        options['password'] = 'wrong-test-password'
        with self.assertRaises(ConnectionError) as caught:
            Connection(**options)
        self.assertEqual(caught.exception.sqlstate, '28P01')
