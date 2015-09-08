from pathlib import PosixPath
from pymongo import MongoClient
from unittest import TestCase
import io, json, os
from json_handler import JsonHandler


class TestJSON(TestCase):

    def setUp(self):
        for f in 'testfile1.json', 'testfile2.JSON', 'fakefile.txt':
            handle = open(f, 'w+')
            handle.write('{"a": 1, "b": 2}')
            handle.close()
        self.j = JsonHandler('.', 'unittest', 'unittest')
        self.client = MongoClient()
        self.client.drop_database('unittest')
        self.coll = self.client['unittest']['unittest']

    def test_can_make_pathlib_object_of_json_files(self):
        self.assertIn(PosixPath('testfile1.json'), self.j.jsons)
        self.assertIn(PosixPath('testfile2.JSON'), self.j.jsons)
        self.assertNotIn('fakefile.txt', self.j.jsons)

    def test_can_parse_json(self):
        self.assertEqual(self.j.parse_one_json(self.j.jsons[0]), {"a": 1, "b": 2})

    def test_can_put_files_in_database(self):
        self.assertEqual(self.coll.count(), 0)
        self.j.put_jsons_in_database(self.j.jsons, self.j.db_name, self.j.coll_name)
        self.assertEqual(self.coll.count(), 2)

    def test_inserted_docs_can_be_retrieved(self):
        self.j.put_jsons_in_database(self.j.jsons, self.j.db_name, self.j.coll_name)
        self.assertEqual(self.coll.find({"a": 1}).count(), 2)

    def tearDown(self):
        for f in 'testfile1.json', 'testfile2.JSON', 'fakefile.txt':
            os.remove(f)
        self.client.drop_database('unittest')
        self.client.close()
