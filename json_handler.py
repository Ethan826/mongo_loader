import json, re, pymongo, sys
from pathlib import Path, PosixPath
from pymongo import MongoClient


class JsonHandler:

    def __init__(self, path, db_name, coll_name):
        self.path = path
        self.jsons = self.get_jsons()
        self.db_name = db_name
        self.coll_name = coll_name

    def get_jsons(self):
        my_files = Path(self.path)
        return [f for f in my_files.iterdir() if re.search(r'\.json$', f.suffix, re.I)]
    
    @staticmethod
    def parse_one_json(json_file):
        with json_file.open() as f:
            contents = json.load(f)
        return contents

    @staticmethod
    def put_jsons_in_database(jsons, db_name, coll_name):
        client = MongoClient()
        coll = client[db_name][coll_name]
        counter, size = 0, len(jsons)
        for j in jsons:
            counter += 1
            print("Inserting document {} of {}.".format(counter, size))
            contents = JsonHandler.parse_one_json(j)
            coll.insert_one(contents)
        client.close()

if __name__ == '__main__':
    if len(sys.argv) == 4:
        j = JsonHandler(sys.argv[1], sys.argv[2], sys.argv[3])
        j.put_jsons_in_database(j.jsons, j.db_name, j.coll_name)
    else:
        raise TypeError("Expected 3 command line arguments, received {}.".format(len(sys.argv) - 1))
