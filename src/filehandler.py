import json


class FileHandler:
    @staticmethod
    def read_json(filename):
        with open(filename, 'r') as f:
            dictionary = json.loads(f.read())

        return dictionary

    @staticmethod
    def write(data, filename):
        with open(filename, 'w') as f:
            f.write(data)
