import json
from argumentparser import ArgumentParser
from filehandler import FileHandler
from dbconnector import *


def json2dict(filename):
    raw_data = FileHandler.read(filename)
    return json.loads(raw_data)


if __name__ == '__main__':
    parser = ArgumentParser()
    args = parser.parse_arguments()

    students_data = json2dict(args['students_file'])
    rooms_data = json2dict(args['rooms_file'])

    # working with database
    connector = HostelDBConnector('configs/config.ini')
    connector.connect()
    #connector.create_tables()
    #connector.insert_rooms(rooms_data)
    #connector.insert_students(students_data)

    print(connector.five_rooms_w_biggest_age_diffs())


    # end working with database

    # serialized_data = output_format_serializer[args['output_format']].serialize(rooms)
    # FileHandler.write(serialized_data, './output/rooms' + '.' + args['output_format'])
