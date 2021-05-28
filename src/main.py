import json
from argumentparser import ArgumentParser
from dbconnector import HostelDBConnector
from filehandler import FileHandler


def json2dict(filename):
    raw_data = FileHandler.read(filename)
    return json.loads(raw_data)


if __name__ == '__main__':
    parser = ArgumentParser()
    args = parser.parse_arguments()

    students_data = json2dict(args['students_file'])
    rooms_data = json2dict(args['rooms_file'])

    connector = HostelDBConnector(args['config_file'])
    connector.connect()

    #connector.create_tables()
    #connector.insert_rooms(rooms_data)
    #connector.insert_students(students_data)

    # adding index on birthday
    # - we have queries that use this column
    # - no need to create index on enum column with only 2 elements ('Sex': 'M', 'F')
    # - we do not use other column in select queries
    index_creation = '''create index idx_student_birthday on hostel.student (birthday) 
                        comment '' 
                        algorithm default lock default;'''
    #connector.execute_query(index_creation)

    selected_data = {
        'room_students_count': connector.get_room_students_count(),
        'five_rooms_w_least_avg_ages': connector.get_five_rooms_w_least_avg_ages(),
        'five_rooms_w_biggest_age_diffs': connector.get_five_rooms_w_biggest_age_diffs(),
        'rooms_w_different_sexes_students': connector.get_rooms_w_different_sexes_students()
    }

    for name, data in selected_data.items():
        serialized_data = args['output_format_serializer'].serialize(data)
        FileHandler.write(serialized_data, './output/' + name + '.' + args['output_format'])
