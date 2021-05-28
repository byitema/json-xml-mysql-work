import mysql.connector as mysql
from configparser import ConfigParser


def read_db_config(filename, section):
    parser = ConfigParser()
    parser.read(filename)

    config = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            config[item[0]] = item[1]
    else:
        raise Exception(f'{section} not found in the {filename} file')

    return config


def split_name(name: str):
    if name.startswith('Dr. ') or name.startswith('Mr. ') or name.startswith('Ms. '):
        name = name[4:]
    elif name.startswith('Mrs. '):
        name = name[5:]

    name_parts = name.split()
    if len(name_parts) == 2:
        name_parts.append('')

    return name_parts


class DBConnector:
    def __init__(self, filename):
        self.db_config = read_db_config(filename, 'mysql')

    def connect(self):
        self.connection = None
        try:
            self.connection = mysql.MySQLConnection(**self.db_config)
        except Exception as ex:
            raise ex

    def select_query(self, query: str):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

            self.connection.commit()

        return rows

    def execute_query(self, query: str):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()


class HostelDBConnector(DBConnector):
    create_room_table = '''create table if not exists room (
                               room_id int,
                               room_name varchar(255),
                               primary key (room_id)
                           );'''

    create_student_table = '''create table if not exists student (
                                  student_id int,
                                  first_name varchar(127),
                                  last_name varchar(127),
                                  degree varchar (31),
                                  birthday datetime,
                                  room_id int,
                                  sex enum ('M', 'F'),
                                  primary key (student_id),
                                  foreign key (room_id)
                                      references room (room_id)
                                      on update restrict on delete cascade
                              );'''

    room_students_count = '''select room.room_id, room.room_name, count(student.student_id) as students_count
                             from room
                             join student on room.room_id = student.room_id
                             group by room.room_id;'''

    five_rooms_w_least_avg_ages = '''select room.room_id, room.room_name, avg(year(current_timestamp) - year(student.birthday)) as avg_age
                                     from room 
                                     join student on room.room_id = student.room_id
                                     group by room.room_id
                                     order by avg_age
                                     limit 5;'''

    five_rooms_w_biggest_age_diffs = '''select room.room_id, room.room_name, max(year(current_timestamp) - year(student.birthday)) - min(year(current_timestamp) - year(student.birthday)) as age_diff
                                        from room 
                                        join student on room.room_id = student.room_id
                                        group by room.room_id
                                        order by age_diff desc
                                        limit 5;'''

    rooms_w_different_sexes_students = '''select room.room_id, room.room_name
                                          from room 
                                          join student on room.room_id = student.room_id
                                          group by room.room_id
                                          having count(distinct student.sex) > 1;'''

    def create_tables(self):
        with self.connection.cursor() as cursor:
            cursor.execute(self.create_room_table)
            cursor.execute(self.create_student_table)

            self.connection.commit()

    def insert_students(self, students):
        query = 'insert into student(student_id, first_name, last_name, degree, birthday, room_id, sex) ' \
                'values(%s,%s,%s,%s,%s,%s,%s)'

        with self.connection.cursor() as cursor:
            for student in students:
                first_name, last_name, degree = split_name(student['name'])

                if degree == '':
                    degree = None

                cursor.execute(
                    query,
                    [student['id'],
                     first_name,
                     last_name,
                     degree,
                     student['birthday'],
                     student['room'],
                     student['sex']]
                )

            self.connection.commit()

    def insert_rooms(self, rooms):
        query = 'insert into room(room_id, room_name) values(%s,%s)'

        with self.connection.cursor() as cursor:
            for room in rooms:
                args = (room['id'], room['name'])
                cursor.execute(query, args)

            self.connection.commit()

    def get_room_students_count(self):
        rows = self.select_query(self.room_students_count)

        res = []
        for row in rows:
            row_dict = dict()
            row_dict['room_id'], row_dict['room_name'], row_dict['students_count'] = row
            res.append(row_dict)

        return res

    def get_five_rooms_w_least_avg_ages(self):
        rows = self.select_query(self.five_rooms_w_least_avg_ages)

        res = []
        for row in rows:
            row_dict = dict()
            row_dict['room_id'], row_dict['room_name'], row_dict['avg_age'] = row
            row_dict['avg_age'] = float(row_dict['avg_age'])
            res.append(row_dict)

        return res

    def get_five_rooms_w_biggest_age_diffs(self):
        rows = self.select_query(self.five_rooms_w_biggest_age_diffs)

        res = []
        for row in rows:
            row_dict = dict()
            row_dict['room_id'], row_dict['room_name'], row_dict['age_diff'] = row
            res.append(row_dict)

        return res

    def get_rooms_w_different_sexes_students(self):
        rows = self.select_query(self.rooms_w_different_sexes_students)

        res = []
        for row in rows:
            row_dict = dict()
            row_dict['room_id'], row_dict['room_name'] = row
            res.append(row_dict)

        return res
