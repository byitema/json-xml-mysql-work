import argparse
import os
from serializer import JSONSerializer, XMLSerializer


class BaseArgumentParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.add_arguments()

    def add_arguments(self):
        pass

    def parse_arguments(self):
        pass


class ArgumentParser(BaseArgumentParser):
    output_format_serializer = {
        'json': JSONSerializer,
        'xml': XMLSerializer
    }

    def add_arguments(self):
        self.parser.add_argument('--students',
                                 help='path to the students file directory',
                                 type=ArgumentParser.check_path)
        self.parser.add_argument('--rooms',
                                 help='path to the rooms file directory',
                                 type=ArgumentParser.check_path)
        self.parser.add_argument('--format',
                                 help='output format (XML or JSON)',
                                 type=ArgumentParser.check_output_format)
        self.parser.add_argument('--config',
                                 help='path to the database config file',
                                 type=ArgumentParser.check_path)

    def parse_arguments(self):
        args = self.parser.parse_args()
        return {'students_file': args.students,
                'rooms_file': args.rooms,
                'output_format': args.format.lower(),
                'output_format_serializer': self.output_format_serializer[args.format.lower()],
                'config_file': args.config}

    @staticmethod
    def check_path(path: str):
        if os.path.isfile(path):
            return path
        else:
            raise argparse.ArgumentTypeError(f'{path} is not a valid path')

    @staticmethod
    def check_output_format(output_format: str):
        if output_format.lower() in ('xml', 'json'):
            return output_format
        else:
            raise argparse.ArgumentTypeError(
                f'{output_format} is not a valid output format (XML or JSON are only available)')
