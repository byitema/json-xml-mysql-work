class FileHandler:
    @staticmethod
    def read(filename):
        with open(filename, 'r') as f:
            data = f.read()

        return data

    @staticmethod
    def write(data, filename):
        with open(filename, 'w') as f:
            f.write(data)
