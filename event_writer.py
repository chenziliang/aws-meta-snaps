import json


class CsvEventWriter(object):

    def __init__(self, fname, mode='a'):
        self.fname = fname
        self.mode = mode
        self.opened_file = None

    def __enter__(self):
        self.opened_file = open(self.fname, self.mode)
        return self

    def __exit__(self, *args):
        self.opened_file.close()

    def write(self, metas):
        assert self.opened_file

        self.opened_file.write(
            '\n'.join(json.dumps(meta) for meta in metas))