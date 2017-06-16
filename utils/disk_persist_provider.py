import os

class DiskPersistProvider(object):
    def __init__(self, persist_path='./persist'):
        self.path = persist_path

    def set_path(self, path):
        self.path = path

    def dump(self, object, name):
        import pickle
        with open(os.path.join(self.path, name+'.pkl'), 'wb') as f:
            pickle.dump(object, f)

    def load(self, name):
        import pandas as pd
        df = pd.read_pickle(os.path.join(self.path, name+'.pkl'))
        return df

