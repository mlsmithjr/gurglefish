import json
import os

from config import storagedir


class Journal(object):

    def __init__(self):
        files = os.listdir(storagedir)
        maxseq = 1
        for fname in files:
            if fname.startswith('jrnl-'):
                seq = int(fname[4:8])
                if seq > maxseq:
                    maxseq = seq
        self.journalname = os.path.join(storagedir, 'jrnl-{}.json'.format(maxseq+1))
        if os.path.isfile(self.journalname):
            with open(self.journalname, 'r') as f:
                self.journal = json.loads(f.read())
        else:
            self.journal = { 'pending': [] }

    def action(self, act):
        self.journal['pending'].append(act)
        self.save()

    def save(self):
        with open(self.journalname, "w+") as f:
            f.write(json.dumps(self.journal, indent=4))
