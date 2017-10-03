import argparse
import json


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", help="json file to use as pattern", metavar="json_filename", required=True)
    args = parser.parse_args()

    if args.json:
        with open(args.json, 'r') as jsonfile:
            adict = json.load(jsonfile)
            print(  'class DictWrapper(object):\n' +\
                    '    bucket = dict()\n')

            for k in adict.keys():
                print('    @property\n' +\
                      '    def {}(self):'.format(k))
                print('        return self.bucket["{}"]\n'.format(k))

