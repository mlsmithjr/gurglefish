import argparse
import yaml


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--yaml", help="yaml file to use as pattern", metavar="yaml_filename", required=True)
    args = parser.parse_args()

    if args.json:
        with open(args.yaml, 'r') as pfile:
            adict = yaml.load(pfile)
            print(  'class DictWrapper(object):\n' +\
                    '    bucket = dict()\n')

            for k in adict.keys():
                print('    @property\n' +\
                      '    def {}(self):'.format(k))
                print('        return self.bucket["{}"]\n'.format(k))

