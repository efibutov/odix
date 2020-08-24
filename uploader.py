import os
import argparse
import inotify.adapters
from multiprocessing import Process


def watch_incoming_files(incoming_dir):
    i = inotify.adapters.Inotify()
    i.add_watch(incoming_dir)

    for event in i.event_gen(yield_nones=False):
        (_, type_names, path, filename) = event

        print("PATH=[{}] FILENAME=[{}] EVENT_TYPES={}".format(
              path, filename, type_names))

        if type_names == ['IN_ATTRIB']:
            print(filename)

    print('FINISHED')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir=", "--foperand", required=True, help="source directory")
    ap.add_argument("--output_dir=", "--soperand", required=True, help="output directory")
    args = vars(ap.parse_args())
    print(args)
    source_dir = args['input_dir=']
    output_dir = args['output_dir=']

    for d in (source_dir, output_dir):
        if not os.path.isdir(source_dir):
            raise ValueError(f'Not existing directory "{source_dir}"')

    t = Process(target=watch_incoming_files, args=(source_dir,))
    t.start()


if __name__ == '__main__':
    main()


