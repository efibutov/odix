import os
import argparse
import requests
import inotify.adapters
from multiprocessing import Pool, cpu_count


def sanitize_file(source_path, file_name, output_path):
    upload_url = 'https://cdrservice.odi-x.com/api/v2/request/'
    files = {'file': open(os.path.join(source_path, file_name), 'rb')}
    headers = {'X-Api-Key': "PYKewn5xs8sWzSdBYyjYZTv76Ial11eFOgXnLzX4"}
    request_data = {'sync': 'true'}
    upload_response = requests.post(upload_url, data=request_data, files=files, headers=headers, verify=False)

    with open(os.path.join(output_path, file_name), 'wb') as w:
        if upload_response.ok:
            w.write(upload_response.content)
        else:
            w.write(upload_response.text.encode('utf-8'))


def watch_incoming_files(incoming_dir, output_dir):
    workers = Pool(cpu_count() - 1)
    i = inotify.adapters.Inotify()
    i.add_watch(incoming_dir)

    for event in i.event_gen(yield_nones=False):
        (_, type_names, path, filename) = event
        if type_names[0] in ['IN_ATTRIB', 'IN_MOVED_TO']:
            workers.apply_async(
                func=sanitize_file,
                args=(path, filename, output_dir),
            )

    workers.close()
    workers.join()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir=", "--foperand", required=True, help="source directory")
    ap.add_argument("--output_dir=", "--soperand", required=True, help="output directory")
    args = vars(ap.parse_args())
    source_dir = args['input_dir=']
    output_dir = args['output_dir=']

    for d in (source_dir, output_dir):
        if not os.path.isdir(source_dir):
            raise ValueError(f'Not existing directory "{source_dir}"')

    watch_incoming_files(source_dir, output_dir)


if __name__ == '__main__':
    main()


