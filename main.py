import json
import os
from parser import parser


def main():
    configs = {}
    if os.path.exists('config.json') and os.path.isfile('config.json'):
        config_file = open('config.json')
        configs = json.load(config_file)

    keys = ["template_csv_file_path",
            "template_directory_path",
            "output_directory_path",
            "flat_file_placeholder",
            "template_table_name",
            "template_values_table_name"]

    for key in keys:
        if key not in configs:
            print("{} doesn't exist".format(key))
            return

    args = [configs[key] for key in keys]

    parser(*args)


main()
