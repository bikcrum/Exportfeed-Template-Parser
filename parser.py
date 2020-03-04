import os
from datetime import datetime
import math
import json

import pandas as pd

logs = []

root = None


def extract_prepended_number(text):
    numstr = []
    for i in range(len(text) - 1, -1, -1):
        if '0' <= text[i] <= '9':
            numstr.append(text[i])
        else:
            return text[0:i + 1], None if len(numstr) == 0 else int(''.join(reversed(numstr)))

    return None, None if len(numstr) == 0 else int(''.join(reversed(numstr)))


def get_data_frame(*file_paths):
    global logs

    for file_path in file_paths:
        if os.path.exists(file_path) and os.path.isfile(file_path):
            print_progress('PROCESSING:%s' % file_path)
            try:
                return pd.read_csv(file_path, header=None, encoding="ISO-8859-1", low_memory=False)
            except pd.errors.EmptyDataError:
                print_progress("EMPTY_FILE:%s is empty" % file_path)
                return None

    print_progress("MISSING:%s doesn't exist" % (','.join(file_paths)))
    return None


def get_logs():
    return logs


def export(data, csv_file, sql_file, columns, csv_index, sql_index, start_index, table_name):
    os.makedirs(os.path.dirname(sql_file), exist_ok=True)

    if csv_file is not None:
        """CREATE CSV FILE"""
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)

        df = pd.DataFrame(data=data,
                          columns=columns)

        # add csv_index as index
        if csv_index:
            df.index.name = csv_index
            df.index += start_index
            df.to_csv(csv_file, index=True)
        else:
            df.to_csv(csv_file, index=False)

    """CREATE SQL FILE"""
    # add sql_index as index
    if sql_index:
        sql_columns = ','.join(['`{}`'.format(column) for column in [sql_index] + columns])
    else:
        sql_columns = ','.join(['`{}`'.format(column) for column in columns])

    text = "INSERT INTO `{}` ({}) VALUES\n".format(table_name, sql_columns)

    for i in range(len(data)):

        if sql_index:
            row = ','.join(["{}".format(json.dumps(str(d))) for d in [i + start_index] + data[i]])
        else:
            row = ','.join(["{}".format(json.dumps(str(d))) for d in data[i]])

        if i == len(data) - 1:
            text += '({});'.format(row)
        else:
            text += '({}),\n'.format(row)

    f = open(sql_file, 'w')
    f.write(text)
    f.close()


def write_logs(output_directory_path, start_time, end_time):
    errors = 0
    out_logs = open('%s/logs-error.txt' % output_directory_path, 'w')
    for log in logs:
        if log.startswith('ERROR:'):
            errors += 1
            out_logs.write(log + '\n')

    out_logs.close()

    empty_files = 0
    out_logs = open('%s/logs-empty-files.txt' % output_directory_path, 'w')
    for log in logs:
        if log.startswith('EMPTY_FILE:'):
            empty_files += 1
            out_logs.write(log + '\n')

    out_logs.close()

    missing_files = 0
    out_logs = open('%s/logs-missing-files.txt' % output_directory_path, 'w')
    for log in logs:
        if log.startswith('MISSING:'):
            missing_files += 1
            out_logs.write(log[len('MISSING:'):] + '\n')

    out_logs.close()

    already_exist = 0
    out_logs = open('%s/logs-already-exist-files.txt' % output_directory_path, 'w')
    for log in logs:
        if log.startswith('ALREADY_EXIST:'):
            already_exist += 1
            out_logs.write(log[len('ALREADY_EXIST:'):] + '\n')

    out_logs.close()

    global current_rows, total_rows
    current_rows = total_rows
    log_text = 'OPERATION COMPLETED in {} with {} errors, {} empty files, {} already exist files, {} missing files. Check logs.'.format(
        end_time - start_time, errors, empty_files, already_exist, missing_files)

    print_progress(log_text)

    out_logs = open('%s/logs.txt' % output_directory_path, 'w')
    out_logs.write('\n'.join(logs))
    out_logs.close()


def print_progress(text):
    logs.append(text)
    # percent = current_row * 100.0 / total_rows
    # percent_ceil = math.ceil(percent)
    # print("\r|{}>{}| %.2f%% Done".format('=' * percent_ceil, '-' * (100 - percent_ceil)) % percent, end=' ')
    print(text, end='\n')


def get_template_data(df, category, country_code, flat_tmp_id):
    now = datetime.now()
    str_now = now.strftime("%Y-%m-%d %H:%M:%S")

    # parsing head in key-value pairs
    kv_dict = {}
    for j in range(len(df.iloc[0])):
        kv = df.iloc[0, j]

        if kv is None or pd.isna(kv):
            continue

        kv = kv.split('=')

        if len(kv) > 1:
            kv_dict[kv[0].strip('\n').strip()] = kv[1].strip('\n').strip()

    # see if all keys in the list exist in kv_dict
    must_keys = ['Version']
    all_key_exist = True
    for key in must_keys:
        if key not in kv_dict:
            all_key_exist = False
            break

    if not all_key_exist:
        print_progress('ERROR:Head of csv do not have some keys in %s of %s' % (category, country_code))
        return None

    # column head
    # 'id', 'tpl_name', 'version', 'country', 'raw', 'imported_status', 'imported_by', 'imported_at',
    # 'flat_tmpl_id', 'status'

    raw = df.to_csv(index=False, header=False, sep='\t')

    return [category, kv_dict['Version'], country_code, raw, 1, 1, str_now, flat_tmp_id, 1]


def get_template_definition(df, df_val, category, country_code, flat_tmp_id):
    kv = {}
    now = datetime.now()
    str_now = now.strftime("%Y-%m-%d %H:%M:%S")

    for i in range(len(df_val.columns)):
        values = []
        for j in range(2, len(df_val[i])):
            value = df_val.iloc[j, i]
            if value is not None and not pd.isnull(value):
                values.append(value)

        head = df_val.iloc[0, i]
        head = [h.strip().strip(']').strip('[').strip() for h in str(head).split('-')]

        pos = 1 if len(head) >= 2 else 0

        key = df_val.iloc[1, i]

        if key in kv:
            kv[key][head[pos]] = values
        else:
            kv[key] = {head[pos]: values}

    new_kv = {}
    for key in kv:
        inner_keys = [k for k in kv[key].keys()]
        if len(inner_keys) >= 2:
            new_kv[key] = kv[key]
        else:
            new_kv[key] = kv[key][inner_keys[0]]

    data = []
    for i in range(3, len(df)):
        field_name = df.iloc[i, 1]
        local_label_name = df.iloc[i, 2]
        example = df.iloc[i, 5]
        required = 1 if df.iloc[i, 6] == 'Required' else 0

        if field_name is None or pd.isnull(field_name) or len(field_name) == 0:
            continue

        # `fields`, `labels`, `examples`, `definition`, `valid_values`, `tmpl_id`, `country`, `required`, `status`, `imported_by`, `imported_at`
        valid_values = json.dumps(new_kv[field_name]) if field_name in new_kv else ''

        field_name_range = [fn.strip() for fn in field_name.split('-')]

        if len(field_name_range) >= 2:
            range_name, lb = extract_prepended_number(field_name_range[0])
            _, ub = extract_prepended_number(field_name_range[1])

            if lb is not None and ub is not None and range_name is not None:
                for j in range(lb, ub + 1):
                    data.append(
                        ['{}{}'.format(range_name, j), local_label_name, example, required, valid_values, flat_tmp_id,
                         country_code, required, 1, 1, str_now])
                continue

        data.append([field_name, local_label_name, example, required, valid_values, flat_tmp_id,
                     country_code, required, 1, 1, str_now])

    return data


def parser(template_csv_file_path, template_directory_path, output_directory_path, flat_file_placeholder,
           template_table_name, template_values_table_name, create_csv, filter_country):
    template_csv_df = pd.read_csv(template_csv_file_path, header=None)

    global logs, total_rows, current_row
    current_row = 0

    logs = []

    start = datetime.now()

    total_rows = len(template_csv_df)

    # group by country code
    groups = dict(tuple(template_csv_df.groupby(2)))
    # country_codes = sorted(groups.keys(), reverse=True)
    country_codes = groups.keys()

    for country_code in country_codes:
        if len(filter_country) > 0 and country_code not in filter_country:
            continue

        template_csv_df_group = groups[country_code]

        template_data = []
        template_data_def = []

        out_path_csv = os.path.join(output_directory_path, country_code, '{}.csv'.format(template_table_name))
        out_path_sql = os.path.join(output_directory_path, country_code, '{}.sql'.format(template_table_name))

        if os.path.exists(out_path_csv) and os.path.isfile(out_path_csv) and os.path.exists(
                out_path_sql) and os.path.isfile(out_path_sql):
            print_progress('ALREADY_EXIST:%s and %s already exist' % (out_path_csv, out_path_sql))
            continue

        for i in range(len(template_csv_df_group)):

            current_row += 1

            flat_tmp_id = template_csv_df_group.iloc[i, 0]
            category = template_csv_df_group.iloc[i, 4]

            """ PROCESS TEMPLATE FILE """
            # all possible paths
            df = get_data_frame(
                os.path.join(template_directory_path, country_code, flat_file_placeholder.format(category, 'Template')),
                os.path.join(template_directory_path, country_code,
                             flat_file_placeholder.format('{}.{}'.format(category, country_code), 'Template')),
                os.path.join(template_directory_path, country_code,
                             flat_file_placeholder.format('{}.com'.format(category), 'Template')),
            )

            if df is None:
                continue

            row = get_template_data(df, category, country_code, flat_tmp_id)
            template_data.append(row)

            """ PROCESS TEMPLATE VALUES FILE """

            df = get_data_frame(
                os.path.join(template_directory_path, country_code,
                             flat_file_placeholder.format(category, 'DataDefinitions')),
                os.path.join(template_directory_path, country_code,
                             flat_file_placeholder.format('{}.{}'.format(category, country_code.lower()),
                                                          'DataDefinitions')),
                os.path.join(template_directory_path, country_code,
                             flat_file_placeholder.format('{}.com'.format(category), 'DataDefinitions')),
            )

            if df is None:
                continue

            df_val = get_data_frame(
                os.path.join(template_directory_path, country_code,
                             flat_file_placeholder.format(category, 'ValidValues')),
                os.path.join(template_directory_path, country_code,
                             flat_file_placeholder.format('{}.{}'.format(category, country_code.lower()),
                                                          'ValidValues')),
                os.path.join(template_directory_path, country_code,
                             flat_file_placeholder.format('{}.com'.format(category), 'ValidValues')),
            )

            if df_val is None:
                continue

            # print_progress(df_val)
            rows = get_template_definition(df, df_val, category, country_code, flat_tmp_id)
            template_data_def += rows

        if len(template_data) > 0:
            export(data=template_data,
                   csv_file=out_path_csv if create_csv else None,
                   sql_file=out_path_sql,
                   columns=['tpl_name', 'version', 'country', 'raw', 'imported_status', 'imported_by',
                            'imported_at',
                            'flat_tmpl_id', 'status'],
                   csv_index='id',
                   sql_index=None,
                   start_index=1,
                   table_name=template_table_name)

        out_path_csv = os.path.join(output_directory_path, country_code, '{}.csv'.format(template_values_table_name))
        out_path_sql = os.path.join(output_directory_path, country_code, '{}.sql'.format(template_values_table_name))

        if len(template_data_def) > 0:
            export(data=template_data_def,
                   csv_file=out_path_csv if create_csv else None,
                   sql_file=out_path_sql,
                   columns=['fields', 'labels', 'examples', 'definition', 'valid_values', 'tmpl_id', 'country',
                            'required', 'status', 'imported_by', 'imported_at'],
                   csv_index='id',
                   sql_index=None,
                   start_index=1,
                   table_name=template_values_table_name)

    end = datetime.now()

    write_logs(output_directory_path, start, end)
