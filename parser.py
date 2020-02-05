import os
from datetime import datetime
import math

import pandas as pd

logs = []

root = None


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


def export(data, csv_file, sql_file, columns, table_name, include_id_in_sql):
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    os.makedirs(os.path.dirname(sql_file), exist_ok=True)

    df = pd.DataFrame(data=data,
                      columns=columns)

    df.to_csv(csv_file, index=False)

    sql_columns = ','.join(['`{}`'.format(column) for column in (columns[0 if include_id_in_sql else 1:])])

    # create sql file
    text = "INSERT INTO `{}` ({}) VALUES\n".format(table_name, sql_columns)

    for i in range(len(data)):
        row = ','.join(['"{}"'.format(d) for d in data[i][1:]])

        if i == len(data) - 1:
            text += '({});'.format(row)
        else:
            text += '({}),'.format(row)

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

    log_text = 'OPERATION COMPLETED in {} with {} errors, {} empty files and {} missing files. Check logs.'.format(
        end_time - start_time, errors, empty_files, missing_files)

    logs.append(log_text)
    print('\n{}'.format(log_text), end='\n')

    out_logs = open('%s/logs.txt' % output_directory_path, 'w')
    out_logs.write('\n'.join(logs))
    out_logs.close()


def print_progress(text):
    logs.append(text)
    percent = current_row * 100.0 / total_rows
    percent_ceil = math.ceil(percent)
    print("\r|{}>{}| %.2f%% Done".format('=' * percent_ceil, '-' * (100 - percent_ceil)) % percent, end=' ')


def get_template_data(df_out, id, category, country_code, flat_tmp_id):
    # parsing head in key-value pairs
    kv_dict = {}
    for j in range(len(df_out.iloc[0])):
        kv = df_out.iloc[0, j]

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

    raw = df_out.to_csv(index=False, header=False, sep='\t')

    return [id, category, kv_dict['Version'], country_code, raw, 1, 1, 1, flat_tmp_id, 1]


def get_template_definition():
    # `id`, `fields`, `labels`, `examples`, `definition`, `valid_values`, `tmpl_id`, `country`, `required`, `status`, `imported_by`, `imported_at`
    pass


def parser(template_csv_file_path, template_directory_path, output_directory_path, flat_file_placeholder,
           template_table_name, template_values_table_name):
    template_csv_df = pd.read_csv(template_csv_file_path, header=None)

    global logs, total_rows, current_row
    current_row = 0

    logs = []

    start = datetime.now()

    template_data = []
    template_data_def = []

    total_rows = len(template_csv_df)

    # group by country code
    for country_code, template_csv_df_group in dict(tuple(template_csv_df.groupby(2))).items():

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

            row = get_template_data(df, i + 1, category, country_code, flat_tmp_id)
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
            # row = get_template_definition(df, df_val)
            # template_data_def.append(row)

            # break

    out_path_csv = os.path.join(output_directory_path, '{}.csv'.format(template_table_name))
    out_path_sql = os.path.join(output_directory_path, '{}.sql'.format(template_table_name))

    if len(template_data) > 0:
        export(data=template_data,
               csv_file=out_path_csv,
               sql_file=out_path_sql,
               columns=['id', 'tpl_name', 'version', 'country', 'raw', 'imported_status', 'imported_by', 'imported_at',
                        'flat_tmpl_id', 'status'],
               table_name=template_table_name,
               include_id_in_sql=False)

    out_path_csv = os.path.join(output_directory_path, '{}.csv'.format(template_data_def))
    out_path_sql = os.path.join(output_directory_path, '{}.sql'.format(template_data_def))

    if len(template_data_def) > 0:
        export(data=template_data_def,
               csv_file=out_path_csv,
               sql_file=out_path_sql,
               columns=['id', 'fields', 'labels', 'examples', 'definition', 'valid_values', 'tmpl_id', 'country',
                        'required', 'status', 'imported_by', 'imported_at'],
               table_name=template_values_table_name,
               include_id_in_sql=False)

    end = datetime.now()

    write_logs(output_directory_path, start, end)


"""
    
            if (os.path.exists(out_path_csv) and os.path.isfile(out_path_csv)) or (
                    os.path.exists(out_path_sql) and os.path.isfile(out_path_sql)):
                print_progress('ALREADY EXIST:%s|%s already exist' % (out_path_csv, out_path_sql), end='\n\n')
                logs.append('ALREADY EXIST:%s|%s already exist' % (out_path_csv, out_path_sql))
                logs.append('')
"""
