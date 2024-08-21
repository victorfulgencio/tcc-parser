import csv
import datetime
import os
import pandas as pd


DIR = "/mnt/c/Users/Victor/Documents/dev/tcc-ufsj/cobertura_movel"
NEW_BASE_DIR = "/mnt/c/Users/Victor/Documents/dev/tcc-ufsj/cobertura_movel_parquet"


def create_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def write_new_file(file_path, rows):
    already_exists = os.path.exists(file_path)

    with open(file_path, "a", newline='') as f:
        w = csv.DictWriter(f, rows[0].keys(), delimiter=";")
        if not already_exists:
            w.writeheader()
        w.writerows(rows)


def store_on_new_dir(code, rows):
    row = rows[0]
    directory = os.path.join(NEW_BASE_DIR, 'state=' + row['uf'] , 'city=' + row['municipio'])
    file_path = os.path.join(directory, code + '.csv')
    print('Writing row to', directory, 'at:', datetime.datetime.now())
    create_dir(directory)
    write_new_file(file_path, rows)


def add(row, data):
    code = row['codigo_setor_censitario']
    if code not in data:
        data[code] = [row]
    else:
        data[code].append(row)


def get_csv_data(file_name):
    data = dict()
    old_file_path = os.path.join(DIR, file_name)
    with open(old_file_path, newline='') as csvfile:
        spamreader = csv.DictReader(csvfile, delimiter=';')
        for row in spamreader:
            add(row, data)
    return pd.DataFrame.from_dict(data)


def read_file(file_name):
    
    for code, rows in data.items():
        store_on_new_dir(code, rows)


read_file('Cobertura_OI.csv')