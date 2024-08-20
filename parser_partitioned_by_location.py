import csv
import datetime
import os
import pandas as pd


DIR = "/mnt/c/Users/Victor/Documents/dev/tcc-ufsj/cobertura_movel"
NEW_BASE_DIR = "/mnt/c/Users/Victor/Documents/dev/tcc-ufsj/cobertura_movel_parquet"


def create_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def write_new_file(file_path, row):
    already_exists = os.path.exists(file_path)

    with open(file_path, "a", newline='') as f:
        w = csv.DictWriter(f, row.keys(), delimiter=";")
        if not already_exists:
            w.writeheader()
        w.writerow(row)


def store_on_new_dir(row):
    directory = os.path.join(NEW_BASE_DIR, 'state=' + row['uf'] , 'city=' + row['municipio'])
    file_path = os.path.join(directory, row['codigo_setor_censitario'] + '.csv')
    print('Writing row to', directory, 'at:' ,datetime.datetime.now())
    create_dir(directory)
    write_new_file(file_path, row)


def read_file(file_name):
    old_file_path = os.path.join(DIR, file_name)
    with open(old_file_path, newline='') as csvfile:
        spamreader = csv.DictReader(csvfile, delimiter=';')
        for row in spamreader:
            store_on_new_dir(row)


read_file('Cobertura_TIM.csv')