import csv
import mysql.connector
import sys
import datetime

if(len(sys.argv) < 2):
    print(sys.argv)
    raise Exception('Filepath needed')


CSV_FILE_PATH = sys.argv[1]
DB_PS = 'cruzeiro'
DB_HOST = 'localhost'
DB_USER = 'root'
DB_NAME = 'tcc'

query_add_cobertura = ("INSERT IGNORE INTO tcc.cobertura "
                       "(ano, Operadora, Tecnologia, Setor_Censitario, Codigo_Municipio, Municipio, UF, Regiao, Domicilios, Moradores, Bairro, Tipo_Setor, Percentual_Cobertura) "
                       "VALUES (%(\ufeffAno)s, %(Operadora)s, %(Tecnologia)s, %(Código Setor Censitário)s, %(Código Município)s, %(Município)s, %(UF)s, %(Região)s, %(Domicílios)s, %(Moradores)s, %(Bairro)s, %(Tipo Setor)s ,%(Percentual Cobertura)s)")


def db_connect_and(execute_action):
    with mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PS) as connection:
        if connection.is_connected():
            print("MySQL connection started")
            with connection.cursor() as cursor:
                execute_action(connection, cursor, CSV_FILE_PATH)


def read_file_to_store_on_db(db_con, db_cursor, file_path):
    with open(file_path, newline='') as csvfile:
        spamreader = csv.DictReader(csvfile, delimiter=';')
        count = 0
        for row in spamreader:
            db_cursor.execute(query_add_cobertura, row)
            if(count == 2000):
                print('Commiting query', datetime.datetime.now())
                db_con.commit()
                count = 0
            else:
                count = count + 1
        db_con.commit()


db_connect_and(read_file_to_store_on_db)
