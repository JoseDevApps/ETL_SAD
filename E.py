"""
Codigo de extraccion de datos
"""

import pandas as pd
import numpy as np
from funciones import *
import psycopg2
import os
from dotenv import load_dotenv
from funciones import *
from Extraccion import *
#############################################
#   Lectura de Base de datos
#############################################
# conectando a la base de datos
load_dotenv(dotenv_path="./db/.env")
USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB = os.getenv("POSTGRES_DB")
HOST = "localhost"
PORT = str(5555)
connection = {
    "dbname": DB,
    "user": USER,
    "password": PASSWORD,
    "port": PORT,
    "host": HOST,
}
conn = psycopg2.connect(**connection)
print(conn)
# Lectura del archivo de configuracion
# Planta QII
pcur = conn.cursor()
# Configuracion de los parques 
pcur.execute('SELECT * FROM "IEC_parque_conf_aerogeneradores";')
req_pc = pcur.fetchall()
# Configuracion de los Aerogeneradores Configuracion
cur = conn.cursor()
cur.execute('SELECT * FROM \"IEC_ag_conf\";')
req = cur.fetchall()
# Configuracion 
cur.execute('SELECT * FROM \"IEC_parque_conf\"')
req_path = cur.fetchall()
# Seleccion de datos por paque y aerogenerador
cur.execute('SELECT * FROM "IEC_ag_conf" t1 INNER JOIN "IEC_parque_conf_aerogeneradores" t2 ON t1.id = t2.ag_conf_id;')
req_pp = cur.fetchall()

# Extrayendo los datos por aerogenerador
i = len(req)
columnas = []
print(req_path)
##########################################
#   Filtrado por Parque
##########################################
# Uniendo en conjuto ID Parques y ID AG
for parque in req_path:
    # Filtrado por parque
    print(parque)
    res = [tup for tup in req_pp if tup[23]==parque[0]]
    print(res)
    # pass
    col = []
    for item in res:
        if item[21] == False:
            col = [
            item[2],
            item[3],
            item[4],
            item[5],
            item[6],
            item[7],
            item[8],
            item[9],
            item[10],
            item[11],
            item[12],
            item[13],
            item[14],
            item[15],
            item[16],
            item[17],
            item[18],
            item[19],]
            break
        
        columnas = [
        item[2],
        item[3],
        item[4],
        item[5],
        item[6],
        item[7],
        item[8],
        item[9],
        item[10],
        item[11],
        item[12],
        item[13],
        item[14],
        item[15],
        item[16],
        item[17],
        item[18],
        item[19],
        ]
        col= columnas+col
        print(col)
    columns = list(filter(None, col))
    date_column = res[0][18]
    path = parque[2]
    filename = list_files(path)[0]
    source = path+'/'+filename
    delimiter = find_delimiter(source)
    if parque[1]=='QII':
        df = pd.read_csv(
            source,
            sep=delimiter,
            engine='python',
            decimal=',',
            thousands='.',
            date_format="mixed",
            parse_dates=[date_column],
            usecols=columns
        )
    else:
        df = pd.read_csv(
            source,
            sep=delimiter,
            engine='python',
            date_format="mixed",
            parse_dates=[date_column],
            usecols=columns
        )
    print(df.columns)
    df.to_csv(parque[1]+'_temp1.csv')
    df.to_feather(parque[1]+'_temp1.feather')