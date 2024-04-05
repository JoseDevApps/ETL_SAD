"""
Codigo de extraccion de datos
"""

import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2

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

# Configuracion 
cur = conn.cursor()
cur.execute('SELECT * FROM \"IEC_parque_conf\"')
req_path = cur.fetchall()

#####################################
# Lectura del archivo temporal
#####################################
path = "./"
filename = "QII_temp1.feather"
source = path + "/" + filename
pathout = "./data/"
df = pd.read_feather(source)
print(df)
columnas = [
    "Aero",
    "time",
    "viento_avg",
    "viento_max",
    "viento_min",
    "p_avg",
    "p_max",
    "p_min",
    "p_teorica",
    "posicion",
]
Diccionario = {}
#####################################
# Separacion por AG
#####################################
listas = list(set(df["Aero"]))
aero = ["AG" + str(a) for a in listas]
data = zip(listas, aero)

for index, i in data:
    mask = df["Aero"] == index
    Diccionario[i] = df[mask]
    Diccionario[i].columns = columnas
    Diccionario[i].to_csv(pathout +i + ".csv")
    Diccionario[i].to_feather(pathout + i + ".feather")
