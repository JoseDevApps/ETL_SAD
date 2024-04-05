"""
Codigo de Carga de datos
"""

import pandas as pd
import numpy as np
from funciones import *
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
#############################################
#   Conexion a la Base de datos
#############################################
load_dotenv(dotenv_path="./db/.env")
USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB = os.getenv("POSTGRES_DB")
HOST = os.getenv("POSTGRES_HOST")
PORT = str(os.getenv("POSTGRES_PORT"))
connection = {
    "dbname": DB,
    "user": USER,
    "password": PASSWORD,
    "port": PORT,
    "host": HOST,
}
conn = psycopg2.connect(**connection)
print(conn)
#############################################
#   Carga de datos a la tabla 
#############################################
# consulta del aerogenerador y Parque eolico
cur = conn.cursor()
cur.execute('SELECT * FROM "IEC_ag_conf" t1 INNER JOIN "IEC_parque_conf_aerogeneradores" t2 ON t1.id = t2.ag_conf_id;')
req_pp = cur.fetchall()

cur.execute('SELECT ID FROM \"IEC_parque_conf\" WHERE NOMBRE = \'QII\'')
request =cur.fetchall()

res = [tup for tup in req_pp if tup[23]==request[0][0]]
print(res)

#####################################
# Lectura del archivo temporal
#####################################
# path = "./data/"
# filename = "AG_temp1.feather"
# source = path + "/" + filename
# df = pd.read_feather(source)

# Consulta por parque
columnas = [
    "ag_ID_id",
    "time",
    "viento_avg",
    "viento_max",
    "viento_min",
    "p_avg",
    "p_max",
    "p_min",
    "pteo_avg",
    "posicion",
]
##############################################
#   SQLALCHEMY
##############################################

engine = create_engine("postgresql+psycopg2://"+USER+":"+PASSWORD+"@"+HOST+":"+PORT+"/"+DB)
path = "./data/"
for a in res:
    cur.execute('SELECT ID FROM \"IEC_aerogenerador_st\"')
    id_data =cur.fetchall()
    if len(id_data)==0:
        id_max = 0
    else:   
        id_max=max(id_data)[0]
    print(a) # aerogeneradores
    print('///')
    filename = a[1]+".feather"
    source = path + "/" + filename
    df = pd.read_feather(source)
    df.columns = columnas
    df['viento_std']=0
    df['p_std']=0
    df['posicion_min']=0
    df['posicion_max']=0
    df['posicion_std']=0
    df['pteo_min']=0
    df['pteo_max']=0
    df['pteo_std']=0
    df['ag_ID_id'] = a[0]
    if id_max==0:
        df['id']=df.index
    else:
        df['id']=np.arange(id_max,id_max+df.shape[0])
    print(df)
    df.to_sql('IEC_aerogenerador_st', engine,if_exists='append',index=False)