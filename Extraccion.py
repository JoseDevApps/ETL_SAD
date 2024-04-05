"""
Codigo de extraccion de datos
"""

import pandas as pd
from funciones import *

def Extraccion(path, filename, fileout):
    source = path + "/" + filename
    df = read_data_csv(source)
    #############################################
    #   Eliminando columnas sin datos
    #############################################
    df.dropna(inplace=True, axis=1)
    print(df.info())
    #############################################
    #   Corrigiendo el formato de los datos
    #############################################
    data_format = [
        int,  # 0
        int,
        pd.api.types.pandas_dtype("datetime64[ns]"),
        float,
        float,
        float,
        float,
        float,
        float,
        float,
        float,  # 10
        float,
        float,
        float,
        float,
        float,
        float,
        float,
        float,
        float,
        float,  # 20
        float,
        int,
    ]
    data = {
        df.columns[0]: data_format[0],
        df.columns[1]: data_format[1],
        df.columns[2]: data_format[2],
        df.columns[3]: data_format[3],
        df.columns[4]: data_format[4],
        df.columns[5]: data_format[5],
        df.columns[6]: data_format[6],
        df.columns[7]: data_format[7],
        df.columns[8]: data_format[8],
        df.columns[9]: data_format[9],
        df.columns[10]: data_format[10],
        df.columns[11]: data_format[11],
        df.columns[12]: data_format[12],
        df.columns[13]: data_format[13],
        df.columns[14]: data_format[14],
        df.columns[15]: data_format[15],
        df.columns[16]: data_format[16],
        df.columns[17]: data_format[17],
        df.columns[18]: data_format[18],
        df.columns[19]: data_format[19],
        df.columns[20]: data_format[20],
        df.columns[21]: data_format[21],
    }
    pattern = "\."
    pattern2 = "\,"
    print(df[' Viento mín. [m/s]'][0])
    # print(type(df[' Viento mín. [m/s]'][0]))
    # x=re.sub('\.',"", df[' Viento mín. [m/s]'][0])
    # print(x)
    # print(type(x))
    # df[' Viento mín. [m/s]'] = df[' Viento mín. [m/s]'].apply(lambda x: re.sub("\,","", x))
    # df[' Energía prod. [kWh]'].replace(pattern, "", inplace=True, regex=True)
    # df[' Energía prod. [kWh].1'].replace(pattern, "", inplace=True, regex=True)
    # df[' Viento mín. [m/s]'].replace(pattern, "", inplace=True)
    # # df.replace(pattern2, "", inplace=True, regex=True)
    # df = df.astype(data)
    # print(df.info())
    #############################################
    #   Exportando a formato Feather
    #############################################
    # fileout = 'clean_QI.feather'
    df.to_feather(fileout)
    return df