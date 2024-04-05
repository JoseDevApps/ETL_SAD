"""
Script to generate a funciones to preprocessing and normalize data
"""

import pandas as pd
import os
import chardet
import csv


def find_delimiter(filename):
    """
    function for finding the delimiter
    """
    sniffer = csv.Sniffer()
    with open(filename) as fp:
        delimiter = sniffer.sniff(fp.read(5000)).delimiter
    return delimiter


def identify(file):
    """
    Function to identify the format of the file
    """
    file = ""
    if file.lower().endswith(".xlsx"):
        print("Excel file")
        file = "Excel"
    if file.lower().endswith(".csv"):
        print("CSV file")
        file = "CSV"
    if file.lower().endswith(".feather"):
        print("Feather file")
        file = "Feather"
    if file.lower().endswith(".xls"):
        print("Excel file 97 - 2003")
        file = "Excel97"
    return file


def list_files(path):
    """
    Function to list all the files inside a folder
    """
    files = os.listdir(path)
    # files = files
    return files


def encoding_identifyer(source):
    """
    Function to identify the encoding of the file
    """
    encoding = chardet.detect(source)
    print(encoding)
    return encoding["encoding"]


def read_data_csv(source, date_label = 'Hora'):
    """
    Function to read the CSV data
    """
    delimiter = find_delimiter(source)
    df = pd.read_csv(
        source,
        sep=delimiter,
        engine="python",
        quotechar='"',
        decimal=",",
        thousands=".",
        date_format="mixed",
        parse_dates=[date_label],
    )
    print(df.info())
    return df
