#!C:\Python_3.10\bazy danych\Scripts\python
from pymongo import MongoClient
from bson.objectid import ObjectId
from pandas import DataFrame
import pymongo
import numpy
import random
from pprint import pprint

URI = 'mongodb://localhost:27017'

client = MongoClient(URI)
db = client['test_no_oop']  # Odwołanie się do bazy danych


def insert_one(collection: str, post: dict, *args):
    """Dodawanie pojedyńczego elementu.

    :param collection: Nazwa kolekcji, do którech chcemy dodać dane.
    :param post: Dane które chcemy przekazać jako dokument.
    :param args: Sprawdzanie poprawności schematu podczas wstawiania
    :return: Dodaje post do Bazy Danych
    """
    table = db[collection]
    table.insert_one(post, args)
    # insert_one('person', {
    #    '_id': 11,
    #    'name': 'Oskar',
    #    'surname': 'Inny',
    #    'age': 31
    # })


def insert_many(collection: str, posts: list):
    table = db[collection]
    for post in posts:
        table.insert_one(post)
    # insert_many('person', [
    #    {'_id': 9, 'name': 'Michał', 'surname': 'Płaski', 'age': 16},
    #    {'_id': 10, 'name': 'Adam', 'surname': 'Kamienisty', 'age': 23}
    # ])


def delete_one(collection: str, _filetr, *args):
    table = db[collection]
    table.delete_one(_filetr, args)


def find(collection: str, query, column=None):
    table = db[collection]
    if column is None:
        for result in table.find(query):
            print(result)
    else:
        for result in table.find(query):
            a = 0
            wynik = []
            for _ in column:
                wynik.append(result[column[a]])
                a += 1
            print(wynik)
    #find('person', {
    #    'name': 'Adam'
    #}, column=['name', 'surname', '_id'])


def find_one(collection: str, query: dict, column: list = None):
    table = db[collection]
    if column is None:
        return table.find_one(query)
    else:
        result = table.find_one(query)
        a = 0
        wynik = []
        for _ in column:
            wynik.append(result[column[a]])
            a += 1
        return wynik
    # print(find_one(
    #    'person',
    #    {'name': 'Adam'},
    #    column=['name', 'age', 'surname']
    # ))


def find_dataframe(collection: str, query: dict, column: list = None):
    table = db[collection]
    if column is None:
        data = table.find(query)
        return DataFrame(data)
    else:
        for result in table.find(query):
            a = 0
            wynik = []
            for _ in column:
                wynik.append(result[column[a]])
                a += 1
            print(wynik)
            

print(find_dataframe('person', {'age': {'$ne': 18}}, column=['name', 'age']))





