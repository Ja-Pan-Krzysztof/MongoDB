from pymongo import MongoClient
from datetime import datetime
from pprint import pprint
from pandas import DataFrame
from bson.objectid import ObjectId
from PIL import Image
import logging, typing


class Datebase(object):
    URI = 'mongodb://localhost:27017'
    DATEBASE = None

    @staticmethod
    def initialize(collection: str):
        client = MongoClient(Datebase.URI)
        Datebase.DATEBASE = client[collection]

    @staticmethod
    def insert_one(collection: str, data):
        """
        Co należy podać do funkcji
        :param collection: str Nazwa kolekcji w mongoDB
        :param data: dist Co na co chcemy zamienić {'name':'foo'}, {'name':'haha}
        :return: 
        """
        Datebase.DATEBASE[collection].insert(data)

    @staticmethod
    def insert_some(collection: str, *data):
        Datebase.DATEBASE[collection].insert(data)

    @staticmethod
    def find(collection: str, query):
        return Datebase.DATEBASE[collection].find(query)

    @staticmethod
    def find_one(collection: str, query):
        return Datebase.DATEBASE[collection].find_one(query)

    @staticmethod
    def update_one(collection: str, update):
        return Datebase.DATEBASE[collection].update_one(update)

    @staticmethod
    def get_post(post_id: str):
        document = db.find_one('person', {'_id': ObjectId(post_id)})
        return document

    @staticmethod
    def few_things(name_things: str):
        list_things = []
        while True:
            add_things = str(input(f'Enter your {name_things} : '))
            if add_things == str(0):
                break
            else:
                list_things.append(add_things)
        return list_things

    @staticmethod
    def person_post():
        person_post = {
            'name': str(input('Enter your name : ')),
            'surname': str(input('Enter your surname : ')),
            'age': int(input('Enter your age : ')),
            'products': db.few_things('products'),
            'date': datetime.utcnow(),
            'price': float(input('Enter price of the products : '))
        }
        return person_post


db = Datebase()
db.initialize('market')
db.update_one('person', {'$set': {'price': 25.25}})






#pprint(db.find_one('person', {'age': 17, 'name': 'Adam'}))  # Zapytanie o 1 dokument
#for post in db.find('person', {'age': 17}):  # Zapytanie dla kilku zgodnych wartości
#    pprint(post)
#pprint(db.get_post('617d135db565ee9097c93e25'))  # Wyszukiwanie po ID
#pprint(db.find_one('products', {'price': 2.35}))
#Image.open('E:/Python/bazy danych/IMG/milk.png').show()
