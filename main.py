import configparser
import json
import os
import sqlalchemy

from models import create_tables, Publisher, Book, Shop, Stock, Sale
from sqlalchemy.orm import sessionmaker


def db_settings_parser():
    config = configparser.ConfigParser()
    config.read('settings.ini')
    db_settings = {}
    driver = config['DB_connection']['driver']
    db_settings['driver'] = driver
    login = config['DB_connection']['login']
    db_settings['login'] = login
    password = config['DB_connection']['password']
    db_settings['password'] = password
    host = config['DB_connection']['host']
    db_settings['host'] = host
    port = config['DB_connection']['port']
    db_settings['port'] = port
    database = config['DB_connection']['database']
    db_settings['database'] = database
    return db_settings


def create_publisher(json_publisher: dict):
    new_publisher = Publisher(name=json_publisher['fields']['name'])
    session.add(new_publisher)
    session.commit()


def create_book(json_book: dict):
    new_book = Book(title=json_book['fields']['title'],
                    id_publisher=json_book['fields']['id_publisher'])
    session.add(new_book)
    session.commit()


def create_shop(json_shop: dict):
    new_shop = Shop(name=json_shop['fields']['name'])
    session.add(new_shop)
    session.commit()


def create_stock(json_stock: dict):
    new_stock = Stock(id_book=json_stock['fields']['id_book'],
                      id_shop=json_stock['fields']['id_shop'],
                      count=json_stock['fields']['count'])
    session.add(new_stock)
    session.commit()


def create_sale(json_sale: dict):
    new_sale = Sale(price=json_sale['fields']['price'],
                    date_sale=json_sale['fields']['date_sale'],
                    id_stock=json_sale['fields']['id_stock'],
                    count=json_sale['fields']['count'])
    session.add(new_sale)
    session.commit()


settings = db_settings_parser()
DSN = (f'{settings['driver']}://{settings['login']}:{settings['password']}@'
       f'{settings['host']}:{settings['port']}/{settings['database']}')

data_json_file_path = os.path.join(os.getcwd(), 'tests_data.json')
with open(data_json_file_path, encoding='UTF-8') as json_file:
    data_json = json.load(json_file)

engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

for data in data_json:
    if data['model'] == 'publisher':
        create_publisher(data)
    elif data['model'] == 'book':
        create_book(data)
    elif data['model'] == 'shop':
        create_shop(data)
    elif data['model'] == 'stock':
        create_stock(data)
    elif data['model'] == 'sale':
        create_sale(data)

publisher_name = input('Введите название издательства: ')

query = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).join(Stock.book)
query = query.join(Sale)
query = query.join(Shop)
query = query.join(Publisher).filter(Publisher.name == publisher_name)
for sale in query:
    for el in sale:
        print(el, end=' | ')
    print()

session.close()
