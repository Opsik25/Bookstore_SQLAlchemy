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
    new_publisher = Publisher(id=json_publisher.get('pk'), **json_publisher.get('fields'))
    session.add(new_publisher)
    session.commit()


def create_book(json_book: dict):
    new_book = Book(id=json_book.get('pk'), **json_book.get('fields'))
    session.add(new_book)
    session.commit()


def create_shop(json_shop: dict):
    new_shop = Shop(id=json_shop.get('pk'), **json_shop.get('fields'))
    session.add(new_shop)
    session.commit()


def create_stock(json_stock: dict):
    new_stock = Stock(id=json_stock.get('pk'), **json_stock.get('fields'))
    session.add(new_stock)
    session.commit()


def create_sale(json_sale: dict):
    new_sale = Sale(id=json_sale.get('pk'), **json_sale.get('fields'))
    session.add(new_sale)
    session.commit()


def get_shops(publisher_name_id: str):
    query = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale)\
        .join(Stock.book)\
        .join(Sale)\
        .join(Shop)\
        .join(Publisher)
    if publisher_name_id.isdigit():
        query = query.filter(Publisher.id == int(publisher_name_id)).all()
    else:
        query = query.filter(Publisher.name == publisher_name_id).all()
    for book, shop, sale_price, sale_date_sale in query:
        print(f"{book: <40} | {shop: <10} | {sale_price: <8} | "
              f"{sale_date_sale.strftime('%d-%m-%Y')}")


settings = db_settings_parser()
DSN = (f"{settings['driver']}://{settings['login']}:{settings['password']}@"
       f"{settings['host']}:{settings['port']}/{settings['database']}")

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


if __name__ == '__main__':
    required_publisher = input('Введите название или идентификатор издательства: ')
    get_shops(required_publisher)
    session.close()
