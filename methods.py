import psycopg2
from datetime import datetime
from pycbrf.toolbox import ExchangeRates
from itertools import zip_longest
from time import sleep
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SPREADSHEET_ID = '1TOgg44UslhS64YGIsnZiSbow9vg6BWwPNECQ9l3PQDw'
RANGE_NAME = 'Лист1'

# процедура возвращает курс доллара на необходимую дату
def curs_RUB(a):
    return ExchangeRates(datetime.now().strptime(a,"%d.%m.%Y"))['USD'].value

# процедура создает новую таблицу ORDERS и вносит в нее обновленные данные
def create_table(cur,conn,a):
    try:
        cur.execute("drop table orders")
        create_table_script='''CREATE TABLE orders(
                            id integer primary key,
                            orderID integer,
                            price_USD integer,
                            date varchar(16),
                            price_RUB float
                            ) '''
        cur.execute(create_table_script)
        aa = list(a)
        for i in aa:
            cur.execute("insert into orders values (%s,%s,%s,%s,%s)", [i[0],i[1],i[2],i[3],i[4]])
        conn.commit()
        print ("Создана таблица ORDERS для хранения обновленных данных. \nORDERS заполнен обновлеными данными. ")
    except Exception as ex:
        print ("При создании таблицы ORDERS произошел сбой: ", ex)
#процедура сравнивает 2 кортежа
def tuple_comparison(a,b):
    try:
        aa = list(a)# дополнительный список
        aaa = () # дополнительный кортеж
        for i in aa:
            i[0] = int (i[0])
            i[1] = int (i[1])
            i[2] = int (i[2])
            i[4] = float (i[4])
            j = tuple(i)
            aaa = aaa+(j,)
        for i, j in zip_longest(aaa,b):
            if not i or not j or i!=j:
                print("Обнаруженны изменения данных. \nБудет создана новая таблица с обновлеными данными")
                return 1
        print ("Данные не изменились")
    except Exception as ex:
        print("При сравнении данных произошел сбой " , ex )
# процедура подключается в бд и создает кортеж с данными из таблицы ORDERS
def work_with_date_db(a):
    try:
        conn = psycopg2.connect(dbname='db_test',user='postgres',password='1',host='localhost',port= '5432')
        cur = conn.cursor()
        cur.execute("select * from ORDERS")
        b = cur.fetchall()
        if tuple_comparison(a,b)==1:
            create_table(cur,conn,a)
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")
#процедура считывает файл с google диска
def read_file():
    print("Скачиваем файл с Google диска.")
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    try:
        service = build('sheets', 'v4', credentials=creds)
        result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME).execute()
        values = result.get('values', [])
        del values[0]
        if not values:
            print('Данных нет')
            return 0
        for i in values:
            for j in i:
                if j == '':
                    print ('Некорректно заполнена таблица. Ошибка в строке ', i[0])
                    return 0
        #добавляем дополнительный столбец с стоимостью товара в рублях
        for i in values:
            i[2] = int (i[2])
            doll_to_rub = i[2]*curs_RUB(i[3])
            doll_to_rub = float('{:.2f}'.format(doll_to_rub))
            i.append(doll_to_rub)
        values = tuple(values)
        print ("Файл успешно считан с Google диска")
        return values # возвращяем кортеж с данными из скаченного файла
    except ValueError as verr:
        print ("Некорректные данные", verr)
        return 0
    except HttpError as err:
        print( "Ошибка соединения",err)
        return 0
    except IndexError as ierr:
        print("Некорректные данные", ierr)
        return 0
#процедура Обратный отсчет
def countdown():
    aggr = 30
    for n in range(30):
        print('\033[F\033[K', end='')
        print("Скрипт запустится через: ", aggr - n, " сек.")
        sleep(1)
# основная функция
def run ():
    while True:
        values = read_file()
        if values==0:
            print ("Исправьте ошибку")
        else:
            work_with_date_db(values)
        countdown()
