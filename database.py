import psycopg2
from datetime import datetime
from pycbrf.toolbox import ExchangeRates

# процедура возвращает курс доллара на необходимую дату
def curs_RUB(a):
    date_in_table = a[6]+a[7]+a[8]+a[9]+"-"+a[3]+a[4]+"-"+a[0]+a[1]
    return ExchangeRates(datetime.now().strftime(date_in_table))['USD'].value

# процедура создает новую таблицу и вносит в нее обновленные данные
def create_table(cur,conn,a):
    table_name = "orders" + datetime.now().strftime("%d%m%Y_%H%M%S")
    cur.execute( "alter table orders rename to %s" % table_name)
    script_='''CREATE TABLE orders(
                        id integer primary key,
                        orderID integer,
                        price_USD integer,
                        date varchar(16),
                        price_RUB numeric
                        ) '''
    cur.execute(script_)
    aa = list(a)
    for i in aa:
        cur.execute("insert into orders values (%s,%s,%s,%s,%s)", [i[0],i[1],i[2],i[3],i[4]])
    conn.commit()

#процедура сравнивает 2 таблицы
def tuple_comparison(a,b):

    aa = list(a)
    aaa = ()
    for i in aa:
        i[0] = int (i[0])
        i[1] = int (i[1])
        i[2] = int (i[2])
        j = tuple(i)
        aaa = aaa+(j,)
    for i, j in zip(aaa,b):
        print (i,j)
        if i != j:
            print("Таблицы неравны\n"
            "Будет создана новая таблица с обновлеными данными")
            return 1
        else:
            print("Таблицы равны")

# процедура подключается в бд и создает кортеж с данными из таблицы ORDERS
def update_date_db(a):
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
