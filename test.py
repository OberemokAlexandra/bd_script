
import urllib.request
# предварительно установила psycopg2 через командную строку
import psycopg2
# подключение к БД
conn = psycopg2.connect(database="logs", user="postgres",
    password="admin", host="localhost", port=5432)
cur = conn.cursor()
# подключение к файлу
response = urllib.request.urlopen("https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/nginx_logs/nginx_logs")
# считывание файла построчно
lines = response.readlines()
# список для хранения значений
values = []
for line in lines:
    # предобработка каждой строки файла, сбор нужных данных для таблицы. С регулярными выражениями возникли трудности, т.к. объекты типа byte
    # разбиваю строку по пробелам
    temp = line.split()
    # временные переменные для хранения частей строки
    b = [temp[3].decode("utf-8"), temp[4].decode("utf-8")]
    c = [temp[5].decode("utf-8"),temp[6].decode("utf-8"), temp[7].decode("utf-8")]
    d = [temp[8].decode("utf-8"), temp[9].decode("utf-8")]
    a = []
    # предыдущие части строки идентичны по шаблону. В "информации о системе" записи содержали разное количество пробелов.
    for i in range(11, len(temp)):
        a.append(temp[i].decode("utf-8"))
    # собираю все в кортеж
    val = (temp[0].decode("utf-8"), ' '.join(b)[1:-1],  ' '.join(c)[1:-1],  ' '.join(d), ' '.join(a)[1:-1] )
    # добавляю кортеж в список значений
    values.append(val)


# удаление таблицы, если она существует
cur.execute("DROP TABLE IF EXISTS l")
# создание таблицы
cur.execute("CREATE TABLE  l(ip text, time timestamp, request text, error_code text, system_info text)")
# структура sql запроса
sql = 'INSERT into l (ip, time, request, error_code, system_info) VALUES (%s, %s, %s, %s, %s)'
# выполнение множества запросов по вставке данных в таблицу
cur.executemany(sql, values)
conn.commit()
conn.close()
