import pymysql , time
from sshtunnel import SSHTunnelForwarder

tabl='dbox_peredast'
tabl3='dbox_peredast_stat'

server = SSHTunnelForwarder(
    ('149.248.8.216', 22),
    ssh_username='root',
    ssh_password='XUVLWMX5TEGDCHDU',
    remote_bind_address=('127.0.0.1', 3306)
)

# Cоздаем подключение !  
def _getConnection(): 
    server.start()
    # Вы можете изменить параметры соединения.
    connection = pymysql.connect(host='127.0.0.1', port=server.local_bind_port, user='chai_cred',
                      password='Q12w3e4003r!', database='credentals',
                      cursorclass=pymysql.cursors.DictCursor)
    return connection



def create_table_stat():
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {tabl3} (
                                    id int PRIMARY KEY AUTO_INCREMENT,
                                    ip_a VARCHAR(40) NOT NULL UNIQUE,
                                    time_update INT DEFAULT 0,
                                    data TEXT DEFAULT ("None"))""")


# Установить значение 
def sets_stat(ip_a,time_update,data:str):
    with _getConnection() as db:
        cursor = db.cursor()
        #cursor.execute('UPDATE %s SET data = "%s" WHERE ip_a = "%s"' %(tabl3,data,ip_a,))
        cursor.execute(f"""INSERT INTO {tabl3} (ip_a,time_update,data) 
                                               VALUES ("{ip_a}",{time_update},"{data}")
                                               ON DUPLICATE KEY UPDATE time_update={time_update},data="{data}" """ )
        db.commit()
        print("OK")
        

# Получить все данные 
def get_all():
    with _getConnection() as db:
        cursor = db.cursor()
        #cur.execute( f"SELECT * FROM {tabl} WHERE status = 'False' " ) # запросим все данные  
        cursor.execute( f"SELECT * FROM {tabl}" ) # запросим все данные  
        str_ok = cursor.fetchall()
        for str_okv in str_ok:
           print(str_okv)
        db.commit()



# Получить один файл установить ВОРК
def get_one_false():
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute(f'SELECT * FROM {tabl} WHERE status_work = "False" ') 
        str_ok = cursor.fetchall()[0]
        print(str_ok)
        if len(str_ok)==0: 
            print('Нет фалов')
            return 'not found'
        
        try:
            #print(str_ok[0][0])
            cursor.execute(f'UPDATE {tabl} SET status_work = "Work" , time_work = {int(time.time())} WHERE id = "{str_ok["id"]}"')
            db.commit()
        except IndexError :
            return None
    return str_ok
#get_one_false()  
 

# Установить True   
def sets_true(id_t):
    with _getConnection() as db:
        cursor = db.cursor()
        cursor.execute('UPDATE %s SET status = "%s" WHERE id = %s' %(tabl,"True",id_t,))
        db.commit()
        print("OK")

create_table_stat()
