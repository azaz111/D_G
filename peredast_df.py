from msql_reqwert import  get_one_false , sets_stat , sets_false_token , sets_ok
from masshare_new import masshare
from gdrive_respons import *
import configparser
import os , re
from time import sleep , time
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError
from requests import get
try:
   ip_address = get('http://ipinfo.io/json').json()['ip']
except:
   ip_address="00.00.00.000"
try:
   from sshtunnel import SSHTunnelForwarder
   import apprise
   from loguru import logger
except:
   os.system('pip install sshtunnel')
   os.system('pip install apprise')
   os.system('pip install loguru')
   from sshtunnel import SSHTunnelForwarder
   import apprise
   from loguru import logger

apobj = apprise.Apprise()
apobj.add('tgram://5035704615:AAE7XGex57LYUN23CxT2T67yNCknzgyy7tQ/183787479')
logger.add('logger_beckup.log', format="{time} - {level} - {message}")
n=0
tabl='dbox_peredast'
full_speed=0
baza_pid={}
#token_read=open("osnova_token.txt", 'r').read()[:-1]

#@logger.catch
#def ls_dbox(sektor):
#   full_plot=[]
#   com=f'rclone ls dbox_{sektor}:'
#   comls= com.split(' ')
#   process = subprocess.Popen(comls, stdout=subprocess.PIPE)
#   process.wait()
#   plots=process.communicate()[0].decode('utf-8').split('\n')[:-1]
#   for x in plots:
#      full_plot.append(x[13:])
#   return full_plot
   

#@logger.catch
def drive_new_config(sektor): # Подготовка конфигураций 
   global list_transfer
   try:
      d_tokens=get_one_false()  # Получили все данные с базы
   except:
      sleep(30)
      d_tokens=None

   if d_tokens == 'not found':
      logger.info('Не осталось фалов для передачи')
      return
   
   if d_tokens:
      #print(d_tokens)
      
      #d_tokens['dbox_token'],d_tokens['plot'],d_tokens['parents'],d_tokens['jsone_nomber']

      team_drive=masshare(json_nomber=d_tokens['jsone_nomber'])
      d_tokens['team_drive']=team_drive
      config = configparser.ConfigParser()
      config[f'dbox_{sektor}'] = {'type' : 'dropbox','token': f'{d_tokens["dbox_token"]}'}
      goog_token=str(d_tokens['google_token']).replace("'",'"')
      config[f'drive_{sektor}'] = {'type': 'drive', 'token': goog_token,  'team_drive': team_drive}
      
      with open('/root/.config/rclone/rclone.conf', 'a') as f:
         config.write(f)

      logger.debug(f"config sozdan")


      return d_tokens

   else:
      print(" Нет доступных фалов или доступа к базе ")
      logger.error(f"🚨 Нет доступных фалов или доступа к базе  ")
      apobj.notify(body=f"[{ip_address}]⚠️🚨 Нет доступных фалов или доступа к базе ")
      sleep(120)
      return drive_new_config(sektor)

#@logger.catch
def stat_progect(ip_ser , work ): # передача с помощью суб процесса
   global full_speed
   global baza_pid
   if chek_ref() != 'НЕВАЛИДНЫЙ ТОКЕН':
      print('ТОКЕН ВАЛИДНЫЙ')
   else : 
      logger.error(f"НЕВАЛИДНЫЙ ТОКЕН {work} {ip_ser}")
      apobj.notify(body=f"[{ip_ser}]⚠️ НЕВАЛИДНЫЙ ТОКЕН ")
      sleep(25)
      try:
         os.system('curl -O http://149.248.8.216/share/D_G/token.json')
      except :
         print('nevihodit')
      return stat_progect( ip_ser , work )

   logger.debug(f"Старт потока {work} {ip_ser}")
   #input('sdfe')
   try:
      some_date = datetime.now()
      start_time= time()
      # Формируем токены и файл для передачи 
      data_drive=drive_new_config(work)
      if data_drive:
         # Формируем Команду 
         com=f'rclone copy dbox_{work}:{data_drive["plot"]} drive_{work}: --drive-stop-on-upload-limit --transfers 1 -P --drive-service-account-file accounts/{data_drive["jsone_nomber"]}.json -v --log-file /root/log/rclone.log'
         print(com)
         comls= com.split(' ')
         process = subprocess.Popen(comls, stdout=subprocess.PIPE, universal_newlines=True)
         print( str(process.pid) )
         pid= str(process.pid)
         logger.info(f'[{(process.pid)}] Start ')
         x=0
         er='None'
         speed_value=0
         progress='not found'

         while True:
             line = str(process.stdout.readline())
             #print('line', line)
             if not line:
                print('Завершено')
                er='OK'

             match2 = re.search(r',\s*(\d+)%\s*,', line)
             if match2:
                 progress = match2.group(1)

             # Ищем число с плавающей точкой, за которым следует "MiB/s"
             match = re.search(r'\d+\.\d+\sMiB/s', line)
             if match:
                 # Получаем совпадение
                 speed = match.group(0)
                 # Извлекаем значение числа с плавающей точкой из совпадения
                 speed_value = float(re.search(r'\d+\.\d+', speed).group(0))

             x+=1
             if x == 200:
                 #print('line', line)
                 now = datetime.now() + timedelta(minutes=480)
                 baza_pid[pid]=speed_value
                 total_sum = sum(baza_pid.values())
                 print(ip_ser,f' | {pid} | Potok : {work} | Progress : {progress} % | Speed_potok : {speed_value} | Total_speed : {int(total_sum)}')
                 x=0
             elif er!='None':
                 break
             
         if pid in baza_pid:
            del baza_pid[pid]

         now_date = datetime.now()
         a=now_date - some_date
         logger.info(f'[{(process.pid)}] Время выполнения {timedelta(seconds=a.seconds)} PEREDAN : {data_drive["plot"]}')
         #reqest_sql_ok(data_drive[3])
         #if time() - start_time > 2000:
            # Проверим по логу передан или нет
         logger.info(f' Проверка по логу  ') 
         with open('/root/log/rclone.log', 'r') as f:
            for line in f:
                if f'{data_drive["plot"]}: Copied (new)' in line:
                    logger.info(f'confirm plots {data_drive["plot"]}')


         # Переносим На шаре и удаляем с базы 
         apobj.notify(body=f'[{ip_ser}]✅ Передан 🕰️ Время: {timedelta(seconds=a.seconds)} plot : {data_drive["plot"]} ') 

   except Exception as err: 
      apobj.notify(body=f'🚨[{ip_ser}] Ошибка {err}')
      logger.error(f"🚨[{ip_ser}] Ошибка {err}")
   

def main(): 
   potok = os.getenv('POTOK')
   if not potok:
      potok=1
   print(potok)
   

   from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
   all_task=[]

   executor =ThreadPoolExecutor(max_workers=int(potok))

   for x in range(1,10000):
      executor.submit(stat_progect,ip_address,x)
      sleep(5)

   #stat_progect(ip_address,1)

   #wait(all_task, return_when=ALL_COMPLETED)

if __name__ == '__main__':
   #drive_new_config(5)
   main()



   # Качаем актуальные джисоны 
   #try:
   #   os.remove('/root/.config/rclone/rclone.conf')
   #except:
   #   pass



#try:
#    sets_stat(ip_ser, data_drive['team_drive'] ,int(time()), f' Work : {tr} | peredano : {pr} | time_wok {ti}')
#except Exception as err: 
#    apobj.notify(body=f'🚨test:[{ip_ser}] Ошибка {err}')
#    logger.error(f"🚨[{ip_ser}] Ошибка {err}")