from msql_reqwert import  get_one_false , sets_stat , sets_true
from masshare_new import masshare
from gdrive_respons import *
import configparser
import os , re
from time import sleep , time
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from requests import get
from concurrent.futures import ThreadPoolExecutor
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

folder_token='D_G'
apobj = apprise.Apprise()
apobj.add('tgram://5035704615:AAE7XGex57LYUN23CxT2T67yNCknzgyy7tQ/183787479')
logger.add('logger_beckup.log', format="{time} - {level} - {message}")
n=0
tabl='dbox_peredast'
full_speed=0
baza_pid={}

#@logger.catch
def drive_new_config(sektor): # Подготовка конфигураций 
   global list_transfer
   try:
      d_tokens=get_one_false()  # Получили все данные с базы
   except:
      sleep(30)
      d_tokens=None

   if d_tokens == 'not found':
      logger.info('Not files transfer')
      return
   
   if d_tokens:
      team_drive=masshare(json_nomber=d_tokens['jsone_nomber'],pap_share=folder_token)
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
   print("stat_progect")
   stat=chek_ref()
   if stat != 'nevalid':
      print('Token Ok')

   elif stat=='not json':
      try:
         os.system(f'curl -O http://149.248.8.216/share/{folder_token}/token.json')
      except :
         print('nevihodit')
   else : 
      logger.error(f"NEVALID TOKEN {work} {ip_ser}")
      apobj.notify(body=f"[{ip_ser}]⚠️ NEVALID TOKEN ")
      sleep(25)
      try:
         os.system(f'curl -O http://149.248.8.216/share/{folder_token}/token.json')
      except :
         print('nevihodit')
      return stat_progect( ip_ser , work )

   logger.debug(f"Start potoka {work} {ip_ser}")
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
                print('Completed')
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
             if x == 800:
                 #print('line', line)
                 now = datetime.now() + timedelta(minutes=480)
                 baza_pid[pid]=speed_value
                 total_sum = sum(baza_pid.values())
                 print(ip_ser,f' | {pid} | Potok : {work} | Progress : {progress} % | Speed_potok : {speed_value} | Total_speed : {int(total_sum)}')
                 try:
                     sets_stat(ip_ser ,int(time()), f' Potok_work {len(baza_pid)} | Total_speed : {int(total_sum)}')
                 except Exception as err: 
                     apobj.notify(body=f'🚨STATISIK SEND:[{ip_ser}] ERROR: {err}')
                     logger.error(f'🚨STATISIK SEND:[{ip_ser}] ERROR: {err}')
                 x=0
             elif er!='None':
                 break
             
         if pid in baza_pid:
            del baza_pid[pid]

         now_date = datetime.now()
         a=now_date - some_date
         logger.info(f'[{(process.pid)}] Time_work {timedelta(seconds=a.seconds)} PEREDAN : {data_drive["plot"]}')
         #reqest_sql_ok(data_drive[3])
         tverda="NO"

         # Проверим по логу передан или нет
         logger.info(f' Get log  ') 
         with open('/root/log/rclone.log', 'r') as f:
            for line in f:
                if f'{data_drive["plot"]}: Copied (new)' in line:
                     logger.info(f'Confirm plots : {data_drive["plot"]} for drive : {data_drive["team_drive"]}')
                     tverda='YES'
                     try:
                         sets_true(data_drive["plot"],data_drive["team_drive"])
                     except Exception as err: 
                         apobj.notify(body=f'🚨STATUS SEND:[{ip_ser}] ERROR: {err}')
                         logger.error(f'🚨STATUS SEND:[{ip_ser}] ERROR: {err}')
                        


         # Переносим На шаре и удаляем с базы 
         apobj.notify(body=f'[{ip_ser}]✅ Передан 🕰️ Время: {timedelta(seconds=a.seconds)} plot: {data_drive["plot"]} \nПодтверждение {str(tverda)} drive : {data_drive["team_drive"]}') 

   except Exception as err: 
      apobj.notify(body=f'🚨[{ip_ser}] Ошибка {err}')
      logger.error(f"🚨[{ip_ser}] Ошибка {err}")
   

def main(): 
   global folder_token
   potok = os.getenv('POTOK')
   if not potok:
      potok=1
   print(potok)
   
   folder_token = os.getenv('FOLDER_TOKEN')
   if not folder_token:
      folder_token='D_G'
   try:
      os.system(f'curl -O http://149.248.8.216/share/{folder_token}/token.json')
   except :
      print('nevihodit')
   
   with open('token.json' , 'r') as ff:
      if 'Not Found' in ff.read():
         print('TOKEN NE ZAGRUCHEN')
         apobj.notify(body=f"[{ip_address}]⚠️🚨 TOKEN NE ZAGRUCHEN | find folder: {folder_token} ")
         sleep(100)
         return main()
         

   executor =ThreadPoolExecutor(max_workers=int(potok))
   for x in range(1,10000):
      executor.submit(stat_progect,ip_address,x)
      sleep(5)

   #stat_progect(ip_address,1)
#export FOLDER_TOKEN=D_G2
#export POTOK=5

if __name__ == '__main__':
   main()
