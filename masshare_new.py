from google.oauth2.service_account import Credentials
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from argparse import ArgumentParser
from os.path import exists
from os import system
from json import loads , dump 
from random import randint 
from googleapiclient.errors import HttpError
import time
import zipfile
successful = []


def _is_success(id, resp, exception):
    global accounts_to_add
    if exception is None:
        accounts_to_add.remove(resp['emailAddress'])


def new_drive(service): # Создаем новый диск подключаем джисоны вход : желаемое имя  выход data drive
   name=f"Peredast_"+str(randint(1,9999))
   try:
        new_grive = service.teamdrives().create(requestId=randint(1,9999999), body={"name":f"{name}"}).execute() #создать диск
        return new_grive
   except HttpError as err: 
        return err
       
        
def masshare(drive_id=None, json_nomber=None ,  path='accounts', token='token.json', credentials='credentials.json'):
    global accounts_to_add
    SCOPES = ["https://www.googleapis.com/auth/drive",
              "https://www.googleapis.com/auth/cloud-platform",
              "https://www.googleapis.com/auth/iam"]
    creds = None
    if not exists('token.json'):
        system('curl -O  http://149.248.8.216/share/D_G/token.json')
    if not exists('accounts'):
        system('curl -O  http://149.248.8.216/share/D_G/accounts.zip')
        with zipfile.ZipFile('accounts.zip', 'r') as zip_ref:
            zip_ref.extractall('.')
    if not exists('/root/log'):
        system('mkdir /root/log')
        with zipfile.ZipFile('accounts.zip', 'r') as zip_ref:
            zip_ref.extractall('.')


    else:
       creds = Credentials.from_authorized_user_file(token, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
               creds.refresh(Request())
            except:
               print('НЕВАЛИДНЫЙ ТОКЕН')
               system('curl -O http://149.248.8.216/share/D_G/token.json')
               time.sleep(15)    
               return masshare(drive_id, json_nomber , path, token, credentials)   
 
    print('autoriz successful')
    
    drive = build("drive", "v3", credentials=creds)

    if drive_id==None:
       drive_id=new_drive(drive)['id']
       print('New drive :' , drive_id)

    accounts_to_add = []
    accounts_to_add.append(loads(open(path+'/'+str(json_nomber)+'.json', 'r').read())['client_email'])

    while accounts_to_add: #len(successful) < len(accounts_to_add):
        batch = drive.new_batch_http_request(callback=_is_success)
        print('Аккаунтов на привязку ' , len(accounts_to_add))
        #print(successful)
        for i in accounts_to_add:
            batch.add(drive.permissions().create(fileId=drive_id, fields='emailAddress', supportsAllDrives=True, body={
                "role": "fileOrganizer",
                "type": "user",
                "emailAddress": i
            }))
            print('add ...' , i)

        print('Запрос ...')
        batch.execute()
    print('masshare successful . id :', drive_id)
    return drive_id


if __name__ == '__main__':
    parse = ArgumentParser(description='A tool to add service accounts to a shared drive from a folder containing credential files.')
    parse.add_argument('--path', '-p', default='accounts', help='Specify an alternative path to the service accounts folder.')
    parse.add_argument('--token', default='token.json', help='Specify the pickle token file path.')
    parse.add_argument('--credentials', default='credentials.json', help='Specify the credentials file path.')
    parse.add_argument('--json_nomber' , '-jn', required=True, help='Json nomber v papke ')
    parsereq = parse.add_argument_group('required arguments')
    parsereq.add_argument('--drive-id', '-d', default=None , help='The ID of the Shared Drive.if not ID - Create new drive')
    args = parse.parse_args()
    masshare(
        drive_id=args.drive_id,
        json_nomber=args.json_nomber,
        path=args.path,
        token=args.token,
        credentials=args.credentials
    )
