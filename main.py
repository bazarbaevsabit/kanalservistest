<<<<<<< HEAD
=======
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from database import update_date_db, curs_RUB
from time import sleep



SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SAMPLE_SPREADSHEET_ID = '1TOgg44UslhS64YGIsnZiSbow9vg6BWwPNECQ9l3PQDw'
SAMPLE_RANGE_NAME = 'Лист1!A2:D51'
#процедура считывает файл с google диска
def read_file():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    try:
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                    range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])

        if not values:
            print('Данных нет')
            return 0
        for i in values:
            for j in i:
                if j == '':
                    print ('Некорректно заполнена таблица в Google документе в строке ', i[0])
                    return 0
        #добавляем дополнительный столбец с стоимостью товара в рублях
        for i in values:
            i[2] = int (i[2])
            i.append(i[2]*curs_RUB(i[3]))
        values = tuple(values)

        print ("Файл успешно считан с Google диска")
        return values
    except ValueError as verr:
        print ("Некорректные данные", verr)
        return 0
    except HttpError as err:
        print( "Ошибка соединения",err)
        return 0

def run ():

    while True:
        value = read_file()
        if value==0:
            print ("Исправьте ошибку")
        else:
            update_date_db(read_file())
        sleep(30)
>>>>>>> ae94447a6dd7e53942b777ceb9819015ef414ff2

from methods import run
if __name__ == '__main__':
    run()
