from init_photo_service import service
import pandas as pd
import pickle
import requests
import os
import pytz
import argparse
import json
from datetime import datetime, date
import time
import glob
import psycopg2
import sys
from pathlib import Path

"""
The reason I have worked on this project so that the person whos using this application can seamlessly work 
and could upload the images/files at a certain fixed time which would create a backup as well. 
The log files are also maintained so that the person knows that which files have been transferred and if there
wsa any failure what was the reason of it.

"""

"""
To run this code you would need to have python3 along with the google photos api libraries installed as this project
is still under development mode. You would need to run it from the terminal and would need to pass the location of the
input (where your images are present) and output on where you want the logs to be maintained

python classTransfer.py --input Source --output /Users/abhinntrivedi/Desktop/Google/Code/Logs 

"""

# Establishing connection with the postgres database
class Connection:
    def createConnection(self):
        try:
            conn = psycopg2.connect(
            dbname = "google",
            user = "postgres",
            host = "0.0.0.0",
            password = "1234",
            port = "5432"
            )
            cur = conn.cursor()
            return conn, cur
        except Exception:
            print('Oopss cannot connect to database')
            print('Please check the server is running')
            sys.exit()

# Fetching todays time and date
class Today:
    def getDate(self):
        try:
            today = date.today()
            copied = today.strftime("%a %d %B %y")
            tz_TX = pytz.timezone('America/Chicago')
            datetime_TX = datetime.now(tz_TX)
            return copied, datetime_TX
        except Exception:
            print('There was a system error to fetch date or CDT')
            sys.exit()

# Taking arguments from the user 
class Arguments:
    def makeArguments(self):
        try:
            ap = argparse.ArgumentParser()
            ap.add_argument("-i", "--input", required=True,
            help="path to source file")
            ap.add_argument("-o", "--output", required=True,
            help="path to log file")
            args = vars(ap.parse_args())
            source_dir = args["input"]
            out_dir = args["output"]
            return source_dir, out_dir
        except Exception:
            print('There was an error in the arguments')

# Fetching albums from the users personalized albums
class Albums:
    def FetchAlbums(self):
        try:    
            response = service.albums().list(
                pageSize=50,
                excludeNonAppCreatedData = False
            ).execute()
            lstAlbums = response.get('albums')
            nextPageToken = response.get('nextPageToken')

            while nextPageToken:
                response = service.albums().list(
                    pageSize = 50,
                    excludeNonAppCreatedData = False,
                    pageToken = nextPageToken
                )
                lstAlbums.append(response.get('albums'))
                nextPageToken = response.get('nextPageToken')
            df_albums = pd.DataFrame(lstAlbums)
            return df_albums
        except Exception:
            print('Cannot fetch albums from Google photos')
    
    def CheckAlbum(self, df_albums, name):
        count = 0
        small_df = df_albums[['id', 'title']]
        for index in small_df['title']:
            if index == name:
                fetched_id = small_df['id'][count]
                return True, fetched_id
        count += 1
        return False, '0'
    
    def MakeAlbum(self, name):
        try:
            request_body = {
                'album': {'title': name}
            }
            response_album = service.albums().create(body=request_body).execute()
            # Fetching the id of the album
            album_id = response_album.get('id')
            return album_id
        except Exception:
            print('Cannot create a new album')

# Converting the format of the date
class Date:
    def convert_date(self, timestamp):
        d = datetime.utcfromtimestamp(timestamp)
        formated_date = d.strftime('%d %b %Y')
        return formated_date

# Working with the images
class Images:
    def UploadImages(self, dir, todayDate, todayTime, album_id, cur):
        try:
            tokens = []
            token = pickle.load(open('token_photoslibrary_v1.pickle', 'rb'))
            os.chdir(dir)
            flag = 0
            source_dir = dir
            date = Date()
            upload = Images()
            path = Path(__file__).parent.absolute()
            with os.scandir() as source_dir:
                for entry in source_dir:
                    info = entry.stat()
                    created = time.ctime(os.path.getctime(entry))
                    image_file = (entry.name)
                    LastModified = date.convert_date(info.st_mtime)
                    DateCopied = todayDate
                    DateCreated =  created
                    fileId = image_file+str(path)+LastModified+DateCopied+DateCreated
                    cur.execute("SELECT EXISTS(SELECT 1 FROM photosdata WHERE fileId = '"+fileId+"')")
                    #cur.execute("SELECT EXISTS(SELECT 1 FROM photos WHERE name = '"+image_file+"' AND filesize = '"+str(FileSize)+"' AND lastmodified = '"+LastModified+"')")
                    result = cur.fetchone()
                    if result==(True,):
                        continue
                    else:
                        flag = 1
                        print(image_file)
                        response = upload.upload_image(image_file, os.path.basename(image_file), token)
                        tokens.append(response.content.decode('utf-8'))
            return tokens, flag

        except Exception:
            print('Cannot upload images')

    def upload_image(self, image_path, upload_file_name, token):
        try:
            
            upload_url = 'https://photoslibrary.googleapis.com/v1/uploads'
            headers = {
                'Authorization': 'Bearer ' + token.token,
                'Content-type': 'application/octet-stream',
                'X-Goog-Upload-Protocol': 'raw',
                'X-Goog-Upload-File-Name': upload_file_name
            }
            img = open(image_path, 'rb').read()
            response = requests.post(upload_url, data=img, headers=headers)
            print(response)
            # print('\nUpload token: {0}'.format(response.content.decode('utf-8')))
            return response
        except Exception:
            print('Cannot upload this image :', upload_file_name)

# Storing the values into the postgres and making a log file
class Store:
    def makeFiles(self, dir,todayDate, todayTime, cur, conn, out_dir):
        try:
            source_dir = dir
            #os.chdir(dir)
            date = Date()
            rasto = Path(__file__).parent.absolute()
            path = str(rasto)
            data = {'files': []}
            postgres_insert_query = """INSERT INTO photosdata (fileId, Name, Path, LastModified, DateCopied, FileSize, ChicagoTime, DateCreated) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""" 
            with os.scandir() as source_dir:
                for entry in source_dir:
                    info = entry.stat()
                    file_stats = os.stat(entry)
                    created = time.ctime(os.path.getctime(entry))
                    Name = str(entry.name)
                    LastModified = date.convert_date(info.st_mtime)
                    DateCopied = todayDate
                    FileSize = (file_stats.st_size / (1024 * 1024))
                    ChicagoTime = todayTime.strftime("%H:%M:%S")
                    DateCreated =  created
                    fileId = Name+str(path)+LastModified+DateCopied+DateCreated
                    cur.execute("SELECT EXISTS(SELECT 1 FROM photosdata WHERE fileId = '"+fileId+"')")
                    result = cur.fetchone()
                    if result==(True,):
                        data['files'].append({
                            'FileId': fileId,
                            'Name': entry.name,
                            'Path': path,
                            'LastModified': date.convert_date(info.st_mtime),
                            'DateCopied': todayDate,
                            'FileSize': file_stats.st_size / (1024 * 1024),
                            "ChicagoTime:": todayTime.strftime("%H:%M:%S"),
                            'DateCreated': created,
                            'Status': 'Failed',
                            'Reason':'File Already Exists'
                        })
                    else:
                        records_to_insert = (fileId, Name, path, LastModified, DateCopied, FileSize, ChicagoTime, DateCreated)
                        cur.execute(postgres_insert_query, records_to_insert)
                        conn.commit()
                        data['files'].append({
                            'FileId': fileId,
                            'Name': entry.name,
                            'Path': path,
                            'LastModified': date.convert_date(info.st_mtime),
                            'DateCopied': todayDate,
                            'FileSize': file_stats.st_size / (1024 * 1024),
                            "ChicagoTime:": todayTime.strftime("%H:%M:%S"),
                            'DateCreated': created,
                            'Status': 'Sucessfully Uploaded'
                        })
            os.chdir(out_dir)
            time_script_run = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
            with open('{}_data.json'.format(time_script_run), 'w') as outfile:
                json.dump(data, outfile)
        
        except Exception:
            print('Cannot insert into the database')


def main():
    connect = Connection()
    conn, cur = connect.createConnection()
    arg = Arguments()
    directory, output = arg.makeArguments()
    today = Today()
    todayDate, todayTime = today.getDate()
    album = Albums()
    allAlbums = album.FetchAlbums()
    answer, album_id = album.CheckAlbum(allAlbums, todayDate)
    if answer == False:
        album_id = album.MakeAlbum(todayDate)
    upload = Images()
    tokens, flag = upload.UploadImages(directory, todayDate, todayTime, album_id, cur)
    if flag == 0:
        print('No new images')
    else:
        #print(tokens)
        new_media_items = [{'simpleMediaItem': {'uploadToken': tok}} for tok in tokens]
        request_body = {
            'albumId': album_id,
            'newMediaItems': new_media_items
        }
        upload_response = service.mediaItems().batchCreate(body=request_body).execute()
        print(upload_response)
    store = Store()
    store.makeFiles(directory, todayDate, todayTime, cur, conn, output)


if __name__ == "__main__":
    main()