from bs4 import BeautifulSoup
import requests
import mysql.connector
import base64
import logging
import sys
import traceback
import datetime, os, argparse
# Logger
logging.basicConfig(filename="logfile.log", level=logging.INFO,
                    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s', datefmt='%H:%M:%S')
# setting database connection
logging.info('Setting Database Connection')
db_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    passwd=''
)
logging.info('Creating Cursor')
db_cursor = db_connection.cursor()
logging.info('Cursor Created Successfully')
logging.info("Creating Database")
db_cursor.execute("CREATE DATABASE IF NOT EXISTS VIENNA_ARCH")
logging.info("Database Created Successfully")
db_cursor.execute("USE VIENNA_ARCH")
logging.info("Creating Table mark_details")
db_cursor.execute(
    "create table IF NOT EXISTS mark_details (application_no VARCHAR(16) primary key, last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, wordmark VARCHAR(256) NOT NULL, proprietor VARCHAR(512) NOT NULL, class VARCHAR(32) NOT NULL, status VARCHAR(32) NOT NULL, error VARCHAR(1024), image_url VARCHAR(1024) DEFAULT NULL)")
logging.info("Table mark_details created Successfully")
logging.info("Creating Table mark_vienna_map")
db_cursor.execute(
    "create table IF NOT EXISTS mark_vienna_map (application_no VARCHAR(16) NOT NULL, vienna_code VARCHAR(16) NOT NULL, last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (application_no, vienna_code))")
logging.info("Table mark_vienna_map created Successfully")
#
ap = argparse.ArgumentParser()
ap.add_argument("-f", "--html_folder_path", type=str, help="path to html files")
ap.add_argument("-i", "--image_folder_path", type=str, help="path to folder for saving image")
args = vars(ap.parse_args())
htmlFolder = args['html_folder_path']
imageFolder = args['image_folder_path']
# htmlFolder = "/home/richa/Downloads/viennaParser/Aniket-1/";
# imageFolder = "/home/richa/Downloads/vienna_parser/ParsedFiles/";
for filename in os.listdir(htmlFolder):
    val_map = []
    val = []
    sql_map = "insert IGNORE into mark_vienna_map(application_no, vienna_code, last_updated_date) values(%s, %s, %s) "
    sql = "insert IGNORE into mark_details(application_no, last_updated_date, wordmark, proprietor, class, status, error, image_url) values(%s , %s, %s, %s, %s, %s, %s, %s)"
    if 'TM' in filename or '.htm' or '.asp' in filename:
        logging.info('Parsing File: ' + filename)
        with open(os.path.join(htmlFolder, filename)) as html_file:
            soup = BeautifulSoup(html_file, 'lxml')
        tot = soup.find('span', id='ContentPlaceHolder1_LblSearchDetail').find('td').text
        tot = tot.strip().split(' ')
        total_marks = int(tot[len(tot) - 1])
        logging.info('Extracting Data from file')
        count_total_marks = 0
        for i in range(total_marks):
            error = ""
            try:
                wordmark = soup.find('span', id='ContentPlaceHolder1_MGVSearchResult_lblsimiliarmark_' + str(i)).text
                proprietor = soup.find('span',
                                       id='ContentPlaceHolder1_MGVSearchResult_LblVProprietorName_' + str(i)).text
                app_num = (soup.find('span', id='ContentPlaceHolder1_MGVSearchResult_lblapplicationnumber_' + str(
                    i)).text).strip(' ')
                class_num = soup.find('span', id='ContentPlaceHolder1_MGVSearchResult_lblsearchclass_' + str(i)).text
                status = soup.find('span', id='ContentPlaceHolder1_MGVSearchResult_Label6_' + str(i)).text
                vienna_code = (
                    soup.find('span', id='ContentPlaceHolder1_MGVSearchResult_LblViennaCode_' + str(i)).text).strip(' ')
                img = soup.find('a', id='ContentPlaceHolder1_MGVSearchResult_LnkDGImage_' + str(i)).find('img')
                if img is None:
                    logging.info("Image Corresponding to application number: {} is missing".format(app_num))
                    img_url = ""
                else:
                    inp = img['src']
                    head, tail = inp.split(';')
                    enc, msg = tail.split(',')
                    imgdata = base64.b64decode(msg)
                    imagename = str(app_num) + '.png'
                    imgFile = open(os.path.join(imageFolder, imagename), 'wb')
                    imgFile.write(imgdata)
                    imgFile.close()
                    img_url = imageFolder + imagename
            except Exception as e:
                error = str(sys.exc_info()[1])
                logging.error("Exception: " + error)
            try:
                curr_time = datetime.datetime.now()
                formatted_date = curr_time.strftime('%Y-%m-%d %H:%M:%S')
                vienna_map = (app_num, vienna_code, formatted_date)
                val_map.append(vienna_map)
                logging.info(
                    "Info Added to Table mark_vienna_map: " + app_num + ',' + vienna_code + ',' + formatted_date)
                count_total_marks += 1
                vienna = (app_num, formatted_date, wordmark, proprietor, class_num, status, error, img_url)
                val.append(vienna)
                if len(val_map) == 40:
                    try:
                        db_cursor.executemany(sql_map, val_map)
                        logging.info("inserted 40 entries")
                    except:
                        exception = sys.exc_info()[1]
                        logging.error("Exception: " + str(exception))
                        db_connection.rollback()
                    db_connection.commit()
                    try:
                        db_cursor.executemany(sql, val)
                    except:
                        exception = sys.exc_info()[1]
                        logging.error("Exception: " + str(exception))
                        db_connection.rollback()
                    db_connection.commit()
                    val_map = []
                    val = []
                logging.info(
                    "Info Added to Table mark_details: " + app_num + ',' + formatted_date + ',' + wordmark + ',' + proprietor + ',' + class_num + ',' + status + ',' + error + ',' + img_url)
            except:
                exception = sys.exc_info()[1]
                logging.error("Exception: " + str(exception))
        try:
            db_cursor.executemany(sql_map, val_map)
        except:
            exception  = sys.exc_info()[1]
            logging.error("Exception: " + str(exception))
            db_connection.rollback()
        db_connection.commit()
        try:
            db_cursor.executemany(sql, val)
        except:
            exception = sys.exc_info()[1]
            logging.error("Exception: " + str(exception))
            db_connection.rollback()
        db_connection.commit()
        if count_total_marks == total_marks:
            logging.info("Number of Rows inserted in Table mark_vienna_map is same as Total Number of Matching Marks")
        else:
            logging.info(count_total_marks, total_marks)
        logging.info("Parsing File" + filename + " Done!")
db_connection.close()
