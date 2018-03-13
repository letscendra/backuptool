#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ==============================================================================================================================================
#                                                                backup_sql.py
# ==============================================================================================================================================
#
# Create a timestamped SQLite database backup, and clean backups older than a defined number of days
# Daily Weekly Monthly Yearly
#
#     FUNCIÓ sqlite3_backup - Backup de la base de dades
#     FUNCIÓ clean_data - Esborrem backups antics
#     FUNCIÓ get_arguments - Arguments que passem al programa
#
# ==============================================================================================================================================
#                                                              LLIBRERIES
# ==============================================================================================================================================
from __future__ import print_function
from __future__ import unicode_literals
print("")
print("*********************************************************************************************************")
print("*********************************************************************************************************")
print("*********************************************************************************************************")
print("")
print("Importació de llibreries")
# PEL BACKUP
import argparse
import sqlite3
import shutil
import time
import datetime
import os
# PEL DROPBOX
import dropbox # https://github.com/dropbox/dropbox-sdk-python, http://dropbox-sdk-python.readthedocs.io/en/latest/
import sys
import time
import os
import subprocess
import sys
import gzip

print("Definició de variables")
print("---------------------------------------------------------------------------------------------------------")
# Definició de variables
backupFile=""
DESCRIPTION = """
weekYear=              Create & Upload to dropbox a timestamped SQLite database backup, and
dayYear=d              clean backups older than a defined number of days
              """
print("---------------------------------------------------------------------------------------------------------")
print("Fi de la definició de variables")
print("")

def sqlite3_backup(dbfile, backupdir):
    """Create timestamped database copy"""
    global backupFile
    # Definició de variable de la funció
    todayDate=datetime.date.today()
    dayWeek=todayDate.strftime("%w")            #Numero de dia de la setmana
    dayMonth=todayDate.strftime("%d")           #Numero de dia del mes
    monthYear=todayDate.strftime("%B")          #Mes de l'any
    weekYear=todayDate.strftime("%W")           #Numero de setmana de l'any
    dayYear=todayDate.strftime("%j")            #Numero de dia de l'any
    currentYear=todayDate.strftime("%Y")
    oneDay=datetime.timedelta(days=1)
    yesterdayDate = todayDate - oneDay
    lastYear=yesterdayDate.strftime("%Y")
    lastMonthYear=yesterdayDate.strftime("%B")
    lastWeekYear=yesterdayDate.strftime("%W")
    firstWeekday=1                              # la setmana comença en dilluns(1)
    
    #dayYear="001"            # Per fer proves
    #dayMonth="01"            # Per fer proves
    
    # Donem nom al fitxer
    if not os.path.isdir(backupdir):
        raise Exception("Backup directory does not exist: {}".format(backupdir))
    if str(dayYear)=="001":
        print("Primer dia del any (Backup Anual) - "+lastYear)
        backupFile = os.path.join(backupdir,"yearly-bck-" +lastYear+"-"+ os.path.basename(dbfile))
    elif str(dayMonth)=="01":
        print("Primer dia del més (Backup Mensual) - "+lastMonthYear)
        backupFile = os.path.join(backupdir,yesterdayDate.strftime("monthly-bck-%Y-")+lastMonthYear+"-" + os.path.basename(dbfile))
    elif str(dayWeek)==str(firstWeekday):
        print("Primer dia de la setmana (Backup Setmanal) - "+lastWeekYear)
        backupFile = os.path.join(backupdir,yesterdayDate.strftime("weekly-bck-%Y-")+monthYear+"-w"+lastWeekYear+"-" + os.path.basename(dbfile))
    else:
        backupFile = os.path.join(backupdir,time.strftime("daily-bck-%Y%m%d-%H%M-") + os.path.basename(dbfile))
    
    # Obrim la base de dades
    connection = sqlite3.connect(dbfile)
    cursor = connection.cursor()

    # Lock database before making a backup
    cursor.execute('begin immediate')
    # Make new backup file
    shutil.copyfile(dbfile, backupFile)
    print ("\nCreating {}...".format(backupFile))
    # Unlock database
    connection.rollback()
    
def backupDBX():
    """Copy to dropbox the file"""
    print(backupFile)
    #TIME
    #now=time.strftime("%Y%m%d_%H%M%S", time.localtime())
    
    #VARIABLES
    FNULL = open(os.devnull, 'w')
    p1 = subprocess.Popen(["pwd"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out, err = p1.communicate()
    path=str(out)[2:-3]+'/'
    print
    
    #filename='bckp-'+now+'.tar.gz'
    pathName, fileName = os.path.split(backupFile)
    fileName=fileName+".tar.gz"
    #print('\n[INFO] Starting program. It will backup `' + source + '` to `' + dbxPath + filename)
    print('\n[INFO] Starting program. It will backup `' + backupFile + '` to `' + dbxPath + fileName)
    print('[INFO] Actual path on es copiarà temporalment el fitxer comprimit: `'+ path+'`')
    print("[INFO] Authenticating at Dropbox... ", end="")
    sys.stdout.flush()
    
    #DROPBOX AUTHENTICATION
    try:
            dbx = dropbox.Dropbox(db_access_token)
            account = dbx.users_get_current_account()
            print("[OK]")
            #print('        - Modified: `' + dbx.files_get_metadata('/Backup/').client_modified + '`')
            print(' - Account_id: `' + account.account_id + '`')
            print(' - Account_name: `' + account.name.given_name+ '`')
            print(' - Account_e-mail: `' + account.email+ '`')
            #print('        - Account_type: `' + account.account_type+ '`')
            #sys.stdout.flush()
    except:
            print("[KO]")
            print("[ERROR] Authentication NOT successful!")
            sys.exit()
    
    #FILE COMPRESSION
    print("[INFO] Compressing folders and files... ", end="")
    sys.stdout.flush()
    try:
            #command='tar -zcvf ' + path + filename + ' ' + source +' -R'
            #p1 = subprocess.Popen(["tar","-zcvf",path+filename,source,"-R"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            p1 = subprocess.Popen(["tar","-zcvf",path+fileName,backupFile,"-R"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            out, err = p1.communicate()
            #print(out)
            #print(err)
            p1.wait();
            #command='chmod 777 ' + path + filename
            #p2 = subprocess.Popen(["chmod","777",path+filename], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            p2 = subprocess.Popen(["chmod","777",path+fileName], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            #print(out)
            #print(err)
            p2.wait();
            print("[OK]")
            #print(' - Temporary file: `' + path+filename+ '`')
            print(' - Temporary file: `' + path+fileName+ '`')
    except:
            print("[KO]")
            print('[ERROR] Something went wrong during compression of '+ path + filename)
            sys.exit()
    
    #DROPBOX UPLOAD
    print("[INFO] Uploading to Dropbox... ", end="")
    sys.stdout.flush()
    try:
            #f = gzip.open(path + filename, mode='r')
            f = gzip.open(path+fileName, mode='r')
            data = f.read()
            fileupload = dbx.files_upload(data,dbxPath+fileName) #Method used to upload files smaller than 150MB
            print("[OK]")
    except:
            print("[KO]")
            print("\n[ERROR] Ouch! Something went wrong!")
            sys.exit()
    
    print(' - Filename: `' + dbxPath+fileName + '`')
    print(' - Modified: `' + str(dbx.files_get_metadata(dbxPath+fileName).client_modified) + '`')
    
    #DELETING LOCAL FILE
    print("[INFO] Deleting local file `" +path+fileName + '`... ', end="")
    sys.stdout.flush()
    try:
    
            p1 = subprocess.Popen(["rm","-f",path+fileName], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            out, err = p1.communicate()
            #print(out)
            #print(err)
            p1.wait();
            print("[OK]")
    except:
            print("[KO]")
            print('[ERROR] Something went wrong during compression of '+ path + fileName)
            sys.exit()

def clean_data(backup_dir):
    """Delete files older"""

    print ("\n------------------------------")
    print ("Cleaning up old backups")
    print("[INFO] Authenticating at Dropbox... ", end="")
    #DROPBOX AUTHENTICATION
    try:
        dbx = dropbox.Dropbox(db_access_token)
        account = dbx.users_get_current_account()
        print("[OK]")
        #print('        - Modified: `' + dbx.files_get_metadata('/Backup/').client_modified + '`')
        print(' - Account_id: `' + account.account_id + '`')
        print(' - Account_name: `' + account.name.given_name+ '`')
        print(' - Account_e-mail: `' + account.email+ '`')
        #print('        - Account_type: `' + account.account_type+ '`')
        #sys.stdout.flush()
    except:
        print("[KO]")
        print("[ERROR] Authentication NOT successful!")
        sys.exit()
    def DBXremove(fitxerBorrar):
        fitxerBorrar=fitxerBorrar+".tar.gz"
        #print("Borrarem de BDX el fitxer: "+fitxerBorrar)
        try:
            dbx.files_get_metadata(fitxerBorrar)
            existeixFitxer = True
        except:
            existeixFitxer = False
        if existeixFitxer == True:
            print ("Deleting in Dropbox {}...".format(fitxerBorrar), end="")
            try:
                dbx.files_delete(fitxerBorrar)
            except:
                print("[KO]")
            print("[OK]")
    for filename in os.listdir(backup_dir):
        backupFile = os.path.join(backup_dir, filename)
        backupFileDBX = dbxPath+filename
        if "daily-bck" in backupFile:
            if os.stat(backupFile).st_ctime < (time.time() - 7 * 86400): # 86400 segons / dia (Posem + per fer proves)
                if os.path.isfile(backupFile):
                    DBXremove(backupFileDBX)
                    print ("Deleting {}...".format(backupFile), end="")
                    os.remove(backupFile)
                    print("[OK]")
        elif "weekly-bck" in backupFile:
            if os.stat(backupFile).st_ctime < (time.time() - 35 * 86400):
                if os.path.isfile(backupFile):
                    DBXremove(backupFileDBX)
                    os.remove(backupFile)
                    print ("Deleting {}...".format(backupFile))
        elif "monthly-bck" in backupFile:
            if os.stat(backupFile).st_ctime < (time.time() - 266 * 86400):
                if os.path.isfile(backupFile):
                    DBXremove(backupFileDBX)
                    os.remove(backupFile)
                    print ("Deleting {}...".format(backupFile))

def get_arguments():
    """Parse the commandline arguments from the user"""

    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('db_file',
                        help='the database file that needs backed up')
    parser.add_argument('backup_dir',
                        help='the directory where the backup'
                              'file should be saved')
    return parser.parse_args()

def getConfigFile():
    # Variables del fitxer de configuració
    global db_access_token
    global dbxPath
    homeDir=os.path.dirname(os.path.abspath(__file__))
    configPath = os.path.join(homeDir,os.path.splitext(os.path.basename(__file__))[0]+".cfg")
    # Preparem fitxer de configuració si existeix
    print("Preparem el fitxer de configuració")
    print("---> Directori principal = ",homeDir)
    print("---> Fitxer de configuració = ",configPath)
    configFile={}
    configVars=["db_access_token","dbxPath"]
    configVars_default=["xxxxxxxxxxxxxxxxxxx","/"]
    if os.path.isfile(configPath):
        firstRun = 0
        with open(configPath, "r") as f:
            for line in f:
                if "=" in line:
                    option, value = line.split("=", 1)
                    option=option.strip()
                    value=value.strip()
                    #if value.isnumeric():
                    if value.isdigit():
                        configFile[option] = int(value)
                    else:
                        try: configFile[option] = float(value)
                        except: configFile[option] = value
    else:
        firstRun = 1

    #Posem les variables per defecte a la llista config
    for i in configVars: # error checking
        if not i in configFile:
            configFile[i] = configVars_default[configVars.index(i)]
            
    #print(firstRun)
    if firstRun:
        # update config file
        with open(configPath, 'w') as configfile:
            for i in configVars:
                configfile.write("%s=%s\n" %(i,configFile[i]))
              
    #DROPBOX PERSONAL DATA
    db_access_token=configFile["db_access_token"]
    dbxPath=configFile["dbxPath"]

print("Inici del programa")
print("---------------------------------------------------------------------------------------------------------")
if __name__ == "__main__":
    args = get_arguments()
    getConfigFile()
    sqlite3_backup(args.db_file, args.backup_dir)
    backupDBX()
    clean_data(args.backup_dir)
    print ("\nBackup update has been successful.")