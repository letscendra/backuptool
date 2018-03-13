#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ==============================================================================================================================================
#                                                                backup_app.py
# ==============================================================================================================================================
#
# Copy one directory, and clean backups older than a defined number of days
# Monthly Yearly
#
#     FUNCIÓ directoryBackup - Backup del directori
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
print("[INFO] Importació de llibreries...",end="")

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

print("[OK]")

print("[INFO] Definició de variables...",end="")
# Definició de variables
backupFile=""

DESCRIPTION = """
weekYear=              Create & Upload to dropbox a directory backup, and
dayYear=d              clean backups older
              """
              
print("[OK]")

print("")

def directoryBackup(sourceDirectory, destinationDirectory):
    """Create directory copy"""
    global backupFile
    
    destinationDirectory=destinationDirectory+'/'
    # Definició de variables de la funció
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
    if not os.path.isdir(destinationDirectory):
        raise Exception("Backup directory does not exist: {}".format(destinationDirectory))
    if str(dayYear)=="001":
        print("[INFO] Primer dia del any (Backup Anual) - "+lastYear)
        backupFile = os.path.join(destinationDirectory,"yearly-bck-" +lastYear+"-"+ os.path.basename(sourceDirectory))
    elif str(dayMonth)=="01":
        print("[INFO] Primer dia del més (Backup Mensual) - "+lastMonthYear)
        backupFile = os.path.join(destinationDirectory,yesterdayDate.strftime("monthly-bck-%Y-")+lastMonthYear+"-" + os.path.basename(sourceDirectory))
    elif str(dayWeek)==str(firstWeekday):
        print("[INFO] Primer dia de la setmana (Backup Setmanal) - "+lastWeekYear)
        backupFile = os.path.join(destinationDirectory,yesterdayDate.strftime("weekly-bck-%Y-")+monthYear+"-w"+lastWeekYear+"-" + os.path.basename(sourceDirectory))
    else:
        backupFile = os.path.join(destinationDirectory,time.strftime("daily-bck-%Y%m%d-%H%M-") + os.path.basename(sourceDirectory))

    pathName, fileName = os.path.split(backupFile)
    fileName=fileName+".tar.gz"
#    print('It will backup directory `' + sourceDirectory + '` to `' + dbxPath + fileName)

    #FILE COMPRESSION
    print("[INFO] Directori a copiar: "+ sourceDirectory)
    print('[INFO] Directori on es copiarà el fitxer comprimit: `'+ destinationDirectory+'`')
    print("[INFO] Compressing folders and files... ", end="")
    sys.stdout.flush()
    try:
        #command='tar -zcvf ' + path + filename + ' ' + source +' -R'
        #p1 = subprocess.Popen(["tar","-zcvf",path+filename,source,"-R"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        p1 = subprocess.Popen(["tar","-zcvf",destinationDirectory+fileName,sourceDirectory,"-R"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        out, err = p1.communicate()
        #print(out)
        #print(err)
        p1.wait();
        #command='chmod 777 ' + path + filename
        #p2 = subprocess.Popen(["chmod","777",path+filename], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        p2 = subprocess.Popen(["chmod","777",destinationDirectory+fileName], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        #print(out)
        #print(err)
        p2.wait();
        print("[OK]")
        #print(' - Temporary file: `' + path+filename+ '`')
        #print(' - Temporary file: `' + destinationDirectory+fileName+ '`')
    except:
        print("[KO]")
        print('[ERROR] Something went wrong during compression of '+ sourceDirectory)
        sys.exit()
        
def backupDBX():
    """Copy to dropbox the file"""
    #print(backupFile)
    pathName, fileName = os.path.split(backupFile)
    pathName=pathName+"/"
    fileName=fileName+".tar.gz"
    
    #DROPBOX AUTHENTICATION
    print("[INFO] Authenticating at Dropbox... ", end="")
    sys.stdout.flush()
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

    #DROPBOX UPLOAD
    print("[INFO] Uploading to Dropbox... ", end="")
    sys.stdout.flush()
    print("\n"+pathName+fileName)
    print(dbxPath+fileName)
    #print(dbxPath+"test.tar.gz")
  #  f = gzip.open(pathName+fileName, mode='r')
  #  data = f.read()
  #  fileupload = dbx.files_upload(data,dbxPath+fileName) #Method used to upload files smaller than 150MB
    try:
        #f = gzip.open(path + filename, mode='r')
        f = gzip.open(pathName+fileName, mode='r')
        data = f.read()
        fileupload = dbx.files_upload(data,dbxPath+fileName) #Method used to upload files smaller than 150MB
        #fileupload = dbx.files_upload(data,dbxPath+"test.tar.gz") #Method used to upload files smaller than 150MB
        print("[OK]")
    except:
        print("[KO]")
        print("\n[ERROR] Ouch! Something went wrong uploading to dropbox!")
        sys.exit()
    
    print(' - Filename: `' + dbxPath+fileName + '`')
    print(' - Modified: `' + str(dbx.files_get_metadata(dbxPath+fileName).client_modified) + '`')

def clean_data(destinationDIR):
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
    for filename in os.listdir(destinationDIR):
        backupFile = os.path.join(destinationDIR, filename)
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
    parser.add_argument('sourceDIR', help='the directory that needs backed up')
    parser.add_argument('destinationDIR', help='the directory where the backup should be saved')
    arguments=parser.parse_args()
    if (os.path.exists(arguments.sourceDIR)==False):
        print("[ERROR] The source directory path provided doesn't exist.")
        sys.exit()
    if (os.path.exists(arguments.destinationDIR)==False):
        print("[ERROR] The destination directory path provided doesn't exist.")
        sys.exit()
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
    directoryBackup(args.sourceDIR, args.destinationDIR)
    backupDBX()
#    clean_data(args.destinationDIR)
    print ("[INFO] Backup update has been successful.")
print("---------------------------------------------------------------------------------------------------------")
print("[INFO] Program finished")
