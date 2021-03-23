import os
import time
import zipfile
import fnmatch
import sys
import getopt
import gzip
import shutil
import logging

def main(argv):
   logging.basicConfig(
      format='%(asctime)s %(levelname)-8s %(message)s',
      level=logging.INFO,
      datefmt='%Y-%m-%d %H:%M:%S')

   inputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   except getopt.GetoptError:
      print ('fme_i3s_2_loadersgl_i3s-server.py -i <slpk file>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-help':
         print ('fme_i3s_2_loadersgl_i3s-server.py -i <slpk file>')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
   logging.info("Starting conversion.")
   start = time.time()
   directoryname = checkInputFile(inputfile)
   logging.info("Unzipping .slpk file.")
   unzipFile(inputfile, directoryname)
   logging.info("Finished unzipping .slpk file.")
   logging.info("Scan directories.")
   convertToIndex(directoryname)
   logging.info("Scanning directories finished.")
   # copy main 3dSceneLayer index.json to main folder
   copy3dNodeIndexDocumentFile(directoryname)
   # create new folder structure
   createSceneServerDirectory(directoryname)
   end = time.time()
   logging.info("Finished conversion in " + str(end - start) + " seconds.")

def createSceneServerDirectory(directoryname):
   dirname = os.path.dirname(directoryname)
   print(dirname)
   basename = os.path.basename(directoryname)
   print(basename)
   newPath = os.path.join(dirname, basename + "_converted", "SceneServer" , "layers", "0")
   os.makedirs(newPath)
   file_names = os.listdir(directoryname)
   for f in file_names:
      shutil.move(os.path.join(directoryname, f), newPath)
   shutil.rmtree(directoryname)

def copy3dNodeIndexDocumentFile(directoryname):
   absPath = os.path.abspath(directoryname)
   oldPath = os.path.join(absPath, "3dSceneLayer", "index.json")
   newPath = os.path.join(absPath, "index.json")
   shutil.copyfile(oldPath, newPath)

def checkInputFile(inputfile):
   if os.path.isfile(inputfile):
      fileName = os.path.splitext(inputfile)
      if fileName[1] == '.slpk':
         return fileName[0]
   logging.debug("Inputfile is no .slpk file or valid path to .slpk file")
   sys.exit(2)

def unzipFile(inputfile, directoryname):
   if(not os.path.exists(directoryname)):
      with zipfile.ZipFile(inputfile, 'r') as zFile:
         zFile.extractall(directoryname)

def convertToIndex(dirName):
   absPath = os.path.abspath(dirName)
   for dirpath, dirs, files in os.walk(os.path.join(absPath)):
      for f in files:
         if fnmatch.fnmatch(f, '*.json.gz'):
            if fnmatch.fnmatch(f, '3dNodeIndexDocument.json.gz'):
               process3dNodeIndexDocumentFile(os.path.join(dirpath, f))
            else:
               processJsonFile(dirpath, f)
         if fnmatch.fnmatch(f, '*.bin.gz'):
            processBinFile(dirpath, f)

def processJsonFile(filePath, fileName):
   with gzip.open(os.path.join(filePath, fileName), 'rb') as f_in:
      # remove the .bin.gz extension
      baseName = fileName.split('.')[0]
      # make a new directory from file name
      newFilePath = os.path.join(filePath, baseName)
      if not os.path.exists(newFilePath):
         os.makedirs(newFilePath)
      # save unzipped file as index.json in new directory
      newFileName = os.path.join(newFilePath, "index.json")
      with open(newFileName, 'wb') as f_out:
         shutil.copyfileobj(f_in, f_out)
   os.remove(os.path.join(filePath, fileName))

def processBinFile(filePath, fileName):
   with gzip.open(os.path.join(filePath, fileName), 'rb') as f_in:
      # remove the .bin.gz extension
      baseName = fileName.split('.')[0]
      # make a new directory from file name
      newFilePath = os.path.join(filePath, baseName)
      if not os.path.exists(newFilePath):
         os.makedirs(newFilePath)
      # save unzipped file as index.json in new directory
      newFileName = os.path.join(newFilePath, "index.bin")
      with open(newFileName, 'wb') as f_out:
         shutil.copyfileobj(f_in, f_out)
   os.remove(os.path.join(filePath, fileName))

def process3dNodeIndexDocumentFile(filePath):
   with gzip.open(filePath, 'rb') as f_in:
      dirname = os.path.dirname(filePath)
      newFilePath = os.path.join(dirname, "index.json")
      with open(newFilePath, 'wb') as f_out:
         shutil.copyfileobj(f_in, f_out)
   os.remove(filePath)

if __name__ == "__main__":
   main(sys.argv[1:])