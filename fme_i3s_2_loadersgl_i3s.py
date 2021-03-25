import os
import time
import zipfile
import fnmatch
import sys
import getopt
import gzip
import shutil
import logging
import subprocess
import multiprocessing
from multiprocessing.pool import ThreadPool

def main(argv):
   logging.basicConfig(
      format='%(asctime)s %(levelname)-8s %(message)s',
      level=logging.INFO,
      datefmt='%Y-%m-%d %H:%M:%S')

   inputfile = ''
   parallel = False
   numberOfThreads = 0
   try:
      opts, args = getopt.getopt(argv,"hi:p:",["ifile=","parallel="])
   except getopt.GetoptError:
      print ('fme_i3s_2_loadersgl_i3s.py -i <slpk file> [optional: -p <number of threads>]')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-help':
         print ('fme_i3s_2_loadersgl_i3s.py -i <slpk file> [optional: -p <number of threads>]')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-p", "--parallel"):
         parallel = True
         numberOfThreads = int(arg)

   logging.info("Starting conversion.")
   start = time.time()
   directoryname = checkInputFile(inputfile)
   newDirectoryname = createSceneServerDirectory(directoryname)
   logging.info("Unzipping .slpk file.")
   unzipFile(inputfile, directoryname)
   logging.info("Finished unzipping .slpk file.")
   if parallel:
      logging.info("Start parallel processing files with " + str(numberOfThreads) + " threads.")
      parallelConvertToIndex(directoryname, numberOfThreads)
      logging.info("Finished parallel processing files.")
   else:
      logging.info("Start processing files.")
      convertToIndex(directoryname)
      logging.info("Finished processing files.")
   copy3dNodeIndexDocumentFile(directoryname)
   moveProcessedFiles(directoryname, newDirectoryname)
   end = time.time()
   logging.info("Finished conversion in " + str(int(end - start)) + " seconds.")

def createSceneServerDirectory(directoryname):
   dirname = os.path.dirname(directoryname)
   basename = os.path.basename(directoryname)
   newDirectoryname = os.path.join(dirname, basename + "_converted", "SceneServer" , "layers", "0")
   if os.path.exists(newDirectoryname):
      logging.info("Targetpath " + newDirectoryname + " already exists.")
      sys.exit()
   os.makedirs(newDirectoryname)
   return newDirectoryname

def moveProcessedFiles(directoryname, newDirectoryname):
   file_names = os.listdir(directoryname)
   for f in file_names:
      shutil.move(os.path.join(directoryname, f), newDirectoryname)
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
   logging.info("Inputfile is no .slpk file or valid path to .slpk file")
   sys.exit(2)

def unzipFile(inputfile, directoryname):
   if(not os.path.exists(directoryname)):
      with zipfile.ZipFile(inputfile, 'r') as zFile:
         zFile.extractall(directoryname)

def convertToIndex(dirName):
   absPath = os.path.abspath(dirName)
   for dirpath, dirnames, filenames in os.walk(absPath):
      for filename in filenames:
         processFile(dirpath, filename)
 

def parallelConvertToIndex(dirName, numberOfThreads):
   pool = ThreadPool(numberOfThreads)
   filelist = []
   absPath = os.path.abspath(dirName)
   for dirpath, dirnames, filenames in os.walk(absPath):
      for filename in filenames:
         filelist.append((dirpath, filename))
         pool.apply_async(processFile, args = (dirpath, filename, ), )
   pool.close()
   pool.join()

def processFile(dirpath, filename):
   filepath = os.path.join(dirpath, filename)
   if fnmatch.fnmatch(filename, '*.gz'):
      if fnmatch.fnmatch(filename, '3dNodeIndexDocument.json.gz'):
         process3dNodeIndexDocumentFile(filepath)
      else:
         with gzip.open(filepath, 'rb') as f_in:
            baseName = filename.split('.')[0]
            newFilePath = os.path.join(dirpath, baseName)
            if not os.path.exists(newFilePath):
               os.makedirs(newFilePath)
            fileExt = filename.split('.')[1]
            newFilename = os.path.join(newFilePath, "index." + fileExt)
            with open(newFilename, 'wb') as f_out:
               shutil.copyfileobj(f_in, f_out)
         os.remove(filepath)

def process3dNodeIndexDocumentFile(filePath):
   with gzip.open(filePath, 'rb') as f_in:
      dirname = os.path.dirname(filePath)
      newFilePath = os.path.join(dirname, "index.json")
      with open(newFilePath, 'wb') as f_out:
         shutil.copyfileobj(f_in, f_out)
   os.remove(filePath)

if __name__ == "__main__":
   main(sys.argv[1:])