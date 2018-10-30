"""

    File:    DbLoadingApi.py

    Providing an API for WFM to parse the cif file and run MYSQL commands to load data to the database 'da_internal'.
 
   __author__    = "Li Chen"
   __email__     = "lchen@rcsb.rutgers.edu"
   __version__   = "V0.01"
   __Date__      = "Oct 4, 2013"

 Updates:

   07-Feb-2014  jdw   Add socket support
   21-May-2015  LC Add function doDataLoadingBcp()
"""

import os,sys,traceback
from subprocess import Popen, PIPE
#
from wwpdb.utils.config.ConfigInfo import ConfigInfo

import signal

class DbLoadingApi(object):
    """
    """
    def __init__(self, log=sys.stderr, verbose=False):
        """
           
        """
        self.__lfh = log
        self.__verbose = verbose
        self.__debug=True
        cI=ConfigInfo()
        self.__pyPath      = cI.get("SITE_PYTHON_SOURCE_PATH")
        self.__pkgPath     = cI.get("SITE_PACKAGES_PATH")
        self.__resrcPath   = cI.get("SITE_RESOURCE_DIRECTORY_PATH")
        self.__dbServer    = cI.get("SITE_DB_SERVER")
        #self.__dbHost      = "pdb-b-linux-6.rutgers.edu"
        self.__dbHost      = cI.get("SITE_DB_HOST_NAME")
        self.__dbUser      = cI.get("SITE_DB_USER_NAME")
        self.__dbPw        = cI.get("SITE_DB_PASSWORD")
        self.__dbPort      = str(cI.get("SITE_DB_PORT_NUMBER"))
        self.__dbSocket    = cI.get("SITE_DB_SOCKET")
        self.__archivePath  = cI.get("SITE_ARCHIVE_STORAGE_PATH")
        
        self.__dbName    = "da_internal"
        self.__workPath  = os.path.join(self.__archivePath,"archive")
        self.__mysql  = "/usr/bin/mysql "
        self.__dbLoader  = os.path.join(self.__pkgPath,"dbloader","bin","db-loader") 
        
        self.__mapping   = os.path.join(self.__resrcPath,"status_rcsb_schema_da.cif")
        
        self.__workDir   = "dbdata"                                 
    
    def doDataLoading(self, depId, sessionDir):
        """
           Take deposition id and session directory as input

        """
        """
          If a sequence of commands appears in a pipeline, and one of the
          reading commands finishes before the writer has finished, the
          writer receives a SIGPIPE signal.
          Set signal for running os.system() with broken PIPE problem
        """
        #signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        
        depId = depId.upper()
        cifPath = self.__workPath+"/"+depId+"/"
        ## test if there is any cif file if yes do ...
        
        if(os.path.exists(cifPath + depId+'_model_P1.cif.V1')):
            dataDir = sessionDir+"/"+self.__workDir
            log1 = "db-loader.log"
            if not os.path.exists(dataDir):
                print("Creating "+dataDir)
                os.makedirs(dataDir)
           
            cmd = "cd "+dataDir
            cmd += "; rm -f *"
            cmd +="; ls -tl "+cifPath+"D_*model_P1.cif.V[1-9]* | head -n1 | awk '{print $9}' >FILELIST"
            cmd +="; "+self.__dbLoader+" -server mysql -list FILELIST -map "+self.__mapping+" -db "+self.__dbName +" >& "+log1
            os.system(cmd)
            #print cmd
            #for file in os.listdir(dataDir):
            #    print file
            
            if(os.path.exists(dataDir+"/FILELIST")):
                # checking generated files
                file1 = os.path.join(dataDir,"DB_LOADER.sql")
                log2  = os.path.join(dataDir,"data_loading.log")
                if(os.path.exists(file1)):
                    cmd = "cd "+dataDir
                    if self.__dbSocket is None:
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" <"+file1+" >&"+log2
                    else:
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" -S "+self.__dbSocket+ " <"+file1+" >&"+log2
                    
                    os.system(cmd)
                    if(os.path.exists(log2)):
                        print("Finished the database commands")
                        file = open(log2, 'r')
                        for line in file:
                            for word in line.split():
                                if(word.upper()=="ERROR"):
                                    print("DbLoadingApi::doDataLoading(): ERROR found during the database loading. Please check the log file "+log2+" for details.")
                                    
                else:
                    print("DbLoadingApi::doDataLoading(): db-loader didn't generate the data file \"DB_LOADER.sql\". Please check the log file "+os.path.join(dataDir,log1)+" for details.")
            else:
                print("DbLoadingApi::doDataLoading(): No cif file found. Please check if the cif file exists.")
                    
            
        else:
            print("DbLoadingApi::doDataLoading(): No any cif file found.")

    def doLoadStatus(self, pdbxFilePath, sessionDir):
        """
           Load the input file into the status database and session directory as input

        """
        #signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        self.__lfh.write("DbLoadingApi::doLoadStatus(): starting with file %s and session %s\n" % (pdbxFilePath,sessionDir))
        try:
            if(os.path.exists(pdbxFilePath)):
                dataDir = sessionDir+"/"+self.__workDir
                log1 = "db-loader.log"
                if not os.path.exists(dataDir):
                    print("Creating "+dataDir)
                    os.makedirs(dataDir)

                cmd = "cd " + dataDir
                cmd +="; "+self.__dbLoader+" -server mysql -f " + pdbxFilePath + " -map " + self.__mapping + " -db " + self.__dbName +" >& "+log1
                os.system(cmd)
                if self.__debug:
                    self.__lfh.write("DbLoadingApi::doLoadStatus(): db-loader command %s\n" % cmd)

                file1 = os.path.join(dataDir,"DB_LOADER.sql")
                log2  = os.path.join(dataDir,"status_load.log")            
                if(os.path.exists(file1)):
                    cmd = "cd "+dataDir
                    if self.__dbSocket is None:
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" <"+file1+" >& "+log2
                    else:
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" -S "+self.__dbSocket+ " <"+file1+" >& "+log2
                    if self.__debug:
                        self.__lfh.write("DbLoadingApi::doLoadStatus(): database server command %s\n" % cmd)
                    os.system(cmd)
                    if(os.path.exists(log2)):
                        file = open(log2, 'r')
                        for line in file:
                            for word in line.split():
                                if(word.upper()=="ERROR"):
                                    self.__lfh.write("DbLoadingApi::doLoadStatus(): ERROR found during the database loading. Please check the log file "+log2+" for details.\n")
                                    return False
                    else:
                        self.__lfh.write("DbLoadingApi::doLoadStatus(): db-loader didn't generate the data file \"DB_LOADER.sql\". Please check the log file "+os.path.join(dataDir,log1)+" for details.\n")

                    #
                    self.__lfh.write("DbLoadingApi::doLoadStatus(): completed\n")
                    return True
                else:
                    self.__lfh.write("DbLoadingApi::doLoadStatus(): failing, no load file created.\n")
                    return False
            else:
                self.__lfh.write("DbLoadingApi::doLoadStatus(): failing, no input cif file found.\n")
                return False
        except:
            self.__lfh.write("DbLoadingApi::doLoadStatus(): failing, with exception.\n")            
            traceback.print_exc(file=self.__lfh)                                        

    def doDataLoadingBcp(self, depId, sessionDir):
        """
           Similar as doDataLoading(), Run db-loader with the option to
           get bcp data files, not sql commands.

        """
        """
          If a sequence of commands appears in a pipeline, and one of the
          reading commands finishes before the writer has finished, the
          writer receives a SIGPIPE signal.
          Set signal for running os.system() with broken PIPE problem
        """
        #signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        #print self.__dbHost
        
        depId = depId.upper()
        cifPath = self.__workPath+"/"+depId+"/"
        ## test if there is any cif file if yes do ...
        
        if(os.path.exists(cifPath + depId+'_model_P1.cif.V1')):
            dataDir = sessionDir+"/"+self.__workDir
            log1 = "db-loader.log"
            if not os.path.exists(dataDir):
                print("Creating "+dataDir)
                os.makedirs(dataDir)
            
            cmd = "cd "+dataDir
            cmd += "; rm -f *"
            cmd +="; ls -tl "+cifPath+"D_*model_P1.cif.V[1-9]* | head -n1 | awk '{print $9}' >FILELIST"
            cmd +="; "+self.__dbLoader+" -server mysql -list FILELIST -map "+self.__mapping+" -db "+self.__dbName +" -bcp >& "+log1
            #print cmd 
            os.system(cmd)
            
            #for file in os.listdir(dataDir):
            #    print file
            
            if(os.path.exists(dataDir+"/FILELIST")):
                # checking generated files
                file1 = os.path.join(dataDir,"DB_LOADER_DELETE.sql")
                file2 = os.path.join(dataDir,"DB_LOADER_LOAD.sql")
                log2  = os.path.join(dataDir,"data_loading.log")
                log3  = os.path.join(dataDir,"data_delete.log")
                if(os.path.exists(file2)):
                    cmd = "cd "+dataDir
                    if self.__dbSocket is None:
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" <"+file1+">& "+log3
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" <"+file2+">& "+log2
                        
                    else:
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" -S "+self.__dbSocket+ " <"+file1+">& "+log3
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" -S "+self.__dbSocket+ " <"+file2+">& "+log2
                    #print cmd
                    os.system(cmd)
                    if(os.path.exists(log2)):
                        print("Finished the database commands")
                        file = open(log2, 'r')
                        for line in file:
                            for word in line.split():
                                if(word.upper()=="ERROR"):
                                    print("DbLoadingApi::doDataLoading(): ERROR found during the database loading. Please check the log file "+log2+" for details.")
                                    
                else:
                    print("DbLoadingApi::doDataLoading(): db-loader didn't generate the data file \"DB_LOADER_LOAD.sql\". Please check the log file "+os.path.join(dataDir,log1)+" for details.")
            else:
                print("DbLoadingApi::doDataLoading(): No cif file found. Please check if the cif file exists.")
                    
            
        else:
            print("DbLoadingApi::doDataLoading(): No any cif file found.")

    def doDataLoadingByMapping(self, depId, sessionDir, mappingFile, dbName):
        """
           Similar as doDataLoading(), Run db-loader with the option to
           get bcp data files using the giving mapping file and dbname.

        """
        """
          If a sequence of commands appears in a pipeline, and one of the
          reading commands finishes before the writer has finished, the
          writer receives a SIGPIPE signal.
          Set signal for running os.system() with broken PIPE problem
        """
        #signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        #print self.__dbHost
        
        depId = depId.upper()
        myMappingFile = os.path.join(self.__resrcPath, mappingFile)
        cifPath = self.__workPath+"/"+depId+"/"
        ## test if there is any cif file if yes do ...
        
        if(os.path.exists(cifPath + depId+'_model_P1.cif.V1')):
            dataDir = sessionDir+"/"+self.__workDir
            log1 = "db-loader.log"
            if not os.path.exists(dataDir):
                print("Creating "+dataDir)
                os.makedirs(dataDir)
            
            cmd = "cd "+dataDir
            cmd += "; rm -f *"
            cmd +="; ls -tl "+cifPath+"D_*model_P1.cif.V[1-9]* | head -n1 | awk '{print $9}' >FILELIST"
            cmd +="; "+self.__dbLoader+" -server mysql -list FILELIST -map "+myMappingFile+" -db "+dbName +" -bcp >& "+log1
            #print cmd 
            os.system(cmd)
            
            #for file in os.listdir(dataDir):
            #    print file
            
            if(os.path.exists(dataDir+"/FILELIST")):
                # checking generated files
                file1 = os.path.join(dataDir,"DB_LOADER_DELETE.sql")
                file2 = os.path.join(dataDir,"DB_LOADER_LOAD.sql")
                log2  = os.path.join(dataDir,"data_loading.log")
                log3  = os.path.join(dataDir,"data_delete.log")
                if(os.path.exists(file2)):
                    cmd = "cd "+dataDir
                    if self.__dbSocket is None:
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" <"+file1+">& "+log3
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" <"+file2+">& "+log2
                        
                    else:
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" -S "+self.__dbSocket+ " <"+file1+">& "+log3
                        cmd += "; "+self.__mysql+"-u "+self.__dbUser+" -p"+self.__dbPw+" -h "+self.__dbHost+" -P "+self.__dbPort+" -S "+self.__dbSocket+ " <"+file2+">& "+log2
                    #print cmd
                    os.system(cmd)
                    if(os.path.exists(log2)):
                        print("Finished the database commands")
                        file = open(log2, 'r')
                        for line in file:
                            for word in line.split():
                                if(word.upper()=="ERROR"):
                                    print("DbLoadingApi::doDataLoading(): ERROR found during the database loading. Please check the log file "+log2+" for details.")
                                    
                else:
                    print("DbLoadingApi::doDataLoading(): db-loader didn't generate the data file \"DB_LOADER_LOAD.sql\". Please check the log file "+os.path.join(dataDir,log1)+" for details.")
            else:
                print("DbLoadingApi::doDataLoading(): No cif file found. Please check if the cif file exists.")
                    
            
        else:
            print("DbLoadingApi::doDataLoading(): No any cif file found.")

                
    if __name__ == '__main__':
        pass
    
