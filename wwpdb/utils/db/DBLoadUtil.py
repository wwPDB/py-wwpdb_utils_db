##
# File:  DBLoadUtil.py
# Date:  06-feb-2015
# Updates:
##
"""
DB loading utility class

This software was developed as part of the World Wide Protein Data Bank
Common Deposition and Annotation System Project

Copyright (c) 2012 wwPDB

This software is provided under a Creative Commons Attribution 3.0 Unported
License described at http://creativecommons.org/licenses/by/3.0/.

"""
__docformat__ = "restructuredtext en"
__author__    = "Zukang Feng"
__email__     = "zfeng@rcsb.rutgers.edu"
__license__   = "Creative Commons Attribution 3.0 Unported"
__version__   = "V0.07"

import os, sys, string, traceback

from wwpdb.utils.config.ConfigInfo                     import ConfigInfo

class DBLoadUtil(object):
    """ Class responsible for loading model cif file(s) into da_internal database
    """
    def __init__(self, reqObj=None, verbose=False, log=sys.stderr):
        self.__verbose=verbose
        self.__lfh=log
        self.__reqObj=reqObj
        self.__sObj=None
        self.__sessionId=None
        self.__sessionPath=None
        self.__siteId  = str(self.__reqObj.getValue("WWPDB_SITE_ID"))
        self.__cI=ConfigInfo(self.__siteId)
        #
        self.__getSession()

    def doLoading(self, fileList):
        """ Update content database
        """
        if not fileList:
            return
        #
        #
        scriptfile = self.__getFileName(self.__sessionPath, 'dbload', 'csh')
        listfile   = self.__getFileName(self.__sessionPath, 'filelist', 'txt')
        logfile    = self.__getFileName(self.__sessionPath, 'dbload', 'log')
        clogfile   = self.__getFileName(self.__sessionPath, 'dbload_command', 'log')
        #
        self.__genListFile(listfile, fileList)
        self.__genScriptFile(scriptfile, listfile, logfile)
        self.__RunScript(self.__sessionPath, scriptfile, clogfile)

    def __getFileName(self, path, root, ext):
        """Create unique file name.
        """
        count = 1
        while True:
            filename = root + '_' + str(count) + '.' + ext
            fullname = os.path.join(path, filename)
            if not os.access(fullname, os.F_OK):
                return filename
            #
            count += 1
            #
            return root + '_1.' + ext
        
    def __RunScript(self, path, script, log):
        """Run script command
        """
        cmd = 'cd ' + path + '; chmod 755 ' + script \
            + '; ./' + script + ' >& ' + log
        os.system(cmd)

    def __genListFile(self, filename, list):
        """
        """
        fn = os.path.join(self.__sessionPath, filename)
        f = file(fn, 'w')
        for entryfile in list:
             f.write(entryfile + '\n')
        #
        f.close()

    def __genScriptFile(self, scriptfile, listfile, logfile):
        """
        """
        dbServer  = 'da_internal'
        dbHost    = self.__cI.get("SITE_DB_HOST_NAME")
        dbUser    = self.__cI.get("SITE_DB_USER_NAME")
        dbPw      = self.__cI.get("SITE_DB_PASSWORD")
        dbPort    = self.__cI.get("SITE_DB_PORT_NUMBER")
        mapping   = self.__cI.get("SITE_DA_INTERNAL_SCHEMA_PATH")
        dbLoader  = os.path.join(self.__cI.get("SITE_PACKAGES_PATH"), "dbloader", "bin", "db-loader")
        #
        script = os.path.join(self.__sessionPath, scriptfile)
        f = file(script, 'w')
        f.write('#!/bin/tcsh -f\n')
        f.write('#\n')
        f.write('if ( -e DB_LOADER.sql ) then\n')
        f.write('    /bin/rm -f DB_LOADER.sql\n')
        f.write('endif\n')
        f.write('#\n')
        f.write(dbLoader + ' -server mysql -list ' + listfile + ' -map ' + mapping + ' -db ' + dbServer + ' >& ' + logfile + '\n')
        f.write('#\n')
        f.write('if ( -e DB_LOADER.sql ) then\n')
        f.write('    /usr/bin/mysql -u ' + dbUser + ' -p' + dbPw + ' -h ' + dbHost + ' -P ' + str(dbPort) + ' < DB_LOADER.sql >>& ' + logfile + '\n')  
        f.write('endif\n')
        f.write('#\n')
        f.close()

    def __getSession(self):
        """ Join existing session or create new session as required.
        """
        #
        self.__sObj=self.__reqObj.newSessionObj()
        self.__sessionId=self.__sObj.getId()
        self.__sessionPath=self.__sObj.getPath()
        if (self.__verbose):
            self.__lfh.write("------------------------------------------------------\n")                    
            self.__lfh.write("+DBLoadUtil.__getSession() - creating/joining session %s\n" % self.__sessionId)
            self.__lfh.write("+DBLoadUtil.__getSession() - session path %s\n" % self.__sessionPath)            
