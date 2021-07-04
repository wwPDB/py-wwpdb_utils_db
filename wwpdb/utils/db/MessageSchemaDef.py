##
# File:    MessageSchemaDef.py
# Author:  J. Westbrook
# Date:    15-Nov-2011
# Version: 0.001 Initial version
#
# Updates:
#    2012-06-05    RPS    Updated to accommodate "READ_STATUS" attribute
##
"""
Database schema defintions for deposition related message data.

"""
__docformat__ = "restructuredtext en"
__author__    = "John Westbrook"
__email__     = "jwest@rcsb.rutgers.edu"
__license__   = "Creative Commons Attribution 3.0 Unported"
__version__   = "V0.001"

import sys
from wwpdb.utils.db.SchemaDefBase import SchemaDefBase


class MessageSchemaDef(SchemaDefBase):
    """ A data class containing schema definitions for deposition related messages.
    """
    _databaseName="wwpdb_message_v1"
    _schemaDefDict = { 
        "DEP_MESSAGE_INFO" : {
            "TABLE_ID"            : "DEP_MESSAGE_INFO",
            "TABLE_NAME"          : "deposition_message_info",
            "TABLE_TYPE"          : "transactional",
            "ATTRIBUTES" :  {
                "ORDINAL_ID"         :  "ordinal_id",
                "MESSAGE_ID"         :  "message_id",             
                "DEP_ID"             :  "deposition_data_set_id", 
                "TIMESTAMP"          :  "timestamp",              
                "SENDER"             :  "sender",                 
                "CONTEXT_TYPE"       :  "context_type",            
                "CONTEXT_VALUE"      :  "context_value",
                "PARENT_MESSAGE_ID"  :  "parent_message_id",
                "MESSAGE_SUBJECT"    :  "message_subject",                
                "MESSAGE_TEXT"       :  "message_text",
                "READ_STATUS"        :  "read_status"   
                },
            "ATTRIBUTE_INFO" :  {
                "ORDINAL_ID"         : {"SQL_TYPE" : "INT UNSIGNED AUTO_INCREMENT",    "WIDTH":  0,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "MESSAGE_ID"         : {"SQL_TYPE" : "CHAR",    "WIDTH": 36,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
                "DEP_ID"             : {"SQL_TYPE" : "VARCHAR", "WIDTH": 10,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 3},
                "TIMESTAMP"          : {"SQL_TYPE" : "DATETIME","WIDTH": 10,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 4},
                "SENDER"             : {"SQL_TYPE" : "VARCHAR", "WIDTH": 15,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 5},
                "CONTEXT_TYPE"       : {"SQL_TYPE" : "VARCHAR", "WIDTH": 15,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 6},
                "CONTEXT_VALUE"      : {"SQL_TYPE" : "VARCHAR", "WIDTH": 40,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 7},
                "PARENT_MESSAGE_ID"  : {"SQL_TYPE" : "VARCHAR", "WIDTH": 36,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 8},
                "MESSAGE_SUBJECT"    : {"SQL_TYPE" : "VARCHAR", "WIDTH": 132,   "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 9},                
                "MESSAGE_TEXT"       : {"SQL_TYPE" : "VARCHAR", "WIDTH": 65000, "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 10},
                "READ_STATUS"        : {"SQL_TYPE" : "CHAR", "WIDTH": 1, "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 11},
                },
            "INDICES"                : {"p1"  : {"TYPE" : "UNIQUE", "ATTRIBUTES" : ["MESSAGE_ID"]},
                                        "i1"  : {"TYPE" : "SEARCH", "ATTRIBUTES" : ["MESSAGE_ID","DEP_ID","TIMESTAMP","SENDER","CONTEXT_TYPE","CONTEXT_VALUE"]},
                                        "i2"  : {"TYPE" : "SEARCH", "ATTRIBUTES" : ["MESSAGE_ID","PARENT_MESSAGE_ID"]},
                                        "i3"  : {"TYPE" : "SEARCH", "ATTRIBUTES" : ["MESSAGE_ID","MESSAGE_SUBJECT"]}
                                        }
            },
        
	"DEP_MESSAGE_DATA" : {
            "TABLE_ID"            : "DEP_MESSAGE_DATA",        
            "TABLE_NAME"          : "deposition_message_data",
            "TABLE_TYPE"          : "transactional",            
            "ATTRIBUTES"  : {
                "ORDINAL_ID"      : "ordinal_id",                
                "MESSAGE_ID"      : "message_id",
                "DEP_ID"          : "deposition_data_set_id", 
                "MESSAGE_TYPE"    : "message_type",
                "MESSAGE_SUBJECT" : "message_subject",                
                "MESSAGE_TEXT"    : "message_text"
                },
            "ATTRIBUTE_INFO" :  {
                "ORDINAL_ID"      : {"SQL_TYPE" : "INT UNSIGNED AUTO_INCREMENT",    "WIDTH":  0,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "MESSAGE_ID"      : {"SQL_TYPE" : "CHAR",    "WIDTH": 36,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
                "DEP_ID"          : {"SQL_TYPE" : "VARCHAR", "WIDTH": 10,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 3},
                "MESSAGE_TYPE"    : {"SQL_TYPE" : "VARCHAR", "WIDTH": 10,    "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 4},
                "MESSAGE_SUBJECT" : {"SQL_TYPE" : "VARCHAR", "WIDTH": 132,   "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 5},                
                "MESSAGE_TEXT"    : {"SQL_TYPE" : "VARCHAR", "WIDTH": 65000, "PRECISION" : 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 6} 
                },
            "INDICES"             : { "p1"   :  {"TYPE" : "UNIQUE",   "ATTRIBUTES" : ["MESSAGE_ID"]},
                                      "i1"   :  {"TYPE" : "SEARCH",   "ATTRIBUTES" : ["MESSAGE_ID","DEP_ID","MESSAGE_TYPE"]},
                                      "i2"   :  {"TYPE" : "SEARCH",   "ATTRIBUTES" : ["DEP_ID"]}, 
                                      "ft1"  :  {"TYPE" : "FULLTEXT", "ATTRIBUTES" : ["MESSAGE_SUBJECT"]},
                                      "ft2"  :  {"TYPE" : "FULLTEXT", "ATTRIBUTES" : ["MESSAGE_TEXT"]},
                                      "ft3"  :  {"TYPE" : "FULLTEXT", "ATTRIBUTES" : ["MESSAGE_SUBJECT","MESSAGE_TEXT"]}
                                     }
            }
        }

    def __init__(self,verbose=True,log=sys.stderr):
        super(MessageSchemaDef,self).__init__(databaseName=MessageSchemaDef._databaseName,schemaDefDict=MessageSchemaDef._schemaDefDict,verbose=verbose,log=log)
        

if __name__ == "__main__":
    msd=MessageSchemaDef()
    tableIdList=msd.getTableIdList()

    for tableId in tableIdList:
        aIdL=msd.getAttributeIdList(tableId)
        tObj=msd.getTable(tableId)
        attributeIdList=tObj.getAttributeIdList()
        attributeNameList=tObj.getAttributeNameList()        
        sys.stdout.write("Ordered attribute Id   list %s\n" % (str(attributeIdList)))
        sys.stdout.write("Ordered attribute name list %s\n" % (str(attributeNameList)))        
    #


