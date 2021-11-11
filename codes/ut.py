# @file ut.py
# @brief The main unit test program of whole project
#    refer UTGeneral functions
#    print the suggested procedure in the console
#    print the suggested check procedure in the console
#    support current supported important features
#    this unit test include in the release procedure
# @author wuulong@gmail.com

#standard
import unittest
#homemake
import codes.globalclasses as gc
from codes.const import *



##### Unit test section ####
#the test ID provide the order of testes. 
#Spec: 
#How/NeedToKnow:  
class UTGeneral(unittest.TestCase):
    #local
    #ID:0-99
    def test_001_setting_signature(self):
        print("\nCheck signature and to see program started")
        self.assertEqual(gc.SETTING["SIGNATURE"],'waterswak')

    def test_002_cli_help(self):
        gc.CLI.do_help("")
        self.assertEqual(True,True)        


