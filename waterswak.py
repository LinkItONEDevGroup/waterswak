# @file waterswak.py
# @brief statup and AP mode selection
# MODULE_ARCH:  
# CLASS_ARCH:
# GLOBAL USAGE:
# @author wuulong@gmail.com
#standard
import getopt
import sys
import unittest

#extend
from configobj import ConfigObj

#library
import codes.globalclasses as gc
from codes.const import *
import codes.app as app
import codes.cli as cli
import codes.ut as ut
import codes.ui as ui

#Spec: program init, mode selection, start
#How/NeedToKnow:
if __name__ =='__main__':
    # Read system parameters which are assigned while we launching "start.py".
    # If the input parameter is invalid, then display usage and return "command
    # line syntax" error code.
    apmode = AP_MODE_NORMAL

    #command line paramters handler
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ht", [])
        for opt, arg in opts:
            if opt == '-h':
                print ('waterswak.py [ -h ] [ -t ]')
                print ('    -h, --help: help message')
                print ('    -t, --test: unit test mode')
                sys.exit()
            elif opt in ("-t", "--test"):
                apmode = AP_MODE_UNITTEST
                print("Running as unittest mode!")
    except getopt.GetoptError:
        print ('usage: waterswak.py [ -h ] [ -t ]')
        sys.exit(2) 
    
    

    #init global classes   
    gc.SETTING  = ConfigObj("include/waterswak.ini")
    gc.UI = ui.UserInterface()
    gc.GAP = app.SApp()
    gc.CLI = cli.Cli()
    
    
    #run by different mode
    if apmode == AP_MODE_UNITTEST:
        suite = unittest.TestLoader().loadTestsFromTestCase(ut.UTGeneral)
        unittest.TextTestRunner(verbosity = 2).run(suite)
    else:
        
        gc.CLI.cmdloop()
    
