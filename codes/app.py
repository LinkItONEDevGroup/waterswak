# @file app.py
# @brief The main module to maintain whole app
# @author wuulong@gmail.com
#standard
import logging
from datetime import datetime,timedelta

#extend
import waterswak
import matplotlib.dates as mdate
#library
import codes.ui as ui
import codes.globalclasses as gc
from codes.const import *

##### Code section #####
#Spec: simulation control, log managment, setting load/save
#How/NeedToKnow:
class SApp:
    def __init__(self):
        self.init_log()
        logging.info("LASS - %s version: v%s" %(TITLE,VERSION))
        self.conn = None
        #logging.info("%s" %(""))
        self.reset()
        
        
    def init_log(self):
        # set up logging to file - see previous section for more details
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='output/waterswak.log',
                            filemode='a')
        # define a Handler which writes INFO messages or higher to the sys.stderr
        console = logging.StreamHandler()
        console.setLevel(logging.INFO) #logging.INFO
        # set a format which is simpler for console use
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        # tell the handler to use this format
        console.setFormatter(formatter)
        # add the handler to the root logger
        logging.getLogger('').addHandler(console)
        
        # Now, we can log to the root logger, or any other logger. First the root...
        #logging.info('Logger initialed')
    
    def load_setting(self):
        pass
    def save_setting(self):
        pass
    def reset(self):
        gc.SETTING.reload()
        self.user_vars={}

