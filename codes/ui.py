# @file ui.py
# @brief User interface related, import/export
# @author wuulong@gmail.com
#standard

#extend
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
#library
import codes.globalclasses as gc
from codes.const import *

##### Code section #####
#Spec: user interface : plot
#How/NeedToKnow:
class UserInterface():
    def __init__(self):
        #private
        #global: these variables allow to direct access from outside.
        self.plot_seq=0
        pass
    # data to plot files  
    def plot(self,data_x,data,label, desc):
        plt.xlabel('Date')
        
        #p = plt.plot(data,label=label)
        if len(data_x)-1==len(data):
            plt.ylabel('Patients day increase rate')
            p = plt.plot(data_x[:-1],data,label=label)
            sr_y = []
            for i in range(1, len(gc.MODEL.srs.sr_x)):
                sr_y.append(float(gc.MODEL.srs.sr_y[i])/gc.MODEL.srs.sr_y[i-1])
            p = plt.plot(gc.MODEL.srs.sr_x[1:],sr_y,label=gc.MODEL.srs.record_file)

        else:
            plt.ylabel('Patients Count')
            p = plt.plot(data_x,data,label=label)
            p = plt.plot(gc.MODEL.srs.sr_x,gc.MODEL.srs.sr_y,label=gc.MODEL.srs.record_file)
        plt.xticks(rotation=22.5)
        plt.legend()
        
        filename = "output/figure-%i.png" %(self.plot_seq)
        logging.info("output figure-%i, desc=%s" %(self.plot_seq, desc))
        plt.savefig(filename)
        self.plot_seq+=1
        plt.close()
        #plt.show()
        
            
