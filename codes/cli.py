# @file cli.py
# @brief CLI of whole tool
# @author wuulong@gmail.com
#standard
import cmd
import logging
#extend
import pandas as pd
import pandasql as ps
#library
import codes.globalclasses as gc
from codes.const import *
from codes.db import *
from codes.lib import *
from codes.riverlog import *

##### Code section #####
#Spec: about, user commands, test commands
#How/NeedToKnow:
class Cli(cmd.Cmd):
    """Simple command processor example."""    
    def __init__(self,stdout=None):
        cmd.Cmd.__init__(self)
        self.cli_eng = CliEng(stdout)
        self.prompt = 'WsCLI>'
        pass
############ cli maintain ####################        
    def do_about(self, line):
        """About this software"""
        print("%s version: v%s" %(TITLE,VERSION))
    def do_quit(self, line):
        """quit"""
        return True
############ top command ####################                      
    def do_reset(self,line):
        """ reset for next run """  
        gc.GAP.reset()
        print("reseted")
    def do_reload_setting(self,line):
        """reload setting from INI"""
        gc.SETTING.reload()
        pd.set_option('display.max_rows', int(gc.SETTING["MAX_ROWS"]))
        pd.set_option('display.max_columns', int(gc.SETTING["MAX_COLUMNS"]))
        pd.set_option('display.max_colwidth', int(gc.SETTING["MAX_COLWIDTH"]))
    def do_status(self,line):
        """ show current status 
            status {desc_id}
            desc_id: 0-summary info, 1- detail info, 2- dot graph
            ex: status 1
        """
        pars=line.split()
        desc_id = 1
        if len(pars)==1:
            desc_id = pars[0]
        if desc_id=="connect":
            gc.GAP.conn=connect_db()
            if gc.GAP.conn is None:
                return "DB connection fail!"
            return ""
        #logging.info(gc.GAP.mm.desc(desc_id))


    def do_test(self,line):
        """ current testing """
        pass
    ############ ethmgr sub cmd ####################
    def do_eng(self,line):
        """ethernet boot and management sub command directory"""
        gc.GAP.conn=connect_db()
        self.cli_eng.prompt = self.prompt[:-1]+':eng>'
        self.cli_eng.cmdloop()

############ ethmgr sub cmd ####################
class CliEng(cmd.Cmd):
    def __init__(self,stdout):
        if stdout:
            cmd.Cmd.__init__(self,stdout=stdout)
        else:
            cmd.Cmd.__init__(self)

    def do_view_by_key(self, line):
        """ 看所有的 table 跟預設好的 sql
        沒給值會列出預設的 keys
        ex: view_by_key [rivercode]"""
        pars=line.split()
        if len(pars)==1:
            key = pars[0]
        else:
            dict = view_by_key(gc.GAP.conn,None)
            print("pre-defined keys=\n%s" %( dict.keys()))
            return
        df=view_by_key(gc.GAP.conn,key)
        print(df)

    def do_key_to_value(self, line):
        """
        ex:
        key_to_value b_河川 river_id 130000.0 river_cname
        """
        tb_name=key_column=key_value=value_column=""
        pars=line.split()
        if len(pars)==4:
            tb_name = pars[0]
            key_column = pars[1]
            key_value = pars[2]
            value_column = pars[3]
        else:
            return
        print(key_to_value(gc.GAP.conn,tb_name,key_column,key_value,value_column))

    def do_key_to_value_help(self, line):
        """
        ex:
        key_to_value_help [b_河川] [river_id]
        """
        tb_name=key_column=None
        pars=line.split()
        if len(pars)==2:
            tb_name = pars[0]
            key_column = pars[1]
        if len(pars)==1:
            tb_name = pars[0]
        key_to_value_help(gc.GAP.conn,tb_name,key_column)


    def do_quit(self, line):
        """quit this sub command"""
        if gc.GAP.conn:
            close_db(gc.GAP.conn)
        """quit"""
        return True
