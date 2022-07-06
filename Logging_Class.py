# -*- coding: utf-8 -*-
"""
Created on Sat Dec 18 18:14:50 2021

@author: Nicholas Krauss
"""
import time
from datetime import datetime as dt
import os
import pandas as pd

class Logging:

    '''
    Lay a log with Kraussy Kakes' Logging Function
    1. init task and log folder via Logging('<taskname>','<path for log file>'))
        1a. create log folder if does not exist via 'log.logging_folder()'
    #############################################################################    
    2. Get logging add wherever needed in your code with free form text messages
       REMEMBER! 'Frank' your string or else I'll shittalk your code behind your back.
       I-[INFO], D-[DEBUG], W-[WARNING], E-[ERROR], C-[CRITICAL]
    '''
    def __init__(self, 
                 task='unknown', 
                 folder = os.getcwd(),
                 datestring = dt.strftime(dt.now(),'%Y%m%d_%H%M%S'),
                 starttime = time.perf_counter(),
                 row_num=0,
                 log_list = [],
                 filename ='undefined',
                 log_print = False
                 ):
        self.task = task
        self.folder = os.path.join(folder,task)
        self.datestring = datestring
        self.starttime = starttime
        self.row_num = row_num
        self.filename  = filename
        self.log_print = log_print
        self.log_list = []
        
        return print(f'[TASK]:{self.task}\n[FOLDER]:{self.folder}\n[DATESTRING]:{self.datestring}\n[INIT_TIME]:{self.starttime}\n[FILENAME]:{self.filename}')
    def logging_folder(self):  
        try:
            os.mkdir(self.folder)
            print(f'CREATED {self.folder}')
        except Exception as e:
               print(e)
        try:
            if self.filename == 'undefined':
                self.filename =  os.path.join(self.folder,f'{self.task}_{self.datestring}.log')
            else:
                self.filename = os.path.join(self.folder,self.filename)
        except Exception as e:
               print(e)
               
    def logging_time(self):
        return str(round((time.perf_counter() - self.starttime),2))

    def logging_func(self, log_level, message):
        try:
            log_types = pd.DataFrame({'valid_types' : ['[INFO]','[DEBUG]','[WARNING]','[ERROR]','[CRITICAL]'],
                                      'valid_entry' : ['I','D','W','E','C']})
            log_level_displayed = log_types[log_types['valid_entry']==log_level]
            log_level_displayed = log_level_displayed['valid_types'].item()
    
        except ValueError:
               log_level_displayed='[UNDEFINED]'       
        try:
            self.row_num += 1
            elapsed = Logging.logging_time(self)
            log_body = f'{dt.now()}~{self.task}~{log_level_displayed}~{self.row_num}:{message}~{elapsed}'
            #capture in script log
            self.log_list.append([log_body])
            if self.log_print is not False:
                print(log_body)
        except Exception as e:
               print(e)
        try:
            with open(self.filename, "a") as file_object:
                file_object.write(f"{log_body}\n")   
        except Exception as e:
                print(e)

    def logging_df(self):
        try:
            print(len(self.log_list))
            log_df = pd.DataFrame(self.log_list,columns=['DATA'])
            log_df = log_df['DATA'].str.split('~',n=5, expand=True)
            log_df = log_df.rename(columns={0:'DATETIME',1:'TASK',2:'LOG_TYPE',3:'MESSAGE',4:'ELAPSED'})
        except Exception as e:
               print(e)    
        return log_df
 





