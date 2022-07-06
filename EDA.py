# -*- coding: utf-8 -*-
"""
Created on Tue Jul  5 20:10:12 2022

@author: 16148
"""
from Profiling_Class import Profiling
import pandas as pd
import os 
from getpass import getuser

user = getuser()
os.chdir(rf'C:\Users\{user}\Downloads\Homework')
os.listdir()

profiles = []
try:
    for f in os.listdir():
        try:
            data = pd.read_excel(f)
            profiles.append(Profiling(data,f).report_generation(special_char_check=True,list_delim=',\n'))
        except Exception as e:
            print(e)        
except Exception as e:
    print(e)
    
finally:
    complete = pd.concat(profiles)