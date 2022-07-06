# -*- coding: utf-8 -*-
"""
Created on Tue Jul  5 20:10:12 2022

@author: 16148
"""
from PostgreSQL_Conn import PG
from Profiling_Class import Profiling
import pandas as pd
import os 
from getpass import getuser
import keyring

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

######################################################################
#create branch database manually
postgres1 = PG(config={'username':'postgres',
                      'password':keyring.get_password('covered_db', 'postgres'),
                      'host':'localhost',
                      'database':'branch',
                      'schema_name':'test_data'},active_logging=True)

#create schema 'test_data'
postgres1.ddl_deploy('''CREATE schema IF NOT EXISTS test_data ;''')


import re
try:
    for f in os.listdir():
        try:
            table_name = re.findall('\_(\w{1,})\.xlsx',f)
            print(f,table_name)
            if len(table_name) > 0:
                try:
                    data = pd.read_excel(f)
                    data.columns = data.columns.str.lower()
                    postgres1.full_upload_statement(data, f'{table_name[0]}', 'replace')
                except Exception as e:
                    print(e)  
            else:
                pass
        except Exception as e:
            print(e)        
except Exception as e:
    print(e)
###############################################################################



