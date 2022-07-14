# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 17:38:31 2022

@author: Nicholas Krauss
@ NOAA API 

"""
import threading
import time 
import random
import string
import json
from datetime import datetime as dt
import os
import pandas as pd
import numpy as np
import keyring 
from getpass import getuser
import requests
import io
import collections
from tabulate import tabulate
####################################
from Logging_Class import Logging
from PostgreSQL_Conn import PG
from Profiling_Class import Profiling
####################################
import warnings

pd.options.mode.chained_assignment = None 

datestring = dt.now().strftime('%Y%m%d_%H%M%S')
log = Logging(task='NOAA_API', folder= fr'C:\Users\{getuser()}\Documents', filename=f'NOAA_API_{datestring}.log',log_print=True)
log.logging_folder()
os.chdir(log.folder)

postgres = PG(config={'username':'postgres',
                     'password':keyring.get_password('covered_db', 'postgres'),
                     'host':'localhost',
                     'database':'postgres',
                     'schema_name':'noaa_app'},active_logging=True)

def api_call(thread_num,url):
    try:
        token = keyring.get_password('NOAA',getuser())
        r = requests.get(url, headers={'token':token})
        assert r.status_code == 200 and r.text is not None
        assert len(r.text) >2
    except Exception as e:
        if AssertionError:
            if r.status_code == 429:
                print(r.text)
                time.sleep(random.randint(5, 15))
                log.logging_func('I',f'THREAD:{thread_num}, RETRYING {url}')
                api_call(thread_num, url)
            else:
                pass
        else:
            log.logging_func('E',f'THREAD:{thread_num},{e}')
    if r.status_code == 200 and r.text is not None:
        return json.loads(r.text)
                     
def flatten(d, sep="_"):
    obj= collections.OrderedDict()
    def recurse(t, parent_key=" "):
        if isinstance(t,list):
            for i in range(len(t)):
                recurse(t[i],parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k,v in t.items():
                recurse(v,parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t
            
    recurse(d)
    return obj

def flatten_further(f):
    try:
        data = flatten(f)
        data = pd.DataFrame.from_dict(data, orient='index')
        data = data.reset_index()
        regex='(\d+)'
        series = data['index']
        column_num = series.str.extract(regex)
        column_num = column_num.rename(columns={0:'update'})
        data = pd.merge(data, column_num, how='outer', left_index=True, right_index=True)
        data['index'] = data['index'].replace('_results_\d+_','', regex=True).replace('\s','', regex=True).str.lower()
        data = data.rename(columns={'index':'column', 0:'value','update':'num'})
        data =  data[(data["column"].notna())& data["column"].apply(lambda x:x not in ['_metadata_resultset_offset','_metadata_resultset_limit','_metadata_resultset_count'])]
        data_piv = data.pivot_table(index='num', columns='column',values='value',aggfunc=np.max)
    except Exception as e:
        log.logging_func('E',f'{e}')
    return data_piv


def api_pull(thread_num, station=[]):
    global complete_threads, active_calls, curated_data
    curated_data = []
    def api_lockup():
        while len(active_calls) >=5:
            time.sleep(.01)
        active_calls.append(current_call:=''.join(random.choice(string.ascii_letters) for x in range(25)))
        time.sleep(1)
        return current_call
    try:
        url_base ='https://www.ncdc.noaa.gov/cdo-web/api/v2/data'
        ghcnd =[]
        stations_remaining = len(station)+1
        stations_completed =-1
        for f in station:
            stations_remaining -= 1
            stations_completed +=1
            try:
                log.logging_func('I',f'STARTING THREAD:{thread_num}, {f},STATIONS COMPLETED: {stations_completed}, STATIONS REMAINING: {stations_remaining}')
                for x in range(2012,2022):
                    for z in range(0,99999,1000):
                        ##########################
                        current_call = api_lockup()
                        api_max= api_call(thread_num,f'{url_base}?datasetid=GHCND&offset={z}&limit=1000&stationid={f}&startdate={x}-01-01&enddate={x}-12-31&includemetadata:false')
                        active_calls.remove(current_call)
                       ############################
                        if (isinstance(api_max, dict) and len(api_max) == 0):
                            log.logging_func('I',f'THREAD:{thread_num},{f}, {x}, {z} FINISHED')
                            break
                        if not isinstance(api_max, dict):
                            pass
                        else:
                            ghcnd.append(api_max)  
            except Exception as e:
                log.logging_func('E',f'{e}')   
    except Exception as e:
        log.logging_func('E',f'{e}')
    finally:
        curated = []   
        for f in ghcnd:
            data = flatten(f)
            data = flatten_further(data)
            curated.append(data)
        complete = pd.concat(curated)
        complete.insert(0,'as_of_date',dt.now())
        complete = complete.drop_duplicates()
        curated_data.append(complete)
        complete_threads.append('X')
        
def thread_up(splits):
    global starttime
    starttime = time.perf_counter()
    log.logging_func('I',f'STARTING BULK THREADS')
    threads = [None] * len(splits)
    try:
        for i in range(len(threads)):
            try:
                threads[i] = threading.Thread(target=api_pull,name=(f'THREAD {i}'), args=(i,splits[i],))
                threads[i].start() 
            except Exception as e:
                log.logging_func('E',f'{e}')   
    except Exception as e:
        log.logging_func('E',f'{e}')
    finally:
        while len(complete_threads) != len(splits):
            continue
        complete = pd.concat(curated_data)
        log.logging_func('I',f'ENDING BULK THREADS')
        try:
            postgres.full_upload_statement(complete, 'raw_data_ghcnd_raw', 'append')
            complete.to_parquet(f'upload_{datestring}_{len(complete)}.gzip',compression='gzip')
            log.logging_func('I',f'ENDING BULK THREADS')
        except Exception as e:
            complete.to_parquet(f'upload_{datestring}_{len(complete)}.gzip',compression='gzip')
            log.logging_func('E',f'{e}') 
        
######################################################################
if __name__ == "__main__":
    try:
        stations = postgres.query(f'''
          SELECT * 
            FROM noaa_app.stations 
            where name like '%, OH US%'
            and id like 'GHCND%'
            and datacoverage::numeric = 1
            and extract(year from maxdate::date) = extract(year from now())
        	and extract(year from mindate::date) <= extract(year from now())-10
            ORDER BY maxdate DESC
        ''')
        splits = np.array_split(stations['id'], 12)
        complete_threads = []
        active_calls = []
        
        thread_up(splits)
        complete = postgres.query(''' 
                       SELECT * FROM raw_data_ghcnd_raw
                       WHERE _datatype in ('TAVG','TMAX','TMIN','TOBS')
                       
                       ''')
        
        temps = pd.DataFrame(["TAVG","TMAX","TOBS","TMIN"],columns=['TEMPS'])
        temp_convert = lambda x:((pd.to_numeric(x)/10.0)*1.8+32)
        merge = temps.merge(complete, how='inner', left_on="TEMPS",right_on='_datatype')
        merge['_value'] = temp_convert(merge['_value'])
        merge = merge[['as_of_date', '_attributes', '_datatype', '_date', '_station','_value']]
        postgres.full_upload_statement(merge, 'raw_data_temp', 'replace')
 
    except Exception as e:
        log.logging_func('E',f'{e}')
