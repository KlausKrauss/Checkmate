"""
Created on Thu Jan 13 00:04:47 2022
@author: Nicholas Krauss
"""
import os
import time
import pandas as pd
import numpy as np
from datetime import datetime as dt
import random
import string
import warnings
import threading
import gc

pd.options.mode.chained_assignment = None

class Profiling:
    '''
    pass a DataFrame, and object name
        <object_name> = Profiling(<df>, <df_name>)
    then generate report:
        <object_name>.report_generation()
    '''
    def __init__(self, df=[], object_name='unknown'):
        self.df = df,
        self.object_name = object_name
        self.data_profiled = []
        self.threads_completed =[]

        
    def columnar_progress_bar(self, attributes, sync_starttime):
        global session, current_call, width_remaining, bracket_count, starttime
        starttime = sync_starttime
        if 'session' not in globals():
            session = []
            current_call = None
        try:
            if current_call not in session:
                session.append(current_call:=''.join(random.choice(string.ascii_letters) for x in range(5)))
                width_remaining = attributes['column_num']
                init_message= f'''[PROFILING DIMENSIONS: COLUMNS = {attributes['column_num']} | ROWS = {attributes['length']} | TOTAL_SIZE = {attributes['size']}]'''
                bar_length = len(init_message) 
                bracket_count = round(bar_length/attributes['column_num']-1)
                
                if (bar_length/attributes['column_num']) < 1:
                    bracket_count = 0
                else:
                    bracket_count = round(bar_length/attributes['column_num']-1)
                print(f'[DATASET NAME]: "{self.object_name}"\n{init_message}\n[', end='')
            else:
                pass
            if current_call != None:
                assert width_remaining > 1
                value = ('='*bracket_count+'+')
                width_remaining -=1   
                return print(value, end='\r')
        except AssertionError:
                final_value = (f"=100%>]\n[TOTAL TIME = {round((time.perf_counter()-starttime),2)} |  PER COLUMN AVG = {round((time.perf_counter()-starttime)/attributes['column_num'],2)}]\n")
                session.remove(current_call)
                current_call = None
                return print(final_value)
        
    def report_generation(self, special_char_check=False, df=None, columnar_progress_bar=True, output=False, threaded=False, dup_count=10, list_delim=', '):
        '''
        EXAMPLE:
            profile = Profiling(query_base2, object_name ='NOAA Postgres').report_generation()
        
        PARAMETERS:
            _________________________________________________________________________________________
            df:                    |default=None     *Only used by thread_up() for DataFrame slices
            columnar_progress_bar: |default=True     *If set to false will not produce a progress bar
            output:                |default=False    *If set to true will output an excel
            threaded:              |default=False    *Note threaded is only used by thread_up()
            _________________________________________________________________________________________
        
        OUTPUT: DataFram and Excel Output if 'output' == True
        ------------------------------------------------------------------------
         [DATASET NAME]: "NOAA Postgres"
         [PROFILING DIMENSIONS: COLUMNS = 9 | ROWS = 2343546 | TOTAL_SIZE = 21091914]
         [=======+=======+=======+=======+=======+=======+=======+=======+=100%>]
         [TOTAL TIME = 132.33 |  PER COLUMN AVG = 14.7]      
     
        '''
        def dtypes(df):
            try:
                df = df[df['value'].notna()]
                dtypes = (df[['value']].applymap(type).apply(pd.value_counts).fillna(0))
                dtypes.reset_index(inplace=True)
                if count:=len(dtypes['index']) > 1:
                    dtypes['combined'] = '[DATATYPE]: '+dtypes['index'].astype(str)+' [COUNT]: '+dtypes['value'].astype(str)
                    dtypes = ';\n'.join(dtypes['combined'])    
                else:
                    dtypes['combined'] = dtypes['index'].astype(str)
                    dtypes = ';\n'.join(dtypes['combined'].to_list())  
        
            except Exception as e:
                dtypes = 'unknown'  
            finally:
                if dtypes == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>" or dtypes == "<class 'datetime.datetime'>":
                    dtypes = 'datetime'
                if dtypes == "<class 'int'>" or dtypes == "<class 'float'>":
                    dtypes = 'number'
                if dtypes == "<class 'str'>" or dtypes == "<class 'bool'>":
                    dtypes = 'string'
            return dtypes, count
        
        num_convert = lambda x: pd.to_numeric(x,errors='coerce')
        sum_total= lambda x: np.sum(x) if (x.dtype =='float64' or x.dtype == 'int64') else None    
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=FutureWarning)
                if df is None:
                    df = self.df[0]
                    sync_starttime = time.perf_counter()
                    attributes ={'size':df.size,'column_num':len(df.columns),'length':len(df),}
                profile = pd.DataFrame(data=df.columns.to_list(), columns=['column_name']).reset_index()
                profile.rename(columns={'index':'ordinal_position'},inplace=True)
        
                melt = df.melt(var_name='variable', value_name='value')
                ######################
                if threaded == False:
                    del self.df
                if threaded == True:
                    del df
                ######################
                melt = melt.replace(r'^\s*$',np.nan, regex=True)
                zert_list = [] 
                slices = []
                zert = profile, melt
                zert_list.append(zert)
            
            for a,b in zert_list:
                for column in a['column_name']:
                    try:
                        if columnar_progress_bar == True:
                            self.columnar_progress_bar(attributes,sync_starttime)
                        else:
                            pass
                        profile_slice = a[a['column_name']==column]
                        melt_slice = b[b['variable']==column]
                        #############################
                        #profile datatypes
                        dtype = dtypes(melt_slice)
                        if dtype[1]  == True:
                            melt_slice['value'] = melt_slice['value'].astype(str)
                        #############################
                        if special_char_check == True:
                            try:
                                char_base = melt_slice.astype(object)
                                leading_ws  = r'(^\s{1,})'
                                trailing_ws = r'(\s{1,}$)'
                                non_alpha_numeric = r'([^0-9A-Za-z\s])'
                                trailing_ws = char_base['value'].str.extractall(pat=trailing_ws).replace(r'\s{1,}','<trailing_whitespace>',regex=True)
                                trailing_ws.reset_index(inplace=True)
                                leading_ws = char_base['value'].str.extractall(pat=leading_ws).replace(r'\s{1,}','<leading_whitespace>',regex=True)
                                leading_ws.reset_index(inplace=True)
                                alpha_num = char_base['value'].str.extractall(pat=non_alpha_numeric)
                                alpha_num.reset_index(inplace=True)
                                ws = pd.concat([trailing_ws,leading_ws,alpha_num])
                                ws= ws.groupby([0]).count()
                                ws.reset_index(inplace=True)
                                ws['common_values'] = '['+ws[0].astype(str)+']: '+ws['match'].astype(str)
                                ws = ws.sort_values(by=['match'],ascending=False)
                                ws = f'{list_delim}'.join(list(ws['common_values']))
                                del char_base
                            except Exception as e:
                                del char_base 
                                ws = ''
                        #############################
                        group_by = melt_slice.reset_index()
                        group_by= group_by.groupby(['variable','value']).count()
                        group_by.reset_index(inplace=True)
                        group_by = group_by[group_by['index']>1]
                        group_by['common_values'] = '['+group_by['value'].astype(str)+']: '+group_by['index'].astype(str)
                        group_by = group_by.sort_values(by=['index'],ascending=False)
                        group_by = f'{list_delim}'.join(list(group_by['common_values'])[:dup_count])

                        #############################
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", category=RuntimeWarning)
                            merge_b = pd.DataFrame({
                                                    'column_name': [column],
                                                    'dtypes':[dtype[0]],
                                                    f'duplicate_top_{dup_count}':[group_by],
                                                    'sample_data': [f'{list_delim}'.join(list(set(melt_slice['value'].drop_duplicates().head(n=20).astype(str))))],
                                                    #numerics
                                                    'std_value': [round(np.array(num_convert(melt_slice['value'])).std(),2)],
                                                    'max_value': [np.array(num_convert(melt_slice['value'])).max()],
                                                    'median_value': [round(num_convert(melt_slice['value']).median(),2)],
                                                    'min_value': [np.array(num_convert(melt_slice['value'])).min()],
                                                    'sum_total': [sum_total(num_convert(melt_slice['value']))],
                                                    #counts
                                                    'count_distinct':[len(set(melt_slice['value'].astype(str)))],
                                                    'total_count':[len(melt_slice['value'])],
                                                    'distinct_perc': [round(len(set(melt_slice['value'].astype(str)))/(len(melt_slice['value']))*100,4)],
                                                    #nulls
                                                    'null_count': [(melt_slice['value'].isna().sum())],
                                                    'null_perc': [round(((melt_slice['value'].isna().sum())/(len(melt_slice['value'])))*100,2)],
                                                    # data lengths
                                                    'max_length':[melt_slice['value'].astype(str).map(len).max()],
                                                    'avg_length':[round(sum(map(len, melt_slice['value'].astype(str)))/len(melt_slice['value']))],
                                                    'min_length':[melt_slice['value'].astype(str).map(len).min()]
                                                    ,
                                                    })
                            if special_char_check == True:
                                merge_b['special_chars'] = ws
                            else:
                                pass
                            del melt_slice
                            slices.append(profile_slice.merge(merge_b, how='outer', on='column_name'))
                    except Exception as e:
                        print(e)
            profile_complete = pd.concat(slices)
            profile_complete.insert(0,'object_name',self.object_name)
            column_count = len(profile_complete.columns)
            profile_complete.insert(column_count,'runtime',dt.now())
        except Exception as e:
            print(e)
        finally:
            if output == True:
                profile_complete.to_csv(f"{self.object_name}_{dt.now().strftime('%Y%m%d_%H%M%S')}.csv",index=False)
            else:
               pass
            if threaded == True:
                
                self.threads_completed.append('X')
                self.data_profiled.append(profile_complete)
               
        return profile_complete 

    def thread_up(self,special_char_check=False, output=False):
        '''
        I created the thread_up() to reach the stars, but she's gone much, much farther than that. 
        She tore a hole in our universe, a gateway to another dimension. A dimension of pure profiled datum.
        
        EXAMPLE:
            profile2 = Profiling(query_base2, object_name = 'NOAA Parquet').thread_up()
        PARAMETERS:
            output:                |default=False    *If set to true will output an excel
        OUTPUT: profiling dataframe, example printing output below:
        ------------------------------------------------------------------------
            [DATASET NAME]: "NOAA Parquet"
            [PROFILING DIMENSIONS: COLUMNS = 9 | ROWS = 2343546 | TOTAL_SIZE = 21091914 | THREADS = 9]
            THREAD 0 [NOAA Parquet: COLUMN: "yyyymm"] STARTING
            THREAD 1 [NOAA Parquet: COLUMN: "as_of_date"] STARTING
            THREAD 2 [NOAA Parquet: COLUMN: "_attributes"] STARTING
            THREAD 3 [NOAA Parquet: COLUMN: "_datatype"] STARTING
            THREAD 4 [NOAA Parquet: COLUMN: "_date"] STARTING
            THREAD 5 [NOAA Parquet: COLUMN: "_station"] STARTING
            THREAD 6 [NOAA Parquet: COLUMN: "_value"] STARTING
            THREAD 7 [NOAA Parquet: COLUMN: "datatype_name"] STARTING
            THREAD 8 [NOAA Parquet: COLUMN: "station_name"] STARTING
            [X]
            [X][X][X]
            [X][X][X][X][X]
            [X][X][X][X][X][X]
            [X][X][X][X][X][X][X]
            [X][X][X][X][X][X][X][X][X]
            [TOTAL TIME = 176.9]
        
        '''
        try:
            df = self.df[0]
            threads = len(self.df[0].columns)
            starttime = time.perf_counter()
    
            assert len(set(df.columns.to_list())) == len(df.columns.to_list()), "{len(set(df.columns.to_list()))} UNIQUE COLUMN NAMES OUT OF {len(df.columns.to_list())} TOTAL COLUMNS"
            profile = pd.DataFrame(data=df.columns.to_list(), columns=['column_name']).reset_index()
            profile.rename(columns={'index':'ordinal_position'},inplace=True)
            
            if threads > 10:
                threads = 10
            init_message= f'''[PROFILING DIMENSIONS: COLUMNS = {len(df.columns)} | ROWS = {len(df)} | TOTAL_SIZE = {df.size} | THREADS = {threads}]\n'''
            print(f'[DATASET NAME]: "{self.object_name}"\n{init_message}', end='')
                            
            splits = np.array_split(df.columns, threads)
            thread = [None] * threads
            ###########################################################
            for i in range(threads):
                try:
                    columns_slice=splits[i]
                    df_slice = df[columns_slice]
                    thread[i] = threading.Thread(target=self.report_generation,name=(self.object_name+' THREAD {i}'), args=(special_char_check,df_slice,False,False,True,))
                    thread[i].start() 
                    del df_slice
                    print(f'THREAD {i} [{self.object_name}: COLUMN: "{", ".join(columns_slice.to_list())}"] STARTING\n',end='')
                except Exception as e:
                    print(e)
            del self.df
            gc.collect()
            ############################################################
            curr_completed = 0 
            while len(self.data_profiled) < threads:
                time.sleep(1)
                if len(self.threads_completed) > curr_completed:
                    curr_completed = len(self.threads_completed)
                    print('['+']['.join(self.threads_completed)+']')
                continue
        except Exception as e:
            print(e)
        finally:
            final_value = (f'\n[TOTAL TIME = {round((time.perf_counter()-starttime),2)}]\n')
            print(final_value)
            profiles = pd.concat(self.data_profiled)
            profiles.drop(columns=['ordinal_position'],inplace=True)
            profile_complete = profile.merge(profiles, how='inner', on='column_name')
            return profile_complete
           ######################
            if output == True:
                try:
                    profile_complete.to_csv(f"{self.object_name}_{dt.now().strftime('%Y%m%d_%H%M%S')}.csv",index=False)
                except Exception as e:
                    print(e)
            #####################        
  # FutureWarning: Inferring datetime64[ns] from data containing strings is deprecated and 
  # will be removed in a future version. To retain the old behavior explicitly pass Series(data, dtype={value.dtype})
  # melt = df.melt()