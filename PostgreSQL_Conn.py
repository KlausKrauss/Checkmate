# -*- coding: utf-8 -*-
"""
Created on Sat Feb 26 15:00:19 2022
@author: Nicholas Krauss
"""

import psycopg2
from sqlalchemy import create_engine
import pandas as pd #pyarrow 
from Logging_Class import Logging
from datetime import datetime as dt
import time
import io
import threading
import psutil
import numpy as np

class PG(object):
    '''
    1.) instantiate your class with a dict containing the following key value pairs
        -username
        -password
        -host
        -database
        -schema_name
    
    2.) indicate if you wish to include active_logging or just have messages printed to the screen
    3.) when complete close your connection with PG.connection[0].close()
    
    '''
    def __init__(self, config, active_logging=False): 
        try:
            config_blank = {'username':'','password':'','host':'','database':'','schema_name':''}
            key1= set(config_blank.keys())
            for key, value in config.items():
                setattr(self, key, value)
            assert key1 == set(config.keys()), f'REQUIRED: {key1}\nRECEIVED: {set(config.keys())}'

            self.active_logging = active_logging
            if self.active_logging == True:
                self.log = self.logging()
                
            self.connection = self.pg_conn()
        except Exception as e:
            print(e)
    
    def logging(self):
        try:
            datestring = dt.strftime(dt.now(),'%Y%m%d_%H%M%S')
            log = Logging(task=f'{self.host}_{datestring}', 
                          filename=f'{self.host}_{datestring}.log',log_print=True)
            log.logging_folder()
        except Exception as e:
            print(e)
        return log
    
    def pg_conn(self):
        '''
        This is instantiated with the class above, all dict values are passed to and create the connection
        connection [0], cursor [1], and engine [2]
        '''
        try:
            engine = create_engine(f'postgresql+psycopg2://{self.username}:{self.password}@{self.host}:5432/{self.database}?options=-csearch_path%3D{self.schema_name}')
            conn = engine.raw_connection()
            cursor = conn.cursor()
            if self.active_logging == True:
                self.log.logging_func('I', f'CONNECTED TO {self.host}')
            else:   
                print(f'CONNECTED TO {self.host}')
        except Exception as e:
            if self.active_logging == True:
                self.log.logging_func('E',f'{e}')
            else:
                print(e)
                
        return [conn,cursor,engine]

    def query(self, sql_syntax):
        '''
        Simply pass correct and complete sql syntax and receive a dataframe
        
        Ex. 'Select to_char(now()::date,'YYYYMMDD')::numeric::money'
                        -TIME EQUALS MONEY
        '''
        try:
            t = time.perf_counter()
            data = pd.read_sql(sql_syntax,self.connection[0])
            elapsed = round((time.perf_counter()-t)/60,2)
            print(f'{len(data)},{elapsed}')
            if self.active_logging == True:
                self.log.logging_func('I', f'\n***SQL SYNTAX START***\n\t{sql_syntax}\n***SQL SYNTAX END***\n[ROW COUNT]:{len(data)},[ELAPSED]:{elapsed}')   
            else:   
                print(f'\n***SQL SYNTAX START***\n\t{sql_syntax}\n***SQL SYNTAX END***\n[ROW COUNT]:{len(data)},[ELAPSED]:{elapsed}')  
        except Exception as e:        
            if self.active_logging == True:
                self.log.logging_func('E',f'{e}')
            else:
                print(e)
        return data   

    def full_upload_statement(self, df, table_name, replace_or_append):
        '''
        Pass the following
        1.) 'dataframe'
        2.) 'table_name' NOTE: this is going to create table in the schema of the connection_object
        3.) indicate either 'replace' or 'append' if the table does not exist and you select 'append'
            it will create new.
        NOTE: this process will create a second session.
        '''
        try:
            
            df.head(0).to_sql(table_name, self.connection[2], if_exists= replace_or_append, index=False)    
            conn = self.connection[0]
            cur =  self.connection[1]
            output = io.StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            cur.copy_from(output, table_name, null="")
            conn.commit()
            if self.active_logging == True:
                self.log.logging_func('I', f'SUCCESSFULLY LOADED {table_name} {len(df)} ROWS TOTAL')
            else:   
                print(f'SUCCESSFULLY INSERTED {table_name} ROWS TOTAL')  
        except Exception as e:
            cur.execute('''ROLLBACK''')
            cur.close()
            conn.commit()
            if self.active_logging == True:
                self.log.logging_func('E',f'{e}')
            else:
                print(e)               

    def insert_statement(self,insert_query,df):
        '''
        For a more nuanced loading and to take advantage of column defaults,
        as in incrementing serial columns, or for UPSERT 
        
        Pass the following:
            1.) the insert SQL query 
                i.e. INSERT INTO <table> (<columns>) VALUES {}
            2.) the dataframe containing the columns to be loaded
        
        '''
        try:
            cur = self.connection[1]
            load = list(df.itertuples(index=False, name=None))
            dynamic_insert = ','.join(['%s']*len(load))
            insert_query_format = f'''{insert_query}'''.format(dynamic_insert)
            cur.execute(insert_query_format,load)
            self.connection[0].commit()

            if self.active_logging == True:
                self.log.logging_func('I', f'\n***SQL SYNTAX START***\n\t{insert_query}\n***SQL SYNTAX END***\nSUCCESSFULLY INSERTED {len(df)} ROWS TOTAL')
            else:   
                print(f'\n***SQL SYNTAX START***\n\t{insert_query}\n***SQL SYNTAX END***\nSUCCESSFULLY INSERTED {len(df)} ROWS TOTAL')    
       
        except Exception as e:
            cur.execute('''ROLLBACK''')
            self.connection[0].commit()
            if self.active_logging == True:
                self.log.logging_func('E',f'{e}')
            else:
                print(e)

    def ddl_deploy(self,sql):
        '''
        To run DDL and create new objects in PostgreSQL
        NOTE: you can create objects outside of your principal connection
        schema 
        '''
        try:
            cur = self.connection[1]
            cur.execute(sql)
            self.connection[0].commit()
            if self.active_logging == True:
                self.log.logging_func('I', f'{sql} DEPLOYED')
            else:   
                print(f'{sql} DEPLOYED')           
        except Exception as e:
            cur.execute('''ROLLBACK''')
            self.connection[0].commit()
            if self.active_logging == True:
                self.log.logging_func('E',f'{e}')
            else:
                print(e)             
####################################################################################################
###################################################################################################

    def shinedown(self, table_name, order_by='ctid', threads=10, memory_threshold=90):
        '''
        
        '''
        if self.active_logging == True:
            self.log.logging_func('I',f'INITIATING SHINEDOWN ON {table_name}')
        else:
            print(f'INITIATING SHINEDOWN ON {table_name}')
        
        global completed_threads, lock 
        lock = None
        completed_threads = []

        def lockup():
            global lock 
            while lock is not None:
                continue
            lock = 'X'

        def offset_fetch(self, table_name, order_by):
            try:
                queries = []
                count = (self.query(f'''SELECT COUNT(*) FROM {table_name}''')).loc[0]
                ranger = range(0,count[0],round(count[0]/threads))
                thread = 0
                for f in ranger:
                    query_template = f'''SELECT '{thread}' AS THREAD, A.* FROM {table_name} A ORDER BY {order_by} OFFSET {f} FETCH NEXT {round(count[0]/threads)} ROWS ONLY'''
                    thread +=1
                    queries.append(query_template)
                split = np.array_split(queries, threads)
                print(' ;\n'.join(queries))
            except Exception as e:
                if self.active_logging == True:
                    self.log.logging_func('E',f'{e}')
            return split, count
            
        def threaded_query(thread_num, split,):
            global output, lock
            output = []
            try:
                queries_run = 1 
                for f in split:
                    try:
                        if self.active_logging == True:
                            self.log.logging_func('I',f'STARTING {thread_num}, {queries_run} OUT OF {len(split)}')
                        chunk = self.query(f)
                        lockup()
                        output.append(chunk)
                        lock = None
                        if self.active_logging == True:
                            self.log.logging_func('I',f'ENDING {thread_num}, {queries_run} OUT OF {len(split)}')
                            queries_run +=1
                    except Exception as e:
                        if self.active_logging == True:
                            self.log.logging_func('E',f'{e}')
    
            except Exception as e:
                if self.active_logging == True:
                    self.log.logging_func('E',f'{e}')
            finally:
                completed_threads.append('X')
                
        def purge_to_disk(mem, memory_threshold):
            global output, lock
            try:
                if mem >= memory_threshold:
                    if self.active_logging == True:
                        self.log.logging_func('C',f'PURGE TO DISK')
                    else:
                        print(f'PURGE TO DISK')
                    lockup()
                    output = pd.concat(output)
                    size = len(output)
                    output.to_parquet(f"{table_name}_{len(output)}_{len(completed_threads)}_of_{len(split)}_{dt.now().strftime('%Y%m%d_%H%M%S')}.gzip",compression='gzip')
                    output = []
                    if self.active_logging == True:
                        self.log.logging_func('C',f'PURGE TO DISK COMPLETE {size} ROWS, {len(completed_threads)} OF {len(split)} TOTAL THREADS')
                    else:
                        print(f'PURGE TO DISK COMPLETE')
                    lock = None
                else:
                    size = 0 
            except Exception as e:
                if self.active_logging == True:
                    self.log.logging_func('E',f'{e}')
            return size
        
        def thread_up(self,split,count):
            global mem 
            starttime = time.perf_counter()
            if self.active_logging == True:
                self.log.logging_func('I',f'STARTING BULK THREADS')
            threads = [None] * len(split)
            try:
                for i in range(len(threads)):
                    try:
                        time.sleep(2)
                        threads[i] = threading.Thread(target=threaded_query
                                                      ,name=(f'THREAD {i}'), 
                                                      args=(i,split[i],))
                        threads[i].start() 
                    except Exception as e:
                        if self.active_logging == True:
                            self.log.logging_func('E',f'{e}')
            except Exception as e:
                if self.active_logging == True:
                    self.log.logging_func('E',f'{e}')
            finally:
                purged_amt = 0 
                while len(completed_threads) != len(split):
                    if len(completed_threads) > 1:
                        purged = purge_to_disk(psutil.virtual_memory()[2],memory_threshold)
                        purged_amt += purged
                        remaining_count = count - purged_amt
                    else:
                        pass
                    continue
                if self.active_logging == True:
                    self.log.logging_func('I',f'ENDED BULK THREADS {round(time.perf_counter()-starttime,2)},{purged_amt} PURGED TO DISK, {remaining_count} IN MEMORY')
                
        details = offset_fetch(self, table_name,order_by)
        split = details[0]
        count = details[1].item()
        print(count)
        thread_up(self, split, count)
        if self.active_logging == True:
            self.log.logging_func('I',f'SHINEDOWN ON {table_name} COMPLETE')
        else:
            print(f'SHINEDOWN ON {table_name} COMPLETE')
        return pd.concat(output)

    # def shinedown_deux(self, obj_name, sql, threads=10, memory_threshold=90):
    #     '''
        
    #     '''
    #     if self.active_logging == True:
    #         self.log.logging_func('I',f'INITIATING SHINEDOWN ON {obj_name}')
    #     else:
    #         print(f'INITIATING SHINEDOWN ON {obj_name}')
        
    #     global completed_threads, lock 
    #     lock = None
    #     completed_threads = []

    #     def lockup():
    #         global lock 
    #         while lock is not None:
    #             continue
    #         lock = 'X'

    #     def offset_fetch(self, table_name):
    #         try:
    #             queries = []
    #             count = (self.query(f'''SELECT COUNT(*) FROM ({sql}) A''')).loc[0]
    #             ranger = range(0,count[0],round(count[0]/threads))
    #             for f in ranger:
    #                 query_template = f'''{sql} OFFSET {f} FETCH NEXT {round(count[0]/threads)} ROWS ONLY'''
    #                 queries.append(query_template)
    #             split = np.array_split(queries, threads)
    #         except Exception as e:
    #             if self.active_logging == True:
    #                 self.log.logging_func('E',f'{e}')
    #         return split, count
            
    #     def threaded_query(thread_num, split,):
    #         global output, lock
    #         output = []
    #         try:
    #             for f in split:
    #                 try:
    #                     chunk = self.query(f)
    #                     lockup()
    #                     output.append(chunk)
    #                     lock = None
    #                 except Exception as e:
    #                     if self.active_logging == True:
    #                         self.log.logging_func('E',f'{e}')
    
    #         except Exception as e:
    #             if self.active_logging == True:
    #                 self.log.logging_func('E',f'{e}')
    #         finally:
    #             completed_threads.append('X')
                
    #     def purge_to_disk(mem, memory_threshold):
    #         global output, lock
    #         try:
    #             if mem >= memory_threshold:
    #                 if self.active_logging == True:
    #                     self.log.logging_func('C',f'PURGE TO DISK')
    #                 else:
    #                     print(f'PURGE TO DISK')
    #                 lockup()
    #                 output = pd.concat(output)
    #                 size = len(output)
    #                 output.to_parquet(f"{obj_name}_{len(output)}_{len(completed_threads)}_of_{len(split)}_{dt.now().strftime('%Y%m%d_%H%M%S')}.gzip",compression='gzip')
    #                 output = []
    #                 if self.active_logging == True:
    #                     self.log.logging_func('C',f'PURGE TO DISK COMPLETE {size} ROWS, {len(completed_threads)} OF {len(split)} TOTAL THREADS')
    #                 else:
    #                     print(f'PURGE TO DISK COMPLETE')
    #                 lock = None
    #             else:
    #                 size = 0 
    #         except Exception as e:
    #             if self.active_logging == True:
    #                 self.log.logging_func('E',f'{e}')
    #         return size
        
    #     def thread_up(self,split,count):
    #         global mem 
    #         starttime = time.perf_counter()
    #         if self.active_logging == True:
    #             self.log.logging_func('I',f'STARTING BULK THREADS')
    #         threads = [None] * len(split)
    #         try:
    #             for i in range(len(threads)):
    #                 try:
    #                     time.sleep(1)
    #                     threads[i] = threading.Thread(target=threaded_query
    #                                                   ,name=(f'THREAD {i}'), 
    #                                                   args=(i,split[i],))
    #                     threads[i].start() 
    #                 except Exception as e:
    #                     if self.active_logging == True:
    #                         self.log.logging_func('E',f'{e}')
    #         except Exception as e:
    #             if self.active_logging == True:
    #                 self.log.logging_func('E',f'{e}')
    #         finally:
    #             purged_amt = 0 
    #             while len(completed_threads) != len(split):
    #                 if len(completed_threads) > 1:
    #                     purged = purge_to_disk(psutil.virtual_memory()[2],memory_threshold)
    #                     purged_amt += purged
    #                     remaining_count = count - purged_amt
    #                 else:
    #                     pass
    #                 continue
    #             if self.active_logging == True:
    #                 self.log.logging_func('I',f'ENDED BULK THREADS {round(time.perf_counter()-starttime,2)},{purged_amt} PURGED TO DISK, {remaining_count} IN MEMORY')
                
    #     details = offset_fetch(self, obj_name)
    #     split = details[0]
    #     count = details[1].item()
    #     print(count)
    #     thread_up(self, split, count)
    #     if self.active_logging == True:
    #         self.log.logging_func('I',f'SHINEDOWN ON {obj_name} COMPLETE')
    #     else:
    #         print(f'SHINEDOWN ON {obj_name} COMPLETE')
        
    #     return pd.concat(output)
############################################################################
############################################################################

