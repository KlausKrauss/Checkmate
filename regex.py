import re
from itertools import groupby
from string import ascii_lowercase


def reg_breakout(string, delim=None)->dict:
    global string_split
    lower_case = set(ascii_lowercase) # set for faster lookup
    def find_regex(val)->str:
        for c in val:
            try:
                if c.isdigit():
                   return "d"
                elif c.isspace():
                   return "s"
                elif c.lower() in lower_case:
                    return "w"
                else:
                   return c
            except Exception as e:
                print(e)
    ##########################################
    details = []
    complete = {}
    try:
        position = 0
        if delim is not None: 
            string_split = string.split(delim)
        elif delim is None:
            string_split= [string]
            delim=''
        for sub_string in string_split:
            sub_list = []
        
            position += len(sub_string)+len(delim)
            for count, value in enumerate(sub_string):
                values=(count, find_regex(value),position,value)
                sub_list.append(values)
            details.append(sub_list)
    except Exception as e:
        print(e)
    ############################################
    try:
        key = 0 
        for f in details:
            key+=1
            list_a, list_b, positions, orig_vals = zip(*f)
            grp = groupby(list(list_b))
            reg_concat=''.join(f'\\{what}{{{how_many}}}' 
                           if how_many>1 else f'\\{what}' 
                           for what,how_many in ((g[0],len(list(g[1]))) for g in grp))
            orig_vals =''.join(orig_vals)
            if key != len(details):
                complete[key]= {'orig_vals':orig_vals,
                                'list_vals':''.join(list(list_b)),
                                'reg_concat':reg_concat,
                                'length': max(list(list_a))+len(delim),
                                'start_pos':  max(list(positions))-(max(list(list_a))+len(delim)),
                                'end_pos':    max(list(positions))}
            else:
                complete[key]= {'orig_vals':orig_vals,
                                'list_vals':''.join(list(list_b)),
                                'reg_concat':reg_concat,
                                'length': max(list(list_a)),
                                'start_pos':  (max(list(positions))-(max(list(list_a))))-len(delim),
                                'end_pos':    max(list(positions))-len(delim)}
    except Exception as e:
        print(e)
    finally:
        return complete
###################################################################################
###################################################################################

