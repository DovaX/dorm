import dorm as dm
import pandas as pd

def initialize_dataframes_from_tables(db1,list_of_table_names):
    dorm_tables={}
    dfs={}
    for table_name in list_of_table_names:
        dorm_tables[table_name]=dm.Table.init_all_columns(db1,table_name)
        dfs[table_name]=dorm_tables[table_name].select_to_df()
    return(dorm_tables,dfs)


def initialize_dicts_between_dataframes(dfs,dictionary_definitions):
    #input list of lists of 4 strings: df_name,column1,column2,dict_name
    dicts={}
    for definition in dictionary_definitions:
        dicts[definition[3]]=dm.df_to_dict(dfs[definition[0]],definition[1],definition[2])  
    return(dicts)


#demand_unloading_address_dict=dm.df_to_dict(dfs['demands'],"id","unloading_address_id") #used for matching model
