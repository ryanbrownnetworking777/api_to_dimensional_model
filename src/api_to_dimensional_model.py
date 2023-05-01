import datetime
import requests
from base64 import b64encode
import time
from io import BytesIO
import os
import pandas as pd
import yaml

def load_configs(yaml_path):
    return yaml.safe_load(yaml_path)

"""
`conversion_function` here is a python function that will take a string, and output a new string according to whatever transformations are necessary. 
conversion_function_arguments is a list of arguments that will be passed positionally to the `conversion_function`
"""
def create_dimension_columns(columns, conversion_function, conversion_function_arguments={}):
    if conversion_function_arguments != {}:
        conversion_function_arguments['columns'] = columns
        print(conversion_function_arguments)
        model_columns = conversion_function(**conversion_function_arguments)
    else:
        model_columns = conversion_function(columns)
    return model_columns

def isolate_dimension(df, dimension_columns,  new_dimension_columns):
    dimension_df = df[dimension_columns]
    dimension_df.columns = new_dimension_columns
    return dimension_df

def initialize_dimension(dimension_df, dimension_check_function, dimension_check_function_arguments, dimension_name, saving_function, saving_function_arguments={}) -> None:
    if dimension_check_function(**dimension_check_function_arguments):
        print(f"{dimension_name} dimension is already initialized.")
        return 
    else:
        dimension_df = dimension_df.drop_duplicates().reset_index(drop='index')
        dimension_id_string = f'{dimension_name}_id'
        dimension_columns = [dimension_id_string] + list(dimension_df.columns)
        dimension_df[dimension_id_string] = dimension_df.index
        dimension_df = dimension_df[dimension_columns]
        today =  int(datetime.datetime.today().date().strftime("%Y%m%d"))
        dimension_df['effective_from'] = today
        dimension_df['effective_till'] = 99990101
        dimension_df['is_active'] = 'Y'
        dimension_df = dimension_df.fillna("N/A")
        if saving_function_arguments != {}:
            saving_function_arguments['df'] = dimension_df
            saving_function(**saving_function_arguments)
        else:
            saving_function(df=dimension_df)
        print(f"{dimension_name} dimension initialized.")
        return 

def append_dimension(dimension_df, dimension_path, dimension_columns, dimension_name):
    dimension_disk_df = pd.read_csv(dimension_path)
    today =  int(datetime.datetime.today().date().strftime("%Y%m%d"))
    new_dimension_df = pd.merge(
            dimension_df[dimension_columns]
            ,dimension_disk_df[dimension_columns]
            ,how='outer'
            ,indicator=True
            )
    new_dimension_df = new_dimension_df[(new_dimension_df._merge == 'left_only')].drop('_merge',axis=1)
    new_dimension_df['effective_from'] = today
    new_dimension_df['effective_till'] = 99990101
    new_dimension_df['is_active'] = 'Y'
    dimension_id_string = f"{dimension_name}_id"
    max_id = dimension_df[dimension_id_string].max()
    new_dimension_df[dimension_id_string] = [(val + max_id+1) for val in new_dimension_df.index]
    dimension_df = pd.concat([dimension_disk_df, new_dimension_df]) 
    dimension_df.sort_values(dimension_id_string).to_csv(dimension_path, index=False) 
    print(f"{dimension_name} dimension updated.")
    return
    
def deactivate_dimension_entries(dimension_df, entries_to_deactivate_df, dimension_path) -> None:
    today =  int(datetime.datetime.today().date().strftime("%Y%m%d"))
    to_deactivate_entries = pd.merge(
            dimension_df
            ,entries_to_deactivate_df
            ,how='outer'
            ,on = list(entries_to_deactivate_df.columns)
            ,indicator=True
            )
    to_deactivate_entries_locations = to_deactivate_entries[(to_deactivate_entries._merge == '_both')].index
    effective_till = list(dimension_df['effective_till'])
    effective_till[to_deactivate_entries_locations] = today
    is_active = list(dimension_df['is_active'])
    is_active[to_deactivate_entries_locations] = 'N'
    dimension_df['effective_till'] = effective_till
    dimension_df['is_active'] = is_active
    dimension_df.to_csv(dimension_path, index=False)
    return



def process_dimension(df, dimension_columns, dimension_name, conversion_function, conversion_function_arguments, dimension_check_function, dimension_check_function_arguments, saving_function, saving_function_arguments) -> None: 
    new_dimension_columns = create_dimension_columns(
                                                    columns=dimension_columns
                                                    ,conversion_function=conversion_function
                                                    ,conversion_function_arguments=conversion_function_arguments
                                                    )
    dimension_df = isolate_dimension(df=df, 
                                     dimension_columns=dimension_columns, 
                                     new_dimension_columns=new_dimension_columns)
    # return dimension_df
    initialize_dimension(dimension_df=dimension_df 
                                        ,dimension_name = dimension_name
                                        ,dimension_check_function=dimension_check_function
                                        ,dimension_check_function_arguments=dimension_check_function_arguments
                                        ,saving_function=saving_function
                                        ,saving_function_arguments=saving_function_arguments
                                        )  
                                       
    # append_dimension(dimension_df=dimension_df 
    #                  ,dimension_path=dimension_path 
    #                  ,dimension_columns=dimension_columns
    #                  ,dimension_name=dimension_name
    #                 )
    return 
# def isolate_fact(df, fact_columns, new_fact_columns):
#     fact_df = df[fact_columns]
#     fact_df.columns = new_fact_columns
#     return fact_df
"""
Columns in the API results will be grouped by whatever id they will ultimately map up too. 
A dictionary will specify the following:
    {

    "new_column":
        {
        
            "processing_function": processing_function
            ,"processing_function_arguments": [arguments as list] 
        }
    
    }
"""
def string_columns_to_integer_id(df, id_column_name, columns, loading_function, loading_function_arguments):
    dimension_df = loading_function(loading_function_arguments)
    id_df = df[columns]
    id = pd.merge(
            id_df
            ,dimension_df
            ,how='inner'
            ,on=columns
            )[id_column_name]
    return id
    

def create_fact(df, fact_column_processing_dict):
    new_columns = [column_key for column_key in fact_column_processing_dict.keys()]
    for new_column in new_columns:
        column_dict = fact_column_processing_dict[new_column]
        df[new_column] = column_dict['processing_function'](*column_dict['processing_function_arguments'])
    new_df = df[new_columns]
    return new_df

def initialize_fact(fact_df, fact_path, fact_name): 
    if os.path.isfile(fact_path):
        print(f"{fact_name} fact is already initialized.")
        return 
    else:
        fact_id_string = f"{fact_name}_id" 
        fact_df[fact_id_string] = fact_df.index
        fact_df.sort_values(fact_id_string).to_csv(fact_path, index=False) 
        print(f"{fact_name} fact initialized.")
        return 

def append_fact(fact_df, fact_path, fact_columns, fact_name):
    fact_disk_df = pd.read_csv(fact_path) 
    new_fact_df = pd.merge(
            fact_df[fact_columns]
            ,fact_disk_df[fact_columns]
            ,how='outer'
            ,indicator=True
            )
    new_fact_df = new_fact_df[(new_fact_df._merge == 'left_only')].drop('_merge',axis=1)
    fact_id_string = f"{fact_name}_id"
    max_id = fact_df[fact_id_string].max()
    new_fact_df[fact_id_string] = [(val + max_id+1) for val in new_fact_df.index]
    fact_df = pd.concat([fact_disk_df, new_fact_df]) 
    fact_df.sort_values(fact_id_string).to_csv(fact_path, index=False) 
    print(f"{fact_name} fact updated.")
    return

def process_fact(df, fact_path, fact_columns, fact_name, fact_column_processing_dict) -> None: 
    fact_df = create_fact(df=df, fact_column_processing_dict=fact_column_processing_dict)

    initialize_fact(fact_df=fact_df 
                                        ,fact_path=fact_path 
                                        ,fact_name = fact_name
                                        )  
                                       
    append_fact(fact_df=fact_df 
                     ,fact_path=fact_path 
                     ,fact_columns=fact_columns
                     ,fact_name=fact_name
                    )
    return 
