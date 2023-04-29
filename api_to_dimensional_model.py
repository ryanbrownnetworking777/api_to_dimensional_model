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
def create_model_columns(columns, conversion_function, conversion_function_arguments):
   model_columns = [conversion_function(*[column + conversion_function_arguments]) for column in columns] 
   return model_columns

def isolate_dimension(df, dimension_columns,  new_dimension_columns):
    dimension_df = df[dimension_columns]
    dimension_df.columns = new_dimension_columns
    return dimension_df

def initialize_dimension(dimension_df, dimension_path, dimension_name):
    if os.path.isfile(dimension_path):
        print(f"{dimension_name} dimension is already initialized.")
        return 
    else:
        dimension_df = dimension_df.drop_duplicates()
        dimension_id_string = f'{dimension_name}_id'
        dimension_df[dimension_id_string] = dimension_df.index
        dimension_columns = [dimension_id_string] + list(dimension_df.columns)
        dimension_df = dimension_df[dimension_columns]
        today =  int(datetime.datetime.today().date().strftime("%y%m%d"))
        dimension_df['effective_from'] = today
        dimension_df['effective_till'] = 99990101
        dimension_df['is_active'] = 'Y'
        dimension_df.sort_values(dimension_id_string).to_csv(dimension_path, index=False) 
        print(f"{dimension_name} dimension initialized.")
        return 

def update_dimension(dimension_df, dimension_path, dimension_columns, dimension_name):
    dimension_disk_df = pd.read_csv(dimension_path)
    today =  int(datetime.datetime.today().date().strftime("%y%m%d"))
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
    
def process_dimension(df, dimension_path, dimension_columns, dimension_name, conversion_function, conversion_function_arguments):
    new_dimension_columns = create_model_columns(
                                                    columns=dimension_columns
                                                    ,conversion_function=conversion_function
                                                    ,conversion_function_arguments=conversion_function_arguments
                                                    )
    dimension_df = isolate_dimension(df=df, 
                                     dimension_columns=dimension_columns, 
                                     new_dimension_columns=new_dimension_columns)

    initialize_dimension(dimension_df=dimension_df 
                                        ,dimension_path=dimension_path 
                                        ,dimension_name = dimension_name
                                        )  
                                       
    update_dimension(dimension_df=dimension_df 
                     ,dimension_path=dimension_path 
                     ,dimension_columns=dimension_columns
                     ,dimension_name=dimension_name
                    )

# def isolate_fact(df, fact_columns, new_fact_columns):
#     fact_df = df[fact_columns]
#     fact_df.columns = new_fact_columns
#     return fact_df

def process_fact(df, fact_columns):

def initialize_fact(fact_df, fact_path, fact_name, fact_column_processing_dict): 
    if os.path.isfile(fact_path):
        print(f"{fact_name} fact is already initialized.")
        return 
    else:
        fact_df = fact_df.drop_duplicates()
        fact_id_string = f'{fact_name}_id'
        fact_df[fact_id_string] = fact_df.index
        fact_columns = [fact_id_string] + list(fact_df.columns)
        fact_df = fact_df[fact_columns]
        """
        Need to have functions here for turning the columns into ids
        Dictionary: 
            column_key - column to create:

        """
        for column_key in fact_column_processing_dict.keys():

        fact_df.sort_values(fact_id_string).to_csv(fact_path, index=False) 
        print(f"{fact_name} fact initialized.")
        return 
