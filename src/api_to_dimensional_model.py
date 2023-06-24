import datetime
import pandas as pd
import yaml
import numpy as np

def load_configs(yaml_path):
    return yaml.safe_load(yaml_path)

"""
`conversion_function` here is a python function that will take a string, and output a new string according to whatever transformations are necessary. 
conversion_function_arguments is a list of arguments that will be passed positionally to the `conversion_function` via the ** operator. `create_dimension_columns` assumes that at least one argument in the `conversion_function` is `columns` 
"""
def create_dimension_columns(columns, conversion_function, conversion_function_arguments={}):
    if conversion_function_arguments != {}:
        conversion_function_arguments['columns'] = columns
        print(conversion_function_arguments)
        model_columns = conversion_function(**conversion_function_arguments)
    else:
        model_columns = conversion_function(columns=columns)
    return model_columns

"""
Returns a pandas dataframe filtered down to the exact columns needed; with those columns renamed with new names. 
"""
def isolate_dimension(df, dimension_columns,  new_dimension_columns) -> pd.DataFrame:
    dimension_df = df[dimension_columns]
    new_dimension_column_mapping = {dimension_columns[i]: new_column for i, new_column in enumerate(new_dimension_columns)}
    dimension_df.rename(mapper=new_dimension_column_mapping, axis=1, inplace=True)
    # dimension_df.columns = new_dimension_columns
    return dimension_df

def initialize_dimension(dimension_df,  dimension_name) -> pd.DataFrame:
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
    print(f"{dimension_name} dimension initialized.")
    return dimension_df 

def append_dimension(dimension_df, existing_dimension_df, dimension_name, dimension_columns) -> pd.DataFrame:
    today =  int(datetime.datetime.today().date().strftime("%Y%m%d"))
    new_dimension_df = pd.merge(
            dimension_df[dimension_columns]
            ,existing_dimension_df[dimension_columns]
            ,how='outer'
            ,indicator=True
            )
    new_dimension_df = new_dimension_df[(new_dimension_df._merge == 'left_only')].drop('_merge',axis=1).drop_duplicates().reset_index(drop='index')
    new_dimension_df['effective_from'] = today
    new_dimension_df['effective_till'] = 99990101
    new_dimension_df['is_active'] = 'Y'
    dimension_id_string = f"{dimension_name}_id"
    max_id = existing_dimension_df[dimension_id_string].max()
    new_dimension_df[dimension_id_string] = [int(val + max_id+1) for val in new_dimension_df.index]
    dimension_df = pd.concat([existing_dimension_df, new_dimension_df]) 
    dimension_df[dimension_id_string] = dimension_df[dimension_id_string].astype(int)
    print(f"{dimension_name} dimension updated with {len(new_dimension_df)} new records.")
    return dimension_df
    
def deactivate_dimension_entries(dimension_df, entries_to_deactivate_df )-> pd.DataFrame:
    today =  int(datetime.datetime.today().date().strftime("%Y%m%d"))
    to_deactivate_entries = pd.merge(
            dimension_df
            ,entries_to_deactivate_df
            ,how='outer'
            ,on = list(entries_to_deactivate_df.columns)
            ,indicator=True
            )
    to_deactivate_entries_locations = list(to_deactivate_entries[(to_deactivate_entries._merge == 'both')].index)
    print(to_deactivate_entries_locations)
    effective_till = (dimension_df['effective_till']).copy()
    effective_till[to_deactivate_entries_locations] = today
    is_active = (dimension_df['is_active']).copy()
    is_active[to_deactivate_entries_locations] = 'N'
    dimension_df['effective_till'] = effective_till
    dimension_df['is_active'] = is_active
    # print(f"{len(to_deactivate_entries_locations)} deactivated from dimension {dimension_name}")
    return dimension_df

'''
Conversion function is a function that will take a list of columnsa and return another list of columns in the order desired for the final dimension table. Conversion function arguments are arguments passed as a dictionary and expanded using ** notation in the processing function. table_check_function will return a boolean for whether or not the table already exists, and arguments are passed as a dictionary and expanded using ** notation.  
'''

#def execute_saving_function(saving_function, saving_function_arguments) -> None:


def process_dimension(df, dimension_columns, dimension_name, conversion_function, conversion_function_arguments, table_check_function, table_check_function_arguments, saving_function, saving_function_arguments, loading_function, loading_function_arguments) -> None: 
    new_dimension_columns = create_dimension_columns(
                                                    columns=dimension_columns
                                                    ,conversion_function=conversion_function
                                                    ,conversion_function_arguments=conversion_function_arguments
                                                    )
    dimension_id_string = f'{dimension_name}_id'
    print(dimension_id_string)
    dimension_df = isolate_dimension(
                                     df=df, 
                                     dimension_columns=dimension_columns, 
                                     new_dimension_columns=new_dimension_columns
                                     )
    dimension_check = table_check_function(**table_check_function_arguments) 
    print(dimension_check)
    if  dimension_check == False:
        dimension_df = initialize_dimension(
                                        dimension_df=dimension_df 
                                        ,dimension_name = dimension_name
                                        )  
        if saving_function_arguments != {}:
            saving_function_arguments['df'] = dimension_df
            if 'additional_processing' in saving_function_arguments.keys():
                additional_processing_string = saving_function_arguments['additional_processing']
                compiled_additional_processing = compile(additional_processing_string,'<string>','exec')
                eval(compiled_additional_processing)
                saving_function_arguments.pop('additional_processing')
                for key in saving_function_arguments.keys():
                    print(key)
            saving_function(**saving_function_arguments)
        else:
            saving_function(df=dimension_df)
    elif dimension_check == True:
        existing_dimension_df = loading_function(**loading_function_arguments)
        dimension_df = append_dimension(
                        dimension_df=dimension_df 
                        ,existing_dimension_df=existing_dimension_df 
                        ,dimension_columns=new_dimension_columns
                        ,dimension_name = dimension_name
                        )
        if saving_function_arguments != {}:
            saving_function_arguments['df'] = dimension_df
            if 'additional_processing' in saving_function_arguments.keys():
                additional_processing_string = saving_function_arguments['additional_processing']
                compiled_additional_processing = compile(additional_processing_string,'<string>','exec')
                eval(compiled_additional_processing)
                saving_function_arguments.pop('additional_processing')
                for key in saving_function_arguments.keys():
                    print(key)
            saving_function(**saving_function_arguments)
        else:
            saving_function(df=dimension_df)
    return 

"""
Columns in the API results will be grouped by whatever id they will ultimately map up too. 
A dictionary will specify the following:
    {

    "new_column":
        {
        
            "processing_function": processing_function
            ,"processing_function_arguments": {arguments as dict} 
        }
    
    }
"""
def string_columns_to_integer_id(df, id_column_name, fact_columns, dimension_columns, loading_function, loading_function_arguments):
    dimension_df = loading_function(**loading_function_arguments)
    id_df = df[fact_columns]
    id_df['Original Order'] = [i for i in range(0, len(id_df))]
    id = pd.merge(
            id_df
            ,dimension_df
            ,how='inner'
            ,left_on=fact_columns
            ,right_on=dimension_columns
            ).sort_values(by='Original Order')[id_column_name]
    return id
    

def create_fact(df, fact_column_processing_dict,fact_name='') -> pd.DataFrame:
    new_columns = [column_key for column_key in fact_column_processing_dict.keys()]
    for new_column in new_columns:
        column_dict = fact_column_processing_dict[new_column]
        df[new_column] = [val for val in (column_dict['processing_function'](**column_dict['processing_function_arguments']))]
    if fact_name !="":
        fact_id_string = f"{fact_name}_id"
        to_concat_columns = [column_key for column_key 
                             in fact_column_processing_dict.keys() 
                             if fact_column_processing_dict[column_key]["id_column"]==True]
        df[fact_id_string] =  df[to_concat_columns].apply(lambda x: ''.join(x.astype(str)), axis=1)
        new_columns = [fact_id_string] + new_columns
    new_df = df[new_columns]
    return new_df


"""
To Do -- Switch this to accepting a criteria function with arguments so any number of cases can be handled.
Function written will have to accept fact_df and existing_fact_df variables in addition to whatever arguments passed into the append function
"""
def append_fact(fact_df, fact_name, existing_fact_df, new_fact_df_function, new_fact_df_function_arguments) -> pd.DataFrame:
    new_fact_df_function_arguments['existing_fact_df'] = existing_fact_df
    new_fact_df_function_arguments['fact_df'] = fact_df
    new_fact_df = new_fact_df_function(**new_fact_df_function_arguments)
    appended_len = len(new_fact_df)
    new_fact_df = pd.concat([existing_fact_df, new_fact_df]).drop_duplicates()
    print(f"{fact_name} fact appended with {appended_len} new records.")
    return new_fact_df

def process_fact(df, fact_name, fact_column_processing_dict, table_check_function, table_check_function_arguments, saving_function, saving_function_arguments, loading_function, loading_function_arguments, new_fact_df_function, new_fact_df_function_arguments)  -> None: 
    fact_df = create_fact(df=df, fact_column_processing_dict=fact_column_processing_dict, fact_name=fact_name)
    fact_check = table_check_function(**table_check_function_arguments) 
    print(fact_check)
    if 'additional_processing' in saving_function_arguments.keys():
        fact_id_string = f"{fact_name}_id"
        print(fact_id_string)
    if  fact_check == False:
        if saving_function_arguments != {}:
            saving_function_arguments['df'] = fact_df
            if 'additional_processing' in saving_function_arguments.keys():
                additional_processing_string = saving_function_arguments['additional_processing']
                compiled_additional_processing = compile(additional_processing_string,'<string>','exec')
                eval(compiled_additional_processing)
                saving_function_arguments.pop('additional_processing')
                for key in saving_function_arguments.keys():
                    print(key, saving_function_arguments[key])
            saving_function(**saving_function_arguments)
        else:
            saving_function(df=fact_df)
        print(f"Fact table {fact_name} initalized.")
    elif fact_check == True:
        existing_fact_df = loading_function(**loading_function_arguments)
        print(new_fact_df_function_arguments)
        new_fact_df_function_arguments['existing_fact_df'] = existing_fact_df
        new_fact_df_function_arguments['fact_df'] = fact_df
        fact_df = append_fact(fact_df=fact_df 
                              ,existing_fact_df=existing_fact_df 
                              ,fact_name=fact_name
                              ,new_fact_df_function=new_fact_df_function
                              ,new_fact_df_function_arguments=new_fact_df_function_arguments).drop_duplicates()
        if saving_function_arguments != {}:
            saving_function_arguments['df'] = fact_df
            if 'additional_processing' in saving_function_arguments.keys():
                additional_processing_string = saving_function_arguments['additional_processing']
                print(additional_processing_string)
                compiled_additional_processing = compile(additional_processing_string,'<string>','exec')
                eval(compiled_additional_processing)
                saving_function_arguments.pop('additional_processing')
                for key in saving_function_arguments.keys():
                    print(key, saving_function_arguments[key])
            saving_function(**saving_function_arguments)
        else:
            saving_function(df=fact_df)
        print(f"Fact table {fact_name} updated.")
    return 
