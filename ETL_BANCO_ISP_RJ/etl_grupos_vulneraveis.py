# %%
import pandas as pd
import numpy as np
import psycopg2 as pg
import locale
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema


# %%
locale.setlocale(locale.LC_TIME, 'pt_BR')
csv_1 = '/caminho_do_arquivo.csv'
csv_2 = '/caminho_do_arquivo.csv'
schema_Name = 'grupos_vulneraveis'
table_Name = 'todos'

# %%
def main() -> None:

    '''Main function that handles the pipeline flow. Steps:
        1. Calls the extract function to extract data from the CSV file and save it in a dataframe that will be used in the transform function.
        2. Calls the transform function to perform data modeling in the extracted dataframe and save the results in another dataframe that will be used in the lead function.
        3. Calls the load function to save the transformed dataframe in the database.

        Parameters:
        - None
    
        Returns:
        - None'''
    
    df_extracted = extract(csv_1, csv_2)
    df_transformed = transform(df_extracted)
    load(df_transformed, schema_Name, table_Name)

# %%
def extract(file_path_1: str, file_path_2: str) -> pd.DataFrame:

    '''Extract the data from two CSV files and save in one dataframe. Steps:
        1. Read the CSV files separately using pandas and save them in saparete dataframes;
        2. Join the two dataframes using pandas.

        Parameters:
        - String containing the first CSV file path;
        - String containing the second CSV file path.

        Return:
        - Joined dataframe.'''
    
    df1 = pd.read_csv(file_path_1, encoding = 'latin1', sep = ";")
    df2 = pd.read_csv(file_path_2, encoding = 'latin1', sep = ";")

    df = pd.concat([df1, df2], axis = 0, ignore_index = True)
    print("DataFrame criado com sucesso.")
    
    display(df.head(5))
    
    return df

# %%
def add_columns(df: pd.DataFrame) -> pd.DataFrame:

    '''Add new columns for the dataframe. Steps:
        1. Creates a new column and assigns the dataframe's update date to it.
        2. Creates a new column and assigns the extracted year from a specific date column to it using pandas.
        3. Creates a new column and assigns the extracted month from the same date column used to extract the year to it using pandas.

        Parameters:
        - Dataframe for which the columns will be added.

        Returns:
        - Dataframe with the added columns.'''

    
    df['atualizado_em'] = date.today()
    print("Coluna 'atualizado_em' adicionada ao DataFrame.")

    df['ano_fato']= pd.to_datetime(df['data_fato']).dt.year.astype(str).str.replace('.0', '')
    print("Coluna 'ano_fato' adicionada ao DataFrame.")

    df['mes_fato']= pd.to_datetime(df['data_fato']).dt.strftime('%B')
    print("Coluna 'mes_fato' adicionada ao DataFrame.")

    return df


# %%
def transform(df: pd.DataFrame) -> pd.DataFrame:
    '''Transform the dataframe by calling the add columns function and changing some columns types using pandas. Steps:
        1. Calls the add columns function and save in a dataframe;
        2. Uses pandas to convert the type of a date column from the dataframe from text to datetime and then to date;
        3. Uses pandas to convert the type of a hour column from the dataframe from text to datetime and then to time;
        4. Uses replace function to replace NULL values to a text;
        5. Uses pandas to convert the type of a age column from the dataframe from float to text and then remove unnecessaries character.
        
        Parameters:
        - Dataframe to be transformed.
        
        Return:
        - Dataframe transformed.'''

    df = add_columns(df)

    df['data_com'] = pd.to_datetime(df['data_com']).dt.date
    print("Coluna 'data_com' convertida para DATE.")

    df['hora_com'] = pd.to_datetime(df['hora_com'], format='%H:%M:%S').dt.time
    print("Coluna 'hora_com' convertida para TIME.")

    df = df.replace(np.nan, 'Não informado')
    print("Valores NULL do dataframe substituidos por 'Não informado'.")

    df['idade'] = df['idade'].astype(str).str.replace('.0', '')
    print("Coluna 'idade' convertida para STRING.")

    display(df.head(5))
    df.info()

    return df

# %%
def get_postgre_connection_parameters() -> dict:

    '''Creates a dict containing the connection parameters info.
        
        Parameters:
        - None
        
        Return:
        - The database connection or the error message.'''
        
    conn_parameters = {
        'host': 'host_do_banco',
        'database': 'nome_do_banco',
        'user': 'usuário',
        'password': 'senha'
    }

    return conn_parameters

# %%
def load(df: pd.DataFrame, schema_name: str, table_name: str) -> None:

    '''Load the transformed dataframe into the postgre database. Steps:
        1. Calls the postgre connection parameters function to receive the parameters;
        2. Creates a SQLAlchemy object for the conection using the parameters connection received;
        3. Sets the schema and creates it if doesn't exist;
        4. Loads the transformed dataframe into the database;
        5. Closes the connection;
        6. Prints a successfully message if the ingestion is complete;
        7. Prints the error message if it's not.
        
        Parameters:
        - Dataframe to be load;
        - String with the schema name where the table will be load;
        - String containing the name the table will be called.
        
        Return:
        - None.'''

    conn_params = get_postgre_connection_parameters()

    try:
        engine = create_engine(f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}/{conn_params['database']}")
        
        with engine.connect() as connection:
            connection.execute(CreateSchema(schema_name, if_not_exists=True))
            connection.commit()

        df.to_sql(table_name, con=engine, index=False, if_exists='replace', schema=schema_name)
        print(">> Ingestão de dados concluída com sucesso <<")
        print("Conexão fechada.")

    except Exception as error:
        print(f"Erro ao fazer a ingestão de dados no banco de dados: {error}")


# %%
if __name__ == "__main__":

    main()


