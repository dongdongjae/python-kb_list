import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from db_params import db_params

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()


def create_table():
    
    file_path = 'xlsx/kb_v2.1.0_map_unlock.xlsx'
    
    engine = create_engine(
        f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
    
    cond_table_name = 'condition'
    sent_table_name = 'sentence'
    map_table_name = 'kb_v2_map'  
    
    cond_sheet = pd.read_excel(file_path, sheet_name='condition')
    sent_sheet = pd.read_excel(file_path, sheet_name='sentence')
    map_sheet = pd.read_excel(file_path, sheet_name='kb_v2.1_map')
    
    cond_sheet.to_sql(cond_table_name, con=engine, index=False, if_exists='replace')
    sent_sheet.to_sql(sent_table_name, con=engine, index=False, if_exists='replace')
    map_sheet.to_sql(map_table_name, con=engine, index=False, if_exists='replace')


create_table()