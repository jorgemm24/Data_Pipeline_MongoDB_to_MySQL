from pipeline.functions import conn_mongo_cloud, conn_mysql, load
from pipeline.col_accounts import extract_transform as et_accounts
from pipeline.col_customers import extract_transform as et_customers
from pipeline.col_transactions import extract_transform as et_transactions
from pipeline.col_transactions_details import extract_transform as et_trasactions_details


def main():
    """
    Función principal el cual llama al package pipeline 
    """
    
    print()
    print('Función Main')
    print()
    
    # Variables en común
    conn_mongo = conn_mongo_cloud()   
    con_db_mysql = conn_mysql()
    
    # (Extract - Transform)
    df_accounts  = et_accounts(conn_mongo)  
    df_customers = et_customers(conn_mongo)
    df_trasactions = et_transactions(conn_mongo)
    df_transactions_details = et_trasactions_details(conn_mongo)
        
    # (Load)
    load(con_db_mysql, df_accounts, table_dest="accounts", chunksize=1000)
    load(con_db_mysql, df_customers, table_dest="customers", chunksize=500)
    load(con_db_mysql, df_trasactions, table_dest="transactions", chunksize=1000)
    load(con_db_mysql, df_transactions_details, table_dest="transactions_details", chunksize=10000 )
    
    print()
    print('Función Main Terminada')
    

if __name__=='__main__':
    main()
    
    