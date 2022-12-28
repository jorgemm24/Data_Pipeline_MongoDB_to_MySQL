
from decouple import config 
from pymongo import MongoClient
from sqlalchemy import create_engine
import pymysql


def conn_mongo_cloud() -> str: 
    """
    Función para realizar la conexión a la base de datos MongoDB Atlas.
    Se puede incluir un parametro con la URI de conexión (opciónal)

    Returns:
        db: Retorna la conexión en una variable definida
    """
    try:
        print("Conectando a MongoCloud...")
        client = MongoClient(config("MONGO_DB_CLIENT"))
        db = client["sample_analytics"]
        print("Conexión Existosa a MongoCloud!!")
        print(f"Colecciones: {db.list_collection_names()}")
        return db
    except Exception as e:
        print(f"Error de conexión a MongoCloud -> {e}")



def conn_mysql() -> str:
    """
    Función para realizar la conexión a la base de datos MySQL

    Returns:
        engine: Retorna el engine de conexión
    """
    print("Conectando a MySQL...")
    try:
        HOST = config("HOST")
        DATABASE = config("DATABASE")
        USER = config("USER")
        PASSWORD = config("PASSWORD")

        connect_args={'ssl':{'fake_flag_to_enable_tls': True}}
        connect_string = f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}'
        engine = create_engine(connect_string,connect_args=connect_args)
        
        return engine
    except Exception as e:
        print(f"Error de conexión MySQL -> {e}")



def flatten_json(y: dict or list) -> dict:
    """
    Función para aplanar un tipo json (diccionario o lista)

    Args:
        y (dict or list): Puede recibir un parametro tipo diccionario o lista 
                        

    Returns:
        out: retorna un diciconario aplanado
    """
    out = {}
 
    def flatten(x: dict or list, name:str =''):
        """
        Función para aplanar un diccionario o lista, evalua si es el parametro x
        es un diccionario o una lista

        Args:
            x (dict or list): recibeun diccionario o lista
            name (str, optional): parametro para concatenar la llave con la lista aninada ( ejm llave_0, llave_1 ..) 
                                  o diccionario anidado (ejmp: llavex_llave1, llavex_llave2 ..). Defaults to ''.
        """
 
        # If the Nested key-value
        # pair is of dict type
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
 
        # If the Nested key-value
        # pair is of list type
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
 
    flatten(y)
    return out




def load(conn_mysql: str, df: str, table_dest: str, chunksize: int ):
    """
    Función para cargar la información extraida y procesada a la base de 
    datos destino

    Args:
        conn_mysql (str): Parametro de conexión a MySQL
        df (object): Dataframe con la información lista para cargar
        table_dest (str): tabla destino (MySQL) donde se carga la información procesada
        chunksize (int): porción de registros que se cargan al destino (MySQL)
    """
    print("Iniciando Función <load>")
    try:
        print("Cargando datos a MySQL")
        with conn_mysql.begin() as conn:
            df.to_sql(name=table_dest ,con=conn , if_exists="append", index=False, chunksize=chunksize)
            print("Proceso Existodo!!")
    except Exception as e: 
        print(f"Error durante el proceso de carga a MySQL -> {e}")



