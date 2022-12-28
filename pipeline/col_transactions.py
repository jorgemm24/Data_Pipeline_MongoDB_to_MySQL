import pandas as pd


def extract_transform(conn_mongo: str) -> object:
    """
    Función para extraer la colección "transactions" de MongoCloud Atlas
    
    Args:
        conn_mongo (str): Conexión a MongoDB

    Returns:
        df: Retorna un dataframe con la colección procesada 
    """
    
    
    print("Iniciando Función <extract_transform>")
    lista = []

    for doc in conn_mongo["transactions"].find({}):
        dic = {}
        try:
            dic["id"] = str(doc["_id"])
            dic["account_id"] = doc["account_id"]
            dic["transaction_count"] = doc["transaction_count"]
            dic["bucket_start_date"] = doc["bucket_start_date"]
            dic["bucket_end_date"] = doc["bucket_end_date"]     
            
            lista.append(dic)
        except Exception as e:
            print(e)
        
    print(f"Registros de la colleción: {len(lista)}")

    df = pd.DataFrame(lista)
    return df


