import pandas as pd


def extract_transform(conn_mongo: str) -> object:
    """
    Función para extraer la colección "customers" de MongoCloud Atlas
    
    Args:
        conn_mongo (str): Conexión a MongoDB

    Returns:
        df: Retorna un dataframe con la colección procesada 
    """

    
    print("Iniciando Función <extract_transform>")

    lista = []

    for doc in conn_mongo["customers"].find({}): 
        dic = {}
        try:
            dic["id"] = str(doc["_id"])
            dic["username"] = doc["username"]
            dic["name"] = doc["name"]
            dic["address"] = doc["address"]
            dic["birthdate"] = doc["birthdate"]
            dic["email"] = doc["email"]
            dic["accounts"] = str(doc["accounts"]).replace("[","").replace("]","").replace(" ","").strip()
            dic["n_accounts"] = len(doc["accounts"])
        except Exception as e:
            print(e)
        
        lista.append(dic)

    print(f"Registros de la colleción: {len(lista)}")
    
    df = pd.DataFrame(lista)
    return df

