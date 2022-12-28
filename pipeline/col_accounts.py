
from pipeline.functions import flatten_json
import pandas as pd


def extract_transform(conn_mongo: str) -> object:
    """
    Extra información de MongoDB Atlas, la colección "accounts"

    Args:
        conn_mongo (str): Conexión a MongoDB Atlas

    Returns:
        df : Retorna un dataframe de la collecion "accounts"
    """
    
    print("Iniciando Función <extract_transform>")
    lista = []
    sin_registro = "no_product" 

    for doc in conn_mongo["accounts"].find({}):  
        dic = {}
        try:
            dic["id"] = str(doc['_id'])
            dic["account_id"] = doc["account_id"]
            dic["limite"] = doc["limit"]
            dic["products"] = str(doc["products"]).replace("[","").replace("]","").replace(" ","").replace("'","").strip()
            dic["n_products"] = len(doc["products"])
            all_products = flatten_json(doc["products"]) 
            dic["product_1"] = all_products.get("0",sin_registro)
            dic["product_2"] = all_products.get("1",sin_registro)
            dic["product_3"] = all_products.get("2",sin_registro)
            dic["product_4"] = all_products.get("3",sin_registro)
            dic["product_5"] = all_products.get("4",sin_registro)
        except Exception as e:
            print(e)

        lista.append(dic)

    print(f"Registros de la colleción: {len(lista)}")

    df = pd.DataFrame(lista)
    return df










