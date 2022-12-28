import pandas as pd


def extract_transform(conn_mongo: str) -> object:
    """
    Función para extraer los detalles de la colección "transactions" de MongoCloud Atlas
    
    Args:
        conn_mongo (str): Conexión a MongoDB

    Returns:
        df: Retorna un dataframe con la colección procesada 
    """
    
    print("Iniciando Función <extract_transform>")
    lista = []

    for doc in conn_mongo["transactions"].find({}): 
        try:
            for emb_doc in doc["transactions"]:
                dic = {}
                dic["account_id"] = doc["account_id"]
                dic["date"] = emb_doc["date"]
                dic["amount"] = emb_doc["amount"]
                dic["transaction_code"] = emb_doc["transaction_code"]
                dic["symbol"] = emb_doc["symbol"]
                dic["price"] = float(emb_doc["price"])
                dic["total"] = float(emb_doc["total"])

                lista.append(dic)
        except Exception as e:
            print(e)

        lista.append(dic)

    print(f"Registros de la colleción: {len(lista)}")
    
    df = pd.DataFrame(lista)
    return df



