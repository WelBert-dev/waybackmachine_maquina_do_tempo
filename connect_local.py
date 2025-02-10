import pymongo
import urllib.parse

def conectarBanco():
    try:
        client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
        # client = pymongo.MongoClient("mongodb+srv://back:"+urllib.parse.quote("dEv#123BR")+"@podermonitor-v1.7uvqf.mongodb.net/?retryWrites=true&w=majority")
        print("Conexão ao banco bem sucedida!!")
        return client
    except:
        print("Erro ao conectar ao banco")