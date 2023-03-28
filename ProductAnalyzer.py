from database import Database
from save_json import writeAJson

db = Database(database="loja_de_roupas", collection="vendas")
db.reset_database()

result1 = db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$cliente_id", "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},

])
writeAJson(result1, "total_gasto_clienteB")

result2 = db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$produtos.nome", "total": {"$sum": "$produtos.quantidade"}}},
    {"$sort": {"total": 1}},
    {"$limit": 1}
])
writeAJson(result2, "produto_menos_vendido")

result3= db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": {"cliente": "$cliente_id", "data": "$data_compra"}, "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
    {"$sort": {"_id.data": 1, "total": 1}},
    {"$group": {"_id": "$_id.data", "cliente": {"$first": "$_id.cliente"}, "total": {"$first": "$total"}}}
])
writeAJson(result3, "cliente_menos_Gastou")

result4 = db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$produtos.nome", "total": { "$gt" : [ "$Acima de 2", 2 ]}}},
])
writeAJson(result4, "produtos_vendasAcima_2")
