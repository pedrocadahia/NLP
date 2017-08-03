#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Codi importa docs processats
# res = esw.search(index=indicewrite, doc_type="doc", body={"size": 1000, "query": {"match": {"documento": '03_002-008_14'}}}, sort="page")
# info = dict(texto = [], tipo = [], pagina = [])
# for ires in res['hits']['hits']:
#     info['texto'].append(ires['_source']['texto'])
#     info['tipo'].append(ires['_source']['tipo'])
#     info['pagina'].append(ires['_source']['pagina'])

# Codi genera un dataframe amb la info per comparar amb fidel
# aux = pd.DataFrame(info)
# aux = aux[aux["tipo"] == "LRR"]
# aux.reset_index()
# aux["pagina"] = aux["pagina"].astype('int')
# aux = aux.sort_values("pagina")
# aux.to_csv('/home/dcortiada/temp/comparativa_fidel.csv', sep = "|")

from elasticsearch import Elasticsearch


def posicionporpagina(datos, posicionespagina, varpagina = "pagina", varposini = "posRawDocIni", varposfin = "posRawDocFin"):

	for i in range(datos.shape[0]):
		pags = datos.loc[i, varpagina].split(",")
		if len(pags) == 1:
			restame = posicionespagina[int(pags[0]) - 1][0]
			datos.loc[i, varposini] = datos.loc[i, varposini] - restame
			datos.loc[i, varposfin] = datos.loc[i, varposfin] - restame
		else:
			restame = posicionespagina[int(pags[0]) - 1][0]
			datos.loc[i, varposini] = datos.loc[i, varposini] - restame
			restame = posicionespagina[int(pags[1]) - 1][0]
			datos.loc[i, varposfin] = datos.loc[i, varposfin] - restame
		
	return datos

def import_doc(doc, criterion, con, indice):
	# conexion a elasticsearch
	
	# revisar indice para consulta. index="aeacus_kdd". uri:aeacus:fs#9e33cbdbd42b4ec98b1e8d2080d64ed4
	res = con.search(index=indice, doc_type="document", body={"size": 1000, "query": {"match": {criterion: doc}}}, sort="page")
	text = ""
	for ires in res['hits']['hits']:
		text = text + ires['_source']['content']
	
	return text

def list_docs(criterion, con, indice):
	res = con.search(index=indice, doc_type="document", body={"fields" : criterion}) 
	all_names = []
	for element in res['hits']['hits']:
		all_names.extend(element['fields'][criterion])
	
	all_names = list(set(all_names))
	
	return all_names

def read_file_pagebypage(doc, criterion, con, indice):
	# conexion a elasticsearch
	
	# revisar indice para consulta. index="aeacus_kdd". uri:aeacus:fs#9e33cbdbd42b4ec98b1e8d2080d64ed4
	res = con.search(index=indice, doc_type="document", body={"size": 1000, "query": {"match": {criterion: doc}}}, sort="page")
	result = []
	for ires in res['hits']['hits']:
		result.append(ires['_source']['content'])
	
	return result
	
# create_index_ES("pruebas_ner", "172.22.248.206:9229")
# def create_index_ES(INDEX_NAME, es__host1):
# 	try:
# 		newesconn = Elasticsearch([es__host1])
# 	except 'connection error':
# 		print("CONNECTION ERROR")
# 
# 	# drop index
# 	if newesconn.indices.exists(INDEX_NAME):
# 		pass
# 		# print("Borrando el indice '%s'..." % (INDEX_NAME))
# 		# res = newesconn.indices.delete(index=INDEX_NAME)
# 		# print("response: '%s'" % (res))
# 	else:
# 		# Configuracion de particion de indices y replicacion
# 		request_body = {
# 			"settings": {
# 				"number_of_shards": 1,
# 				"number_of_replicas": 0
# 			}
# 		}
# 
# 		print('')
# 		print("Creando el indice '%s'..." % (INDEX_NAME))
# 		res = newesconn.indices.create(index = INDEX_NAME, body = request_body)
# 		print("response: '%s'" % (res))
# 		print('')



def load2Elastic(dict_files, INDEX_NAME, TYPE_NAME, newesconn):
	# dict_files: diccionari amb keys que seran els indexos, i conte diccionaris
	# amb tots els parametres que hem d'omplir de l'index

	bulk_data = []

	# Preparamos los datos a cargar en ES
	for ikey in dict_files.keys():
	
		data_dict = dict_files[ikey]
		doc = {
			"index": {
				"_index": INDEX_NAME,
				"_type": TYPE_NAME,
				"_id": ikey
			}
		}
		
		bulk_data.append(doc)
		bulk_data.append(data_dict)
		
	# bulk index the data
	# print("bulk indexing...")
	newesconn.bulk(index = INDEX_NAME, body = bulk_data, refresh = True, request_timeout = 60)
	# print("Finalizada la carga de datos")

def eliminaDoc(documento, INDEX_NAME, TYPE_NAME, newesconn):
	documento = documento.replace(".pdf", "").replace(".PDF", "")
	# Eliminamos datos del mismo documento si existen
	scroll = newesconn.search(index=INDEX_NAME, doc_type=TYPE_NAME, body={"size": 1000, "query": {"match": {"documento": documento}}})
	bulk = ""
	# En hits_number almacenamos el número de registros de la anterior consulta
	hits_number = scroll['hits'][u'total']
	# Si el número de registros es distinto de cero creamos el bulk para el borrado de datos masivo
	if hits_number != 0:
		for result in scroll['hits']['hits']:
			bulk = bulk + '{ "delete" : { "_index" : "' + str(result['_index']) + '", "_type" : "' + str(result['_type']) + '", "_id" : "' + str(result['_id']) + '" } }\n'
		newesconn.bulk(body=bulk)

	
## def load2Elastic(vec_list, INDEX_NAME, TYPE_NAME, newesconn):
## 	bulk_data = []
## 	i = 1
## 	filename = name	 # NO se usa :S
## 
## 	# Preparamos los datos a cargar en ES
## 	for data in vec_list:
## 		indexid = filename + "_" + str(i)
## 		data_dict = {
## 			"file": filename,
## 			"phrase": unicode(data[0], "utf-8"),
## 			"page": data[1],
## 			"score": data[2],
## 			"scroll": data[3]
## 		}
## 		doc = {
## 			"index": {
## 				"_index": INDEX_NAME,
## 				"_type": TYPE_NAME,
## 				"_id": indexid
## 			}
## 		}
## 		bulk_data.append(doc)
## 		bulk_data.append(data_dict)
## 		i = i + 1
## 
## 	# bulk index the data
## 	print("bulk indexing...")
## 	newesconn.bulk(index=INDEX_NAME, body=bulk_data, refresh=True, request_timeout=60)
## 	print("Finalizada la carga de datos")
## 
## '''	
## def load2Elastic(data2Load, con = es, indice = indicewrite):
## 	# data2Load: Lista con 
## 
## 	con.bulk(index=indice, body=bulk_data, refresh=True, request_timeout=60)
## 
## bulk_data = [doc(doc1), data(doc1), doc(doc2), data(doc2), doc(doc3), data(doc3)]	 
## 
## def load2Elastic(data2Load, vec_list, INDEX_NAME, TYPE_NAME, newesconn):
## 	bulk_data = []
## 	i = 1
## 	filename = name	 # NO se usa :S
## 
## 	# Preparamos los datos a cargar en ES
## 	for data in vec_list:
## 		indexid = filename + "_" + str(i)
## 		data_dict = {
## 			"file": filename,
## 			"phrase": unicode(data[0], "utf-8"),
## 			"page": data[1],
## 			"score": data[2],
## 			"scroll": data[3]
## 		}
## 		doc = {
## 			"index": {
## 				"_index": INDEX_NAME,
## 				"_type": TYPE_NAME,
## 				"_id": indexid
## 			}
## 		}
## 		bulk_data.append(doc)
## 		bulk_data.append(data_dict)
## 		i = i + 1
## 
## 	# bulk index the data
## 	print("bulk indexing...")
## 	newesconn.bulk(index=INDEX_NAME, body=bulk_data, refresh=True, request_timeout=60)
## 	print("Finalizada la carga de datos")
## 	
## 	
## def carga_1(name, vec_list, INDEX_NAME, TYPE_NAME, newesconn):
## 	bulk_data = []
## 	i = 1
## 	filename = name	 # NO se usa :S
## 
## 	# Preparamos los datos a cargar en ES
## 	for data in vec_list:
## 		indexid = filename + "_" + str(i)
## 		data_dict = {
## 			"file": filename,
## 			"phrase": unicode(data[0], "utf-8"),
## 			"page": data[1],
## 			"score": data[2],
## 			"scroll": data[3]
## 		}
## 		doc = {
## 			"index": {
## 				"_index": INDEX_NAME,
## 				"_type": TYPE_NAME,
## 				"_id": indexid
## 			}
## 		}
## 		bulk_data.append(doc)
## 		bulk_data.append(data_dict)
## 		i = i + 1
## 
## 	# bulk index the data
## 	print("bulk indexing...")
## 	newesconn.bulk(index=INDEX_NAME, body=bulk_data, refresh=True, request_timeout=60)
## 	print("Finalizada la carga de datos")
## 
## 
## def loadSummaryIntoElastic(name, vec_list, es__host2, es__host1):
## 	try:
## 		newesconn = Elasticsearch([es__host1])
## 	except 'connection error':
## 		print("CONNECTION ERROR")
## 
## 	INDEX_NAME = 'pruebas'
## 	TYPE_NAME = 'summary'
## 	# drop index
## 	if newesconn.indices.exists(INDEX_NAME):
## 		pass
## 		# print("Borrando el indice '%s'..." % (INDEX_NAME))
## 		# res = newesconn.indices.delete(index=INDEX_NAME)
## 		# print("response: '%s'" % (res))
## 	else:
## 		# Configuracion de particion de indices y replicacion
## 		request_body = {
## 			"settings": {
## 				"number_of_shards": 1,
## 				"number_of_replicas": 0
## 			}
## 		}
## 
## 		print('')
## 		print("Creando el indice '%s'..." % (INDEX_NAME))
## 		res = newesconn.indices.create(index=INDEX_NAME, body=request_body)
## 		print("response: '%s'" % (res))
## 		print('')
## 
## 	# antes de cargar de nuevo los datos en ES
## 	try:
## 		es = Elasticsearch([es__host2])
## 	except 'connection error':
## 		print("CONNECTION ERROR")
## 
## 	try:
## 		res = newesconn.search(index=INDEX_NAME, doc_type=TYPE_NAME, body={"query": {"match": {"file": filename}}},
## 								 sort="page")
## 	except:
## 		print('error')
## '''
## 