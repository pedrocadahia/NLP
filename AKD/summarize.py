# -*- coding: utf-8 -*-
# Conexion y Consulta elastisearch
# spark-submit --master yarn-cluster --deploy-mode cluster --py-files cod.py cod.py 172.22.248.206:9229 aeacus_kdd 172.22.248.206:9229 pruebas 03_002-008_14.PDF name aeacus/aeacus/aeacus##sij2016/172.22.248.221/5432 False
# cd /home/ijdocs/just_docs/akd/summarize; python summarize.py 172.22.248.206:9229 aeacus_dev 172.22.248.206:9229 akd_dev aeacus:fs#b2c8b153f65a42e7904c4c1f5fafb3ed/aeacus:fs#22414d7a6491497d8b2f031d1a050128/aeacus:fs#5a2dc471a31c4fd98a1fcffb86ffe02c/aeacus:fs#1a9a5a3baca1477d9523a5146213eea3 uri aeacus/aeacus/aeacus##sij2016/172.22.248.221/5432 False True

import sys

es_hdfs = False
if len(sys.argv) >= 9:
	if sys.argv[8] in ["True", "true", "TRUE", True]:
		es_hdfs = True
		
import sys

utiliza_logs = False
if len(sys.argv) >= 10:
	if sys.argv[9] in ["True", "true", "TRUE", True]:
		utiliza_logs = True
	
if utiliza_logs:
	import time
	old_stdout = sys.stdout
	sufix = time.strftime("%Y_%m_%d_%H_%M_%S")
	filelogsname = "/home/ijdocs/just_docs/akd/logs/filelogs" + sufix + ".log"
	log_file = open(filelogsname, "w")
	sys.stdout = log_file
	

from elasticsearch import Elasticsearch

if es_hdfs:
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext, HiveContext

	# Crear la configuración de spark
	conf = (SparkConf()
			.setAppName("summarize")
			.set("spark.executor.memory", "1g")
			.set("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/usr/bin/python")
			.set("spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON", "/usr/bin/python"))

	# Crear el SparkContext con la configuración anterior
	sc = SparkContext(conf=conf)

	# Conectores SQL para trabajar en nuestro script
	sqlContext = SQLContext(sc)
	hiveContext = HiveContext(sc)


if len(sys.argv) < 7:
	errorMessage = "No se han introducido todos los argumentos requeridos o se han introducido de manera incorrecta\n"
	errArg1 = " Arg 1: Cadena de conexion donde leer el documento. IP. Ej: 172.22.248.206:9229 \n"
	errArg2 = " Arg 2: Cadena de conexion donde guardar el resúmen. IP. Ej: 172.22.248.206:9229 \n"
	errArg3 = " Arg 3: Indice de donde se lee el documento \n"
	errArg4 = " Arg 4: Indice de donde se guarda el resúmen del documento \n"
	errArg5 = " Arg 5: Nombre del documento o identificador. Ej: 03_002-008_14.PDF \n"
	errArg6 = " Arg 6: Nombre del criterio de busqueda, Ej: name, uuid o uri... \n"

	raise Exception(errorMessage + errArg1 + errArg2 + errArg3 + errArg4 + errArg5 + errArg6)


def conn_query(es_search_host, index_name, doc_name, criterion_name):
	# creamos la conexion a elasticsearch con el host de consulta
	try:
		es = Elasticsearch([es_search_host])
	except 'connection error':
		print("CONNECTION ERROR")
	try:
		# revisar indice para consulta. index="aeacus_kdd". uri:aeacus:fs#9e33cbdbd42b4ec98b1e8d2080d64ed4
		# lanzamos la consulta sobre elasticsearch con el criterio y nombre del documento que viene dado por parámetro en el script
		# por defecto en las consultas de elasticsearch el size = 10, si no lo modificamos sólo nos mostrará los 10 primero registros sobre nuestra consulta
		r = es.search(index=index_name, doc_type="document",
						body={"size": 1000, "query": {"match_phrase": {criterion_name: doc_name}}},
						sort="page")
		# recorremos todo el contenido de la consulta para poder alamcenar el texto completo en una sola lista
		if r['hits']['hits'] != []:
			txt = []
			for doc in r['hits']['hits']:
				txt.append(doc['_source']['content'])
			keys = r['hits']['hits'][0]['_source'].keys()
			if 'lang' in keys:
				lang = r['hits']['hits'][0]['_source']['lang']
			else:
				raise Exception('No se ha encontrado lang en el JSON')
			return txt, lang
		else:
			raise Exception('Consulta fallida, el documento no se ha encontrado')
	except 'Warning':
		raise Exception("La consulta no se ha realizado de manera adecuada")


# Preprocesado basado en elastic search
def remove_accents(input_str, encoding="latin1"):
	try:
		input_str = unicode(input_str.decode(encoding))
	except UnicodeEncodeError:
		pass
	import unicodedata
	nfkd_form = unicodedata.normalize('NFKD', input_str)
	only_ascii = nfkd_form.encode('ASCII', 'ignore')
	return only_ascii


def map_many(iterable, function, *other):
	# Multimap several functions
	import numpy as np
	if other:
		return map_many(map(function, iterable), *other)
	return np.array(map(function, iterable))


def del_page_num(txt, regex):
	import re
	aux = re.findall(regex, txt)
	s = re.compile(r"\b(" + "|".join(aux) + ')\\' + regex[-1:], re.I)
	pa = s
	s = pa.pattern
	first = '('
	last = ')'
	try:
		start = s.index(first) + len(first)
		end = s.index(last, start)
		res = s[start:end]
	except ValueError:
		res = ""
	if len(res) == 0:
		return txt
	else:
		return pa.sub("", txt)


def del_strange_charact(txt):
	# Los textos de elastic search son una maravilla...
	prev_txt = ''
	while prev_txt != txt:
		prev_txt = txt
		txt = txt.replace(',.', ',')
		txt = txt.replace(', .', ',')
	return prev_txt


# Set de funciones para preprocessing para tokenizar correctamente
def reduce_concat(x, sep=""):
	from functools import reduce as reduc	 # compatible con python 3 reduce
	return reduc(lambda s, y: s + sep + y, x)


def out_art(text):
	# Elimina el formato 1.2 que confunde al tokenizar frases
	import re
	auxexp = re.findall('[0-9]\.[0-9]', text)
	while len(auxexp) > 0:
		for expr in auxexp:
			auxexpr = expr.replace(".", "")
			text = text.replace(expr, auxexpr)
			auxexp = re.findall('[0-9]\.[0-9]', text)
	return text


# Definimos lenguajes
lang_dict = {'en': 'english',
			 'es': 'spanish',
			 'it': 'italian',
			 'de': 'deustch',
			 'fr': 'french',
			 'pt': 'portuguese'}


def fin_lang(formated_string, dictionary):
	# formated_string es idioma_pais, siendo pais opcional, con REGEX obtenemos idioma.
	# el output es el idioma madre, independientemente del pais
	if '_' in formated_string:
		silabs = formated_string.partition('_')[0]
		return dictionary[silabs]
	else:
		return dictionary[formated_string]


def process_text(txt, lang, outstopwords=True, outpunkt=True):
	# Funcion que elimina los acentos, stopwords, puntuaciones y convierte a minusculas

	# Quitamos acentos
	# texto = remove_accents(texto) # considerado en otro pretratado de texto
	# txt = remove_accents(text)

	# Pasamos a minuscula
	txt = txt.lower()

	# Eliminamos symbolos
	symbols = ['[', ']', '•', '^', '*', '/', '=']
	for symbol in symbols:
		txt = txt.replace(symbol, '')

	if outpunkt:
		# except '.'
		puntuations = [',', ':', ';', '(', ')', '?', '¿', '!', '¡', "'", '"']
		for puntuacion in puntuations:
			txt = txt.replace(puntuacion, '')

	# Hacemos algunas conversiones utiles
	prev_txt = ''
	while prev_txt != txt:
		prev_txt = txt
		txt = txt.replace('	 ', ' ')	# Quitamos dobles espacios
		#	 txt = txt.replace('. ', '.')	 # Quitamos espacios despues de puntos
		#	 txt = txt.replace(' .', '.')	 # Quitamos espacios antes de puntos
		txt = txt.replace('\n\n', '\n')	 # Quitamos dobles parrafos
		txt = txt.replace('\n \n', '\n')	# Quitamos dobles parrafos con espacio

	# Eliminamos stopwords
	if outstopwords:
		from nltk.corpus import stopwords
		stop_words = stopwords.words(lang_dict[lang])
		for i in range(len(stop_words)):
			stop_words[i] = remove_accents(stop_words[i])

		txt_wd = txt.split(" ")
		txt_wd = [word for word in txt_wd if word not in stop_words]
		if len(txt_wd) > 0:
			txt = reduce_concat(txt_wd, sep=" ")
		else:
			txt = txt_wd
	return txt


def nonum(text):
	# Elimina ciertas expresiones que distorsionan la delimitacion de parrafos
	symbols = ['art.', ' art ', 'num.', 'i.', 'ii.', 'iii.', '.-']
	for symbol in symbols:
		text = text.replace(symbol, " ")	# Erease expresion
	return text


def outwords(text):
	# Dada una lista, elimina las palabras en un texto
	# Ampliar lista?
	word_list = ['ley', 'leyes', 'fj', 'derechos', 'derecho', 'justicia', ' ce ', 'LOTJ']
	for word in word_list:
		text = text.replace(word, "")	 # Erease expresion
	return text


# Consiguiendo una tokenizacion "perfecta"
def sentences(text, lang):
	# split en frases
	if lang == 'es':
		import pattern.es as pa

	elif lang == 'en':
		 import pattern.en as pa

	elif lang == 'pt':
		import pattern.es as pa

	elif lang == 'it':
		import pattern.it as pa

	elif lang == 'fr':
		import pattern.fr as pa

	elif lang == 'de':
		import pattern.de as pa
	else:
		raise Exception('El lenguaje no se encuentra desarrollado para el algoritmo.')

	parsed = pa.parse(text, tokenize=True, tags=0,
						chunks=0, relations=0, lemmata=False, encoding='utf-8', tagset=None)
	bad_list = parsed.split('\n')
	ulist = [s.replace('&slash;', '/') for s in bad_list]	 # edicion por mala codifiacion
	return [x.encode('utf-8') for x in ulist]


def preprosentence(txt, lang):
	# Preprocesamiento previo a la metrica
	return del_strange_charact(nonum(outwords(process_text(out_art(txt), lang))))

## # Nuevas funciones con implementacion map
## def get_cosine(pair):
##     rawstr1, rawstr2 = pair
##     import re
##     import math
##     from collections import Counter
##     comp = re.compile(r'\w+')
##     str1 = preprosentence(rawstr1, lang)
##     str2 = preprosentence(rawstr2, lang)
##     words1 = comp.findall(str1)
##     words2 = comp.findall(str2)
##     vec1 = Counter(words1)
##     vec2 = Counter(words2)
##     intersection = set(vec1.keys()) & set(vec2.keys())
##     numerator = sum([vec1[x] * vec2[x] for x in intersection])
## 
##     sum1 = sum([vec1[x] ** 2 for x in vec1.keys()])
##     sum2 = sum([vec2[x] ** 2 for x in vec2.keys()])
##     denominator = math.sqrt(sum1) * math.sqrt(sum2)
## 
##     if not denominator:
##         return rawstr1, rawstr2, 0.0
##     else:
##         return rawstr1, rawstr2, float(numerator) / denominator
## 
## 
## def connect(nodes):
##     from itertools import permutations
## 
##     per = list(permutations(nodes, 2))
##     return map(get_cosine, per)
	
def pre_cosine(string_proc, lang):
	import re
	from collections import Counter
	comp = re.compile(r'\w+')
	string_proc = preprosentence(string_proc, lang)
	words = comp.findall(string_proc)
	vec_count = Counter(words)
	return vec_count

def get_cosine_prep(vec1, vec2, lang):
	import math
	intersection = set(vec1.keys()) & set(vec2.keys())
	numerator = sum([vec1[x] * vec2[x] for x in intersection])

	sum1 = sum([vec1[x] ** 2 for x in vec1.keys()])
	sum2 = sum([vec2[x] ** 2 for x in vec2.keys()])
	denominator = math.sqrt(sum1) * math.sqrt(sum2)

	if not denominator:
		return 0.0
	else:
		return float(numerator) / denominator
	
# def get_cosine(str1, str2, lang):
# 	import re
# 	import math
# 	from collections import Counter
# 	comp = re.compile(r'\w+')
# 	str1 = preprosentence(str1, lang)  # DCR: Preprocesar solo una vez cada frase!!
# 	str2 = preprosentence(str2, lang)
# 	words1 = comp.findall(str1)
# 	words2 = comp.findall(str2)
# 	vec1 = Counter(words1)
# 	vec2 = Counter(words2)
# 	intersection = set(vec1.keys()) & set(vec2.keys())
# 	numerator = sum([vec1[x] * vec2[x] for x in intersection])
# 
# 	sum1 = sum([vec1[x] ** 2 for x in vec1.keys()])
# 	sum2 = sum([vec2[x] ** 2 for x in vec2.keys()])
# 	denominator = math.sqrt(sum1) * math.sqrt(sum2)
# 
# 	if not denominator:
# 		return 0.0
# 	else:
# 		return float(numerator) / denominator


def connect(nodes_prep, frases):
	if len(nodes_prep) != len(frases):
		raise Exception("Las frases y los vectores deben tener la misma longitud!!")
	return [(frases[i], frases[j], get_cosine_prep(nodes_prep[i], nodes_prep[j], lang))
			for i in range(len(nodes_prep))
			for j in range(len(nodes_prep))
			if i is not j]


def rank(nodes, edges):
	import networkx as nx
	# el output es un dict de unos nodes y edges
	graph = nx.DiGraph()
	graph.add_nodes_from(nodes)
	graph.add_weighted_edges_from(edges)
	return nx.pagerank(graph)


def get_phrase(fra_res, con_pag, langu):
	from functools import partial as par
	# fra_pag es una lista de listas
	# fra_res es una lista de strings
	# la funcion busca en cada lista de listas, si esta contenido el elemento de
	# fra_res y devuelve una lista de vectores (posicion en sublista, frase)
	
	pagines = []
	for res_sent in fra_res:
		aux_phrase = res_sent
		trobat = False
		
		i = 1
		while not trobat and i < len(con_pag):
	#    for i in range(1, len(con_pag)):
			if aux_phrase in con_pag[i - 1]:
				pagines.append(i)
				trobat = True
			elif aux_phrase in con_pag[i]:
				pagines.append(i + 1)
				trobat = True
			elif aux_phrase in (con_pag[i - 1] + con_pag[i]):
				pagines.append((i, i + 1))
				trobat = True
				
			i += 1
			
		if not trobat:
			pagines.append(0)
			print "No hem trobat la frase: " + res_sent + "!!!"
	
	return pagines


def filter_length(vec_l, length):
	# filter results by length
	import numpy as np
	index = np.where(np.array(list(map(len, vec_l[0]))) > length)	 # params: length, vec_l[0] = phrases
	return [np.array(x)[index] for x in vec_l]


def summarize(es_search_host, search_index, doc_name, criterion_name):
	
	from functools import partial as par

	text = ""
	try:
		text = conn_query(es_search_host, search_index, doc_name, criterion_name)	 # conseguir lang
	except TypeError as e:
		print(e)
	# txt = map_many(text[0], par(del_page_num, regex=r"([A-Z]{1,2}[/][\d])\w"),
	#				 par(del_page_num, regex=r"\b(\d{1,2}/\d{1,2})\b"))

	# Definimos variable global para usarla dentro de get_cosine
	global lang
	lang = text[1]
	
	result_nodes = [j for j in list(map(par(sentences, lang=text[1]), text[0]))]
	nodes = []
	pag_nodes = []
	for item in result_nodes:
		pag_nodes.append(reduce_concat(item, sep = ""))
		for frase in item:
			nodes.append(frase)
		
#	nodes = [i for j in list(map(par(sentences, lang=text[1]), text[0])) for i in j]
	nodes_pre = [pre_cosine(item, lang) for item in nodes]
	edges = connect(nodes_pre, nodes)
	scores = rank(nodes, edges)
	phrases = sorted(scores, key=scores.get, reverse=True)	# order phrase by score desc
	paginas = get_phrase(phrases, pag_nodes, text[1])
	scroll = []
	for key in phrases:
		if key in nodes:
			try:
				scroll.append(float(nodes.index(key)) / len(nodes) * 100)
			except:
				pass

	scores = sorted(scores.values(), reverse=True)	# obtener scores del dict y ordenar desc
	scores = [float(i) / max(scores) for i in scores]	 # estandarizar scores
	vec_list = (phrases, paginas, scores, scroll)

	return filter_length(vec_list, 150)


def load_data_into_es(newesconn, INDEX_NAME, TYPE_NAME, name, vec_list):
	print "load_data_into_es"
	bulk_data = []
	print("Comienza la carga de datos en ES")
	i = 1
	filename = name
	# De vec_list obtenemos cuatro listas con las frases, el resultado de la función get_phrase, los scores y el scroll
	datas = zip(vec_list[0], vec_list[1], vec_list[2], vec_list[3])
	# Vamos a preparar los datos procedentes del resultado de la ejecución del algoritmo para su subida a elasticsearch
	if len(datas) > 0:
		for data in datas:
			if data[1] == 0:
				data = list(data)
				data[2] = 0
			# print("vamos a ir viendo data")
			# print(data[0])
			index_id = filename + "_" + str(i)
			data_dict = {
				"file": filename,
				"phrase": unicode(data[0], "utf-8"),
				"page": data[1],
				"score": data[2],
				"scroll": data[3]
			}
			doc = {
				"index": {
					"_index": INDEX_NAME,
					"_type": TYPE_NAME,
					"_id": index_id
				}
			}
			print data_dict  # Comprovar que funciona lo de page = 0 llavors score = 0
			bulk_data.append(doc)
			bulk_data.append(data_dict)
			i = i + 1

		# bulk index the data
		print("bulk indexing...")
		# Hacemos la carga masiva de datos
		newesconn.bulk(index=INDEX_NAME, body=bulk_data, refresh=True, request_timeout=60)
		print("Finalizada la carga de datos")


def load_summary_into_elastic(es_load_host, load_index, doc_name, vec_list):
	print "load_summary_into_elastic"
	INDEX_NAME = load_index
	# Creamos la conexion a elasticsearch con el host de carga. Aquí es donde cargaremos el resultado de la ejecución
	# del algoritmo anterior
	try:
		newesconn = Elasticsearch([es_load_host])
	except 'connection error':
		print("CONNECTION ERROR")
	TYPE_NAME = 'summary'
	# Vamos a comprobar si el índice existe en elasticsearch, de no ser así lo creamos en la sentencia "else"
	if newesconn.indices.exists(INDEX_NAME):
		pass
	else:
		# Configuracion de particion de indices y replicacion
		request_body = {
			"settings": {
				"number_of_shards": 1,
				"number_of_replicas": 0
			}
		}

		# OJO, sólo si no existe o se ha borrado
		print('')
		print("Creando el indice '%s'..." % INDEX_NAME)
		response_create_index = newesconn.indices.create(index=INDEX_NAME, body=request_body)
		print("response: '%s'" % response_create_index)
		print('')

	try:
		# Habrá que buscar los _id asociados al nombre del documento y borrar todos si existen
		# antes de cargar de nuevo los datos en ES
		# Con la siguiente consulta buscamos todos los registros que cumplan que el campo "file" coincide con
		# la variable doc_name, que viene dada como parámetro a la función
		scroll = newesconn.search(index=INDEX_NAME, doc_type=TYPE_NAME,
									body={"size": 1000, "query": {"match_phrase": {"file": doc_name}}})
		bulk = ""
		# En hits_number almacenamos el número de registros de la anterior consulta
		hits_number = scroll['hits'][u'total']
		# Si el número de registros es distinto de cero creamos el bulk para el borrado de datos masivo
		if hits_number != 0:
			for result in scroll['hits']['hits']:
				bulk = bulk + '{ "delete" : { "_index" : "' + str(result['_index']) + '", "_type" : "' + str(
					result['_type']) + '", "_id" : "' + str(result['_id']) + '" } }\n'
			newesconn.bulk(body=bulk)
	except:
		print("No hay datos para borrar asociados al documento")
	# Ejecutamos la función load_data_into_es que cargará el resumen en elasticsearch
	# Para no crear dos veces el conector, se lo pasaremos por parámetro
	if len(vec_list) > 0:
		load_data_into_es(newesconn, INDEX_NAME, TYPE_NAME, doc_name, vec_list)


def response_summarize(es_search_host, search_index, es_load_host, load_index, doc_name, criterion_name):
	summaries = summarize(es_search_host, search_index, doc_name, criterion_name)
	print(summaries)
	if len(summaries) > 0:
		load_summary_into_elastic(es_load_host, load_index, doc_name, summaries)
		print("Finalizado el proceso de generación de resúmen")
	else:
		print("No se han detectado frases relevantes para este documento")


# if __name__ == "__main__":
# res(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])

def load_execution_result_into_elastic(es__host2, INDEX_NAME, script_name, arguments, resultado_ejecucion):
	print "load_execution_result_into_elastic"
	import time
	import datetime
	try:
		es = Elasticsearch([es__host2])
	except 'connection error':
		print("CONNECTION ERROR")
	ts = time.time()
	# Vamos a tomar el datetime para tener guardado el momento de la ejecución
	# Además lo añadiremos al id, ya que este ha de ser único
	st_field = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
	st_index = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
	# INDEX_NAME = 'pruebas'
	TYPE_NAME = 'akd_result'
	bulk_data = []
	index_id = str(script_name) + str("_") + str(st_index)
	data_dict = {
		"script_name": script_name,
		"arguments": arguments,
		"resultado": resultado_ejecucion,
		"time_stamp": str(st_field)
	}
	doc = {
		"index": {
			"_index": INDEX_NAME,
			"_type": TYPE_NAME,
			"_id": index_id
		}
	}
	bulk_data.append(doc)
	bulk_data.append(data_dict)
	es = Elasticsearch(hosts=[es__host2])
	res = es.bulk(index=INDEX_NAME, body=bulk_data, refresh=True, request_timeout=60)
	return res

def grabaResultadoPosgre(argumentos, resultado, filename, criterion):

	import psycopg2 as p

	# Parametros db
	pars_db = argumentos.split("/")
	dbname= pars_db[0]
	dbuser= pars_db[1]
	dbpass= pars_db[2]
	host= pars_db[3]
	port = pars_db[4]
	con_str = str('dbname='+'\''+dbname+'\''+' user='+'\''+dbuser+'\''+' host='+'\''+host+'\''+' password='+'\''+dbpass+'\''+' port='+'\''+port+'\'')
	connection = p.connect(con_str)
	connection.autocommit = True
	cursor = connection.cursor()

	# Hacemos update en posgre
	queryUpdate = "update imp_request set akdstate = " + str(resultado) + ", lastchange = localtimestamp where id in (select id from content where " + criterion + " = '" + filename + "')"
	print "Ejecutando query " + queryUpdate + "..."
	cursor.execute(queryUpdate)
	print "Query ejecutada"
	
	
	
if __name__ == "__main__":
	script_name = "summarize.py"
	# Preparamos en un string los argumentos recibidos para la función de resultado
		
	files2Process = sys.argv[5].split("/")
	for filename in files2Process:
		print "Procesando " + filename + "..."
		arguments = str(sys.argv[1]) + str("|") + str(sys.argv[2] + str("|") + sys.argv[3]) + str("|") + str(
			sys.argv[4] + str("|") + filename) + str("|") + str(sys.argv[6])
			
		try:
			# Comenzamos la ejecución
			response_summarize(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], filename, sys.argv[6])
			# En caso de que la ejecución haya sido satisfactoria cargamos en la variable "resultado" el valor success
			load_execution_result_into_elastic(sys.argv[3], sys.argv[4], script_name, arguments, "success")
			grabaResultadoPosgre(sys.argv[7], 200, filename, str(sys.argv[6]))
			
		# print(res(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6]))
		except Exception as e:
			# En caso de que la ejecución no haya sido satisfactoria cargamos en la variable "resultado" el valor failure
			print "Error:"
			print e
			try:
				error_control = load_execution_result_into_elastic(sys.argv[3], sys.argv[4], script_name, arguments, "failure")
				print(error_control)
			except Exception as e2:
				print e2
			grabaResultadoPosgre(sys.argv[7], 500, filename, str(sys.argv[6]))

			
		print filename + " finalizado!"
		
if utiliza_logs:
	sys.stdout = old_stdout
	log_file.close()
	