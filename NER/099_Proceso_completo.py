#!/usr/bin/env python
# -*- coding: utf-8 -*-

# spark-submit --master yarn-cluster --deploy-mode cluster --py-files 099_Proceso_completo.py 099_Proceso_completo.py 172.22.248.206:9229 172.22.248.206:9229 aeacus_kdd pruebas_ner name 04_032-033_2.PDF True
# cd /home/ijdocs/just_docs/ner/syntax/099_Proceso_completo; python 099_Proceso_completo.py 172.22.248.206:9229 172.22.248.206:9229 aeacus_dev ner_dev uri aeacus:fs#b2c8b153f65a42e7904c4c1f5fafb3ed/aeacus:fs#22414d7a6491497d8b2f031d1a050128/aeacus:fs#5a2dc471a31c4fd98a1fcffb86ffe02c/aeacus:fs#1a9a5a3baca1477d9523a5146213eea3 False aeacus/aeacus/aeacus##sij2016/172.22.248.221/5432 True

# =============================== #
# =============================== #
# ===== Definimos funciones ===== #
# =============================== #
# =============================== #

# ========================== #
# === Funciones busqueda === #
# ========================== #

from nltk import ne_chunk, pos_tag, word_tokenize
from nltk.tree import Tree
import re
import numpy as np
from nltk.corpus import stopwords



def buscaPosicionRegexTexto(regex, texto):
	iter = re.finditer(regex, texto)
	indices = [m.span() for m in iter]
	return(indices)

def buscaPrincipioRegexTexto(regex, texto):
	# Busca en un texto todas las palabras que empiezan por 
	# la expresion regular de input y devuelve la palabra
	buscame = '((^|[\s])' + regex + ')'
	aux = np.array(re.findall(buscame, texto))
	aux = aux[:, 0]
	result = []
	for item in aux:
		result.extend(re.findall(regex, item))
		
	return result

def buscaPosicionPrincipioRegexTexto(regex, texto):
	# Busca en un texto todas las palabras que empiezan por 
	# la expresion regular de input y devuelve su posicion
	buscame = '((^|[\s])' + regex + ')'
	
	aux = buscaPosicionRegexTexto(buscame, texto)
	result = []
	for i in range(len(aux)):
		result.append([])
		item = aux[i]
		auxText = texto[item[0]:item[1]]
		if len(re.findall('\s', auxText[0])) > 0:
			result[i].append(aux[i][0] + 1)
		else:
			result[i].append(aux[i][0])
			
		result[i].append(aux[i][1])
			
	return result

def buscaPosicionPalabraCompletaTexto(palabraCompleta, texto):
	buscame = '((^|[\s])' + palabraCompleta + '($|[\s]|[^\P{P}-]?))'
	
	aux = buscaPosicionRegexTexto(buscame, texto)
	result = []
	for i in range(len(aux)):
		result.append([])
		item = aux[i]
		auxText = texto[item[0]:item[1]]
		if len(re.findall('\s', auxText[0])) > 0:
			result[i].append(aux[i][0] + 1)
		else:
			result[i].append(aux[i][0])
			
		result[i].append(aux[i][1])
			
	return result

# Funcion para buscar un diccionario de palabras relevantes dentro de un texto, debajo hay un ejemplo de como usarla
def buscaDiccionarioTexto(diccionario, texto):

	result = dict()
	elementosBusca = diccionario.keys()

	for elemento in elementosBusca:

		result[elemento] = []
		act_dict = diccionario[elemento]
		
		# Buscamos palabras completas
		if 'complete_word' in act_dict.keys():
			for comp_wd in act_dict['complete_word']:
				positions = buscaPosicionPalabraCompletaTexto(comp_wd, texto)
				result[elemento].extend(positions)
		
		# Buscamos expresiones regulares
		if 'reg_exp' in act_dict.keys():
			for reg_exp in act_dict['reg_exp']:
				positions = list(buscaPosicionRegexTexto(reg_exp, texto))
				result[elemento].extend(positions)
		
		if 'complete_exp' in act_dict.keys():
			for complete_exp in act_dict['complete_exp']:
				positions = list(buscaPosicionCompexTexto(complete_exp, texto))
				result[elemento].extend(positions)
		
	return result


# =================================== #
# === Funciones comparacion texto === #
# =================================== #

import numpy as np
from math import factorial
import pandas as pd

def comparaFrases(llistaFrases, fileMod = None, totesContraTotes = True):
	
	compta = 0
	nRep = factorial(len(llistaFrases)) / (factorial(2) * factorial(len(llistaFrases) - 2))
	resultado = pd.DataFrame(data = dict(item1 = np.repeat(None, nRep), item2 = np.repeat(None, nRep), valor_comp = np.repeat(None, nRep)))
	if not totesContraTotes:
		avgVal = np.mean([len(frase.split(" ")) for frase in llistaFrases[1:]])
	for i in range(len(llistaFrases)):
		for j in range(len(llistaFrases)):
			if i > j:	 # Nomes comparem si son diferents i no les hem comparat ja
				resultado["item1"][compta] = i
				resultado["item2"][compta] = j
				frase1 = llistaFrases[i].split(" ")
				frase2 = llistaFrases[j].split(" ")
				vals_comuns = []
				for paraula1 in frase1:
					for paraula2 in frase2:
						if len(paraula1) > 2 and len(paraula2) > 2:
							iguals = 0
							total = 0
							for str1 in paraula1:
								for str2 in paraula2:
									total += 1
									if str1 == str2:
										iguals += 1
							vals_comuns.append(float(iguals)/total)
				if totesContraTotes:
					resultado["valor_comp"][compta] = np.mean(vals_comuns)
				else:
					resultado["valor_comp"][compta] = np.mean(vals_comuns)*(1 - (abs(avgVal - min(len(frase1), len(frase2))))/len(frase1 + frase2))
					
				compta += 1

	return resultado

def unirGrupos(item1, item2, vanJunts):
	from copy import deepcopy
	import itertools

	# Creem llista de grups i amb qui es relacionen
	dictRels = dict()
	for i in range(len(vanJunts)):
		if item1[i] in dictRels.keys() and vanJunts[i]:
			dictRels[item1[i]].append(item2[i])
		elif vanJunts[i]:
			dictRels[item1[i]] = []
			dictRels[item1[i]].append(item2[i])
		elif item1[i] not in dictRels.keys():
			dictRels[item1[i]] = []
		
		if item2[i] in dictRels.keys() and vanJunts[i]:
			dictRels[item2[i]].append(item1[i])
		elif vanJunts[i]:
			dictRels[item2[i]] = []
			dictRels[item2[i]].append(item1[i])
		elif item2[i] not in dictRels.keys():
			dictRels[item2[i]] = []
			
	for ikey in dictRels.keys():
		 dictRels[ikey].append(ikey)
		 dictRels[ikey] = list(set(dictRels[ikey]))
	
	# Sacamos los grupos a partir de las relaciones
	dictAnt = deepcopy(dictRels)
	dictGroups = dict()
	while dictAnt != dictGroups:
		dictAnt = deepcopy(dictGroups)
		dictGroups = dict()
		for ikey in dictRels.keys():
			grupAct = deepcopy(dictRels[ikey])
			for jkey in dictRels.keys():
				if ikey != jkey and ikey in dictRels[jkey]:
					grupAct.extend(dictRels[jkey])
					grupAct = list(set(grupAct))
			
			dictGroups[ikey] = grupAct
			
		dictRels = deepcopy(dictGroups)
	
	# Identificamos grupos unicos
	aux_l = []
	for ikey in dictGroups.keys():
		aux_l.append(dictGroups[ikey])
	aux_l.sort()
	result = list(aux_l for aux_l,_ in itertools.groupby(aux_l))
	
	return result


# ========================= #
# === Funciones elastic === #
# ========================= #

try:
	from elasticsearch import Elasticsearch
	found = True
except ImportError:
	found = False

if found:
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
		
		if con.indices.exists(indice):
			# revisar indice para consulta. index="aeacus_kdd". uri:aeacus:fs#9e33cbdbd42b4ec98b1e8d2080d64ed4
			res = con.search(index=indice, doc_type="document", body={"size": 1000, "query": {"match_phrase": {criterion: doc}}}, sort="page")
			text = ""
			for ires in res['hits']['hits']:
				text = text + ires['_source']['content']
			
			return text
		else:
			raise Exception("No existe el indice de donde tenemos que leer!")

	def list_docs(criterion, con, indice):
		if con.indices.exists(indice):
			res = con.search(index=indice, doc_type="document", body={"fields" : criterion}) 
			all_names = []
			for element in res['hits']['hits']:
				all_names.extend(element['fields'][criterion])
			
			all_names = list(set(all_names))
		
			return all_names
		else:
			raise Exception("No existe el indice de donde tenemos que leer!")

	def read_file_pagebypage(doc, criterion, con, indice):
		# conexion a elasticsearch
		
		if con.indices.exists(indice):
			# revisar indice para consulta. index="aeacus_kdd". uri:aeacus:fs#9e33cbdbd42b4ec98b1e8d2080d64ed4
			res = con.search(index=indice, doc_type="document", body={"size": 1000, "query": {"match_phrase": {criterion: doc}}}, sort="page")
			result = []
			for ires in res['hits']['hits']:
				result.append(ires['_source']['content'])
			
			return result
		else:
			raise Exception("No existe el indice de donde tenemos que leer!")

	def load2Elastic(dict_files, INDEX_NAME, TYPE_NAME, newesconn):
		# dict_files: diccionari amb keys que seran els indexos, i conte diccionaris
		# amb tots els parametres que hem d'omplir de l'index

		if not newesconn.indices.exists(INDEX_NAME):
			# Configuracion de particion de indices y replicacion
			request_body = {
				"settings": {
					"number_of_shards": 1,
					"number_of_replicas": 0
				}
			}

			response_create_index = newesconn.indices.create(index = INDEX_NAME, body = request_body)
		
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
		if newesconn.indices.exists(INDEX_NAME):
			documento = documento.replace(".pdf", "").replace(".PDF", "")
			# Eliminamos datos del mismo documento si existen
			scroll = newesconn.search(index=INDEX_NAME, doc_type=TYPE_NAME, body={"size": 1000, "query": {"match_phrase": {"documento": documento}}})
			bulk = ""
			# En hits_number almacenamos el número de registros de la anterior consulta
			hits_number = scroll['hits'][u'total']
			# Si el número de registros es distinto de cero creamos el bulk para el borrado de datos masivo
			if hits_number != 0:
				for result in scroll['hits']['hits']:
					bulk = bulk + '{ "delete" : { "_index" : "' + str(result['_index']) + '", "_type" : "' + str(result['_type']) + '", "_id" : "' + str(result['_id']) + '" } }\n'
				newesconn.bulk(body=bulk)
				
		else:
			request_body = {
				"settings": {
					"number_of_shards": 1,
					"number_of_replicas": 0
				}
			}

			response_create_index = newesconn.indices.create(index = INDEX_NAME, body = request_body)



	# ====================== #
	# === Funciones HDFS === #
	# ====================== #

	def reduce_concat_hdfs(x, sep = ""):
		for i in range(len(x)):
			if isinstance(x[i], str):
				x[i] = quitaDoblesEspacios(x[i])
		if isinstance(x, unicode):
			return(reduce(lambda x, y: x + sep + y, x))
		else:
			return(reduce(lambda x, y: str(x) + sep + str(y), x))

	def guardaHDFS(texto, filename, sc):
		rdd = sc.parallelize(texto)
		rdd.saveAsTextFile(filename)

	def guardaDFHDFS(dataframe, filename, sc):

		textoGuarda = ''
		for columna in dataframe.columns:
			textoGuarda += columna + "\t"
			textoGuarda += str(reduce_concat_hdfs(dataframe[columna].tolist(), sep = "   ")) + "\n"
		
		guardaHDFS([textoGuarda], filename, sc)
		
	def readDFHDFS(path2read, sc):

		# llegim path
		textFile = sc.textFile(path2read)
		texto = textFile.collect()
		
		if isinstance(texto, list):
			texto = str(texto)
			
		# Preparem
		texto = texto.replace("[u'", "")
		texto = texto.replace("]", "")
		texto = texto.replace('", u"', "', u'")
		texto = texto.replace("', u\"", "', u'")
		texto = texto.replace("\", u'", "', u'")
		lista_texto = texto.split("', u'")
		
		# Construim dades
		newDict = dict()
		for item in lista_texto:
#			print item
			aux_l = item.split("\\t")
			if len(aux_l) == 1:
				aux_l = item.split("\t")
			
			if len(aux_l) > 1:
				values = aux_l[1].split("		")
				
				# Si todos son numericos lo pasamos a numerico
				if all([len(re.findall("[0-9]", item)) == len(item) for item in values]):
					values = [int(item) for item in values]
					
				elif all([len(re.findall("[0-9]", item)) == (len(item) - 1) for item in values]) and all([(len(item) - 1) == len(item.replace(".", "")) for item in values]):
					values = [float(item) for item in values]
					
				newDict[aux_l[0]] = values
				
		result = pd.DataFrame(newDict)
		
		return result
		
	def guardarDictHDFS(dict2Save, filename, sc):

		textoGuarda = ''
		for ikey in dict2Save.keys():
			textoGuarda += "\t" + ikey + "\n"
			if not isinstance(dict2Save[ikey], dict):
				if isinstance(dict2Save[ikey], list):
					textoGuarda += "\t:::" + reduce_concat_hdfs(dict2Save[ikey], sep = "	 ") + "\n"
				else:
					textoGuarda += "\t:::" + dict2Save[ikey] + "\n"
			else:
				for ikey2 in dict2Save[ikey].keys():
					textoGuarda += "\t\t" + ikey2 + "\n"
					if not isinstance(dict2Save[ikey][ikey2], dict):
						if isinstance(dict2Save[ikey][ikey2], list):
							textoGuarda += "\t\t:::" + reduce_concat_hdfs(dict2Save[ikey][ikey2], sep = "		") + "\n"
						else:
							textoGuarda += "\t\t:::" + dict2Save[ikey][ikey2] + "\n"
					else:
						for ikey3 in dict2Save[ikey][ikey2].keys():
							textoGuarda += "\t\t\t" + ikey3 + "\n"
							if not isinstance(dict2Save[ikey][ikey2][ikey3], dict):
								if isinstance(dict2Save[ikey][ikey2][ikey3], list):
									textoGuarda += "\t\t\t:::" + reduce_concat_hdfs(dict2Save[ikey][ikey2][ikey3], sep = "	 ") + "\n"
								else:
									textoGuarda += "\t\t\t:::" + dict2Save[ikey][ikey2][ikey3] + "\n"
							else:
								textoGuarda += "\t\t\t:::" + str(dict2Save[ikey][ikey2][ikey3])
								textoGuarda +=	"\n"
				

		guardaHDFS([textoGuarda], filename, sc)

	def readDictHDFS(path2read, sc):

		# llegim path
		textFile = sc.textFile(path2read)
		texto = textFile.collect()
		
		texto = unicode(str(texto))
		
		# Preparem
		texto = texto.replace("[u'", "")
		texto = texto.replace("]", "")
		lista_texto = texto.split("', u'")
		
		# Construim dades
		noms_llista = ["referencias", "fin", "inicio"]

		result = dict()
		for item in lista_texto:
			if not ":::" in item:
				if "\t\t\t" in item:
					ikey3 = item.replace("\t\t\t", "")
				elif "\t\t" in item:
					ikey2 = item.replace("\t\t", "")
				elif "\t" in item:
					ikey = item.replace("\t", "")
					
			else:
				if "\t\t\t" in item:
					if ikey not in result.keys():
						result[ikey] = dict()
					if ikey2 not in result[ikey].keys():
						result[ikey][ikey2] = dict()
					
					if ikey3 in noms_llista:
						result[ikey][ikey2][ikey3] = []
						result[ikey][ikey2][ikey3].append(item.replace("\t\t\t:::", ""))
					else:
						result[ikey][ikey2][ikey3] = item.replace("\t\t\t:::", "")
					
				elif "\t\t" in item:
					if ikey not in result.keys():
						result[ikey] = dict()
					if ikey2 in noms_llista:
						result[ikey][ikey2] = []
						result[ikey][ikey2].append(item.replace("\t\t:::", ""))
					else:
						result[ikey][ikey2] = item.replace("\t\t:::", "")
						
				elif "\t" in item:
					if ikey in noms_llista:
						result[ikey] = []
						result[ikey].append(item.replace("\t:::", ""))
					else:
						result[ikey] = item.replace("\t:::", "")
			
		return result


# =============================== #
# === Funciones lectura datos === #
# =============================== #

if not found:
	import sys
	sys.path.append('C:/Python27/Lib/site-packages')

	from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
	from pdfminer.converter import TextConverter
	from pdfminer.layout import LAParams
	from pdfminer.pdfpage import PDFPage
	from cStringIO import StringIO

	# Definimos funciones
	def pdf2txt(path):
			from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
			from pdfminer.converter import TextConverter
			from pdfminer.layout import LAParams
			from pdfminer.pdfpage import PDFPage
			from cStringIO import StringIO
			rsrcmgr = PDFResourceManager()
			retstr = StringIO()
			codec = 'utf-8'
			laparams = LAParams()
			device = TextConverter(rsrcmgr, retstr, codec = codec, laparams = laparams)
			fp = file(path, 'rb')
			interpreter = PDFPageInterpreter(rsrcmgr, device)
			password = ""
			maxpages = 0
			caching = True
			pagenos=set()

			for page in PDFPage.get_pages(fp, pagenos, maxpages = maxpages, password = password, caching = caching, check_extractable = True):
					interpreter.process_page(page)

			text = retstr.getvalue()

			fp.close()
			device.close()
			retstr.close()
			return text
		

# ============================== #
# === Funciones preprocesado === #
# ============================== #

import unicodedata
from nltk.corpus import stopwords

def reduce_concat(x, sep = ""):
	return(reduce(lambda x, y: x + sep + y, x))

def sentences(text):
	# split en frases
	caracteres_nosplit = ["Avda.", "avda.", "c.", "C.", "Num.", "num.", "Núm.", "núm.", "Sr.", "Sra.", "sr.", "sra.", "D.", "Da."]
	caracteres_split = [".-", ". "]
	for char_nosplit in caracteres_nosplit:
		text = text.replace(char_nosplit, char_nosplit.replace(".", ""))
	for char_split in caracteres_split:
		text = text.replace(char_split, " .\n")
	return text

def filter_func(char):
		return char == '\n' or 32 <= ord(char) <= 126

def remove_accents(input_str, enc = 'utf-8'):
	try:
		input_str = unicode(input_str.decode(enc))
	except UnicodeEncodeError as e:
		pass
	nfkd_form = unicodedata.normalize('NFKD', input_str)
	only_ascii = nfkd_form.encode('ASCII', 'ignore')
	return(only_ascii)
	
def preProcesoTexto(texto, eliminarStopwords = True, eliminarPuntuaciones = False, enc = "utf-8"):
	# Funcion que elimina los acentos, stopwords, puntuaciones y convierte a minusculas todo el texto en espanyol

	# Quitamos acentos
	texto = remove_accents(texto, enc = enc)

	# Pasamos a minuscula
	texto = texto.lower()

	# Eliminamos simbolos
	symbols = ['[', ']', '•', '^', '*', '/', '=']
	for simbol in symbols:
		texto = texto.replace(simbol, '')
	
	if eliminarPuntuaciones:
		puntuations = [',', ':', '.', ';', '(', ')', '?', '¿', '!', '¡', "'", '"']
		for puntuacion in puntuations:
			texto = texto.replace(puntuacion, '')
	
	# Hacemos algunas conversiones utiles
	textoAnt = ''
	while textoAnt != texto:
		textoAnt = texto
		texto = texto.replace('	 ', ' ')	# Quitamos dobles espacios
		texto = texto.replace('\n\n', '\n')	 # Quitamos dobles parrafos
		texto = texto.replace('\n \n', '\n')	# Quitamos dobles parrafos con espacio
		
		
	# Eliminamos stopwords
	if eliminarStopwords:
		stop_words = stopwords.words('spanish')
		for i in range(len(stop_words)):
			stop_words[i] = remove_accents(stop_words[i])
		
		texto_wd = texto.split(" ")
		texto_wd = [word for word in texto_wd if word not in stop_words]
		texto = reduce_concat(texto_wd, sep = " ")
	
	return(texto)


# ================================= #
# === Funciones transforma refs === #
# ================================= #

DictSubsMeses = dict(
	es = dict(
		esp = dict(
			enero = "01",
			febrero = "02",
			marzo = "03",
			abril = "04",
			mayo = "05",
			junio = "06",
			julio = "07",
			agosto = "08",
			setiembre = "09",
			septiembre = "09",
			octubre = "10",
			noviembre = "11",
			diciembre = "12"
		)
	),
	pt = dict(
		bra = dict(
			janeiro = "01",
			fevereiro = "02",
			marco = "03",
			abril = "04",
			maio = "05",
			junho = "06",
			julho = "07",
			agosto = "08",
			setembro = "09",
			outubro = "10",
			novembro = "11",
			dezembro = "12"
		)
	)
)


# transformaNumsString("dos mil setecientos trentaydos")
def transformaNumsString(numstr):

	DictPaisDictUnidades = dict(
		es = dict(
			esp = dict(
				uno = 1,
				dos = 2,
				tres = 3,
				cuatro = 4,
				cinco = 5, 
				seis = 6,
				siete = 7,
				ocho = 8,
				nueve = 9
			)
		),
		pt = dict(
			bra = dict(
				um = 1,
				dois = 2,
				tres = 3,
				quatro = 4,
				cinco = 5, 
				seis = 6,
				sete = 7,
				oito = 8,
				nove = 9
			)
		)
	)
	
	DictPaisDictDecenas = dict(
		es = dict(
			esp = dict(
				diez = 10,
				once = 11,
				doce = 12,
				trece = 13,
				catorce = 14,
				quince = 15,
				dieciseis = 16,
				diecisiete = 17,
				dieciocho = 18,
				diecinueve = 19,
				veinti = 20,
				veinty = 20,
				veintiun = 21,
				veintyun = 21,
				veinte = 20,
				treinta = 30,
				trenta = 30,
				cuarenta = 40,
				cincuenta = 50,
				sesenta = 60,
				setenta = 70,
				ochenta = 80,
				noventa = 90
			)
		),
		pt = dict(
			bra = dict(
				dez = 10,
				onze = 11,
				doze = 12,
				treze = 13,
				catorze = 14,
				quinze = 15,
				dezesseis = 16,
				dezessete = 17,
				dezoito = 18,
				dezenove = 19,
				vinte = 20,
				trinta = 30,
				quarenta = 40,
				cinquenta = 50,
				sessenta = 60,
				setenta = 70,
				oitenta = 80,
				noventa = 90
			)
		)
	)
	
	DictPaisDictCentenas = dict(
		es = dict(
			esp = dict(
				cien = 100,
				ciento = 100,
				quinientos = 500,
				setecientos = 700,
				novecientos = 900
			)
		),
		pt = dict(
			bra = dict(
				cem = 100,
				duzentos = 200,
				trezentos = 300,
				quatrocentos = 400,
				quinhentos = 500,
				seiscentos = 600,
				setecentos = 700,
				oitocentos = 800,
				novecentos = 900
			)
		)
	)
	
	result = "0000"
	
	# Miles
	if "mil" in numstr:
		result = "1000"
		aux_num = numstr.split("mil")
		aux_num = aux_num[0].strip()
		if aux_num != "":
			result = str(DictPaisDictUnidades[idioma][pais][aux_num]) + result[1:]
	
	# Centenas
	centenas = False
	for ikey in DictPaisDictCentenas[idioma][pais].keys():
		if ikey in numstr:
			centenas = True

	if centenas:
		aux_num = numstr.split("mil")
		aux_num = aux_num[len(aux_num) - 1].strip()
		if pais == "bra" and idioma == "pt":
			keyutiliza = ""
			for ikey in DictPaisDictCentenas[idioma][pais].keys():
				if len(re.findall("\\b" + ikey + "\\b", aux_num)) > 0:
					keyutiliza = ikey
			
			if keyutiliza != "":
				result = result[0] + str(DictPaisDictCentenas[idioma][pais][keyutiliza])
			
		elif "cientos" in numstr:
			aux_num = aux_num.split("cientos")
			aux_num = aux_num[0].strip()
			
			if aux_num in DictPaisDictUnidades[idioma][pais].keys():
				result = result[0] + str(DictPaisDictUnidades[idioma][pais][aux_num]) + result[2:]
			elif "setecientos" in numstr or "novecientos" in numstr:
				if "setecientos" in numstr:
					result = result[0] + str(DictPaisDictCentenas[idioma][pais]["setecientos"])
				elif "novecientos" in numstr:
					result = result[0] + str(DictPaisDictCentenas[idioma][pais]["novecientos"])
			else:
				pass
				#print "No sabemos de que numero hablas!!"
			
		elif "quinientos" in numstr:
			result = result[0] + str(DictPaisDictCentenas[idioma][pais]["quinientos"])
		elif "ciento" or "cien" in numstr:
			result = result[0] + str(DictPaisDictCentenas[idioma][pais]["ciento"])
		else:
			pass
			#print "No sabemos de que numero hablas!!"
		
	# Decenas
	encontrado = False
	for ikey in DictPaisDictDecenas[idioma][pais].keys():
		if len(re.findall("\\b" + ikey + "\\b", numstr)) > 0 and not encontrado:
			result = result[:2] + str(DictPaisDictDecenas[idioma][pais][ikey])
			encontrado = True
			keyDecenas = ikey
		elif ikey in numstr and encontrado:
			pass
			#print "No puede ser que exista mas de una key de decenas en el mismo numero!!"
	
	if not encontrado:
		for ikey in DictPaisDictDecenas[idioma][pais].keys():
			if len(re.findall("\\b" + ikey, numstr)) > 0 and not encontrado:
				result = result[:2] + str(DictPaisDictDecenas[idioma][pais][ikey])
				encontrado = True
				keyDecenas = ikey
				
	if not encontrado:
		keyDecenas = " "
	
	# Unidades
	if result[3] == "0":
		auxnum = numstr.split(keyDecenas)
		auxnum = auxnum[len(auxnum) - 1].strip()
		auxnum = auxnum.split(" ")
		auxnum = auxnum[len(auxnum) - 1]
		if pais == "esp" and idioma == "es":
			auxnum = auxnum.split("y")
			auxnum = auxnum[len(auxnum) - 1]
			
		auxnum = auxnum.split("-")
		auxnum = auxnum[len(auxnum) - 1]
		if auxnum in DictPaisDictUnidades[idioma][pais].keys():
			result = result[:3] + str(DictPaisDictUnidades[idioma][pais][auxnum])
		else:
			pass
			#print "No sabemos detectar las unidades!!"
	
	while result[0] == "0":
		result = result[1:]
		
	return result

def transformaGrandesNumsString(grannumstr):

	if pais == "esp" and idioma == "es":
		grannumstr = grannumstr.replace("un ", "uno ")
		grannumstr = grannumstr.replace("miles", "mil")
		grannumstr = grannumstr.replace("millones", "millon")
		auxstr = grannumstr.split("millon")
		if "mil" in auxstr[len(auxstr) - 1]:
			grannumstr = grannumstr.replace("millon", "mil")
		else:
			grannumstr = grannumstr.replace("millon", "mil mil")
			
		auxnum = grannumstr.split("mil")
		
	elif pais == "bra" and idioma == "pt":
		grannumstr = grannumstr.replace("milhares", "mil")
		grannumstr = grannumstr.replace("milhoes", "milhao")
		auxstr = grannumstr.split("milhao")
		if "mil" in auxstr[len(auxstr) - 1]:
			grannumstr = grannumstr.replace("milhao", "mil")
		else:
			grannumstr = grannumstr.replace("milhao", "mil mil")
			
		auxnum = grannumstr.split("mil")
	
		
	result = []
	for i in range(len(auxnum)):
		item = auxnum[i]
		if i == 0 and item.replace(" ", "") == "":
			result.append("001")
		elif item.replace(" ", "") == "":
			result.append("000")
		else:
			auxres = transformaNumsString(item.strip())
			while len(auxres) < 3:
				auxres = "0" + auxres
			result.append(auxres)
	
	result = reduce_concat(result, sep = "")

	while result[0] == "0":
		result = result[1:]
	
	return result


# =================================== #
# === Funciones tratamiento texto === #
# =================================== #

import re
import numpy as np

def unosDentroOtros(posiciones):
	# posiciones es una lista que tiene esta forma: [[posini, posfin], [posini, posfin], [posini, posfin]]
	from numpy import array
	from numpy import sort
	from numpy import ndarray
	posiciones = array(posiciones)
	posiciones = sort(posiciones, 0)
	posiciones = ndarray.tolist(posiciones)
	jresta = 0
	for i in range(1, len(posiciones)):
		if posiciones[i - jresta][0] < posiciones[i - 1 - jresta][1]:
			posiciones[i - jresta] = [posiciones[i - 1 - jresta][0], posiciones[i - jresta][1]]
			posiciones.pop(i - 1 - jresta)
			jresta += 1
	
	return posiciones

def table(variable):
	aux_var = list(set(variable))
	variable = np.array(variable)
	result = dict()
	for item in aux_var:
		result[item] = sum(variable == item)
	return result

def sentences(text):
	# split en frases
	caracteres_nosplit = ["Avda.", "avda.", "c.", "C.", "Num.", "num.", "Núm.", "núm.", "Sr.", "Sra.", "sr.", "sra.", "D.", "Da.", "art."]
	caracteres_split = [".-", ". "]
	for char_nosplit in caracteres_nosplit:
		text = text.replace(char_nosplit, char_nosplit.replace(".", ""))
	for char_split in caracteres_split:
		text = text.replace(char_split, " .\n")
	return text

def quitaDoblesEspacios(texto):
	while "  " in texto:
		texto = texto.replace("  ", " ")
	return texto

def depPuntArt(texto):
	# Elimina el formato Art.1.2 que confunde al tokenizar frases'''
	auxExp = re.findall('[0-9]\.[0-9]', texto)
	while len(auxExp) > 0:
		for expr in auxExp:
			auxexpr = expr.replace(".", "")
			texto = texto.replace(expr, auxexpr)
			auxExp = re.findall('[0-9]\.[0-9]', texto)
			
	return(texto)

def reduce_concat(x, sep = ""):
	return(reduce(lambda x, y: x + sep + y, x))
	
def separaFrasesParrafos(texto):
	# Separamos por frases
	textoParrafos = texto.split('\n')
	textoFrases = []
	for parrafo in textoParrafos:
		if parrafo != '':
			auxList = []
			aux = parrafo.split('.')
			for frase in aux:
				if frase != '':
					auxList.append(frase)

			textoFrases.append(auxList)
	
	return(textoFrases)

def cuentaPalabrasRelevantes(texto, palabrasRelevantes):
	# OJO!! Esta funcion devuelve la palabra aunque sea mas larga, 
	# ejemplo: buscamos 'hola' dentro de: 'hola que tal?? holas hola!!' -> hola aparece 3 veces (holas tambien cuenta)
	result = dict()
	for palabra in palabrasRelevantes:
		result[palabra] = len(re.findall(palabra, texto))
		
	return(result)

def posWords(texto, palabra):
	return(list(all_occurences(texto, palabra)))

def all_occurences(texto, word):
	initial = 0
	while True:
		initial = texto.find(word, initial)
		if initial == -1: return
		yield initial
		initial += len(word)



# ============================= #
# ============================= #
# ===== Ejecucion proceso ===== #
# ============================= #
# ============================= #


# ==================================== #
# === Definiciones e importaciones === #
# ==================================== #

import platform
import os

# Possibles valors pais: esp, bra
pais = "esp"

# Possibles valors idioma: es, pt
idioma = "es"

nuevas_leyes = False

import sys

utiliza_logs = False
if len(sys.argv) > 7:
	if sys.argv[9] in ["True", "true", "TRUE", True]:
		utiliza_logs = True
	
if utiliza_logs:
	import time
	old_stdout = sys.stdout
	sufix = time.strftime("%Y_%m_%d_%H_%M_%S")
	filelogsname = "/home/ijdocs/just_docs/ner/logs/filelogs" + sufix + ".log"
	log_file = open(filelogsname, "w")
	sys.stdout = log_file


# sys.argv = ['099_Proceso_completo.py', '172.22.248.206:9229', '172.22.248.206:9229', 'aeacus_dev', 'ner_dev', 'uri', 'aeacus:fs#b2c8b153f65a42e7904c4c1f5fafb3ed', 'False', 'aeacus/aeacus/aeacus##sij2016/172.22.248.221/5432', 'True']
	

# '''
# import sys
# 
# old_stdout = sys.stdout
# log_file = open("message.log", "w")
# sys.stdout = log_file
# 
# 
# print "this will be written to message.log"
# sys.stdout = old_stdout
# 
# log_file.close()
# 
# '''



if platform.system() == "Windows":
	PATH = "D:/Judicial/"
	sys.path.append('C:/Python27/Lib/site-packages')
	eslocal = True
	es_hdfs = False
else:
	# Encontramos PATH actual
	tenimDirectori = False
	auxdir = sys.argv[0].split("/")
	vecLog = [item == "syntax" for item in auxdir]
	
	if any(vecLog):
		i = 0
		vecDir = []
		while not vecLog[i]:
			vecDir.append(auxdir[i])
			i += 1
			
		currDir = reduce_concat(vecDir, sep = "/")
		
		if os.path.isdir(currDir):
			tenimDirectori = True
			PATH = currDir + "/"

	if not tenimDirectori:
		auxdir = os.getcwd().split("/")
		vecLog = [item == "syntax" for item in auxdir]
		
		if any(vecLog):
			i = 0
			vecDir = []
			while not vecLog[i]:
				vecDir.append(auxdir[i])
				i += 1
				
			PATH = reduce_concat(vecDir, sep = "/") + "/"
		
		else:
			PATH = os.getcwd() + "/"
		
	eslocal = False

if not eslocal:
	if len(sys.argv) < 8:
		raise Exception("Tiene que haber como minimo 6 argumentos!!\nEjemplo llamada:\n	 python 000_Llamada_proceso.py ip_donde_leer(172.22.248.206:9229) ip_donde_escribir(172.22.248.206:9229) indice_donde_leer indice_donde_escribir criterio(name) fichero_a_procesar1 fichero_a_procesar2")
	else:
		ipread = sys.argv[1]
		ipwrite = sys.argv[2]
		indiceread = sys.argv[3]
		indicewrite = sys.argv[4]
		criterion = sys.argv[5]
		files2Eval = sys.argv[6].split("/")
		es_hdfs = sys.argv[7]
		pars_db = sys.argv[8]
		print "Los archivos a evaluar son los siguientes:\n	 '" + reduce(lambda x, y: x + "', '" + y, files2Eval) + "'."

if es_hdfs in ["True", "true", "TRUE", True]:
	es_hdfs = True
else:
	es_hdfs = False

if es_hdfs:
	PATH = "hdfs:///user/ijdocs/"
	import subprocess
	from pyspark import SparkConf, SparkContext
	from pyspark.sql import SQLContext, HiveContext
	
	# Crear la configuracion de spark
	conf = (SparkConf()
					.setAppName("ejemplo")
					.set("spark.executor.memory", "1g")
					.set("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/usr/bin/python")
					.set("spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON", "/usr/bin/python"))
	
	# Crear el SparkContext con la configuración anterior
	sc = SparkContext(conf=conf)
	
	# Conectores SQL para trabajar en nuestro script
	sqlContext = SQLContext(sc)
	hiveContext = HiveContext(sc) 

# Eliminamos archivos parciales del mismo ID
from os import listdir
from os import remove
INPUTNORMLEYES = PATH + "output/resultados_leyes/"
INPUTNORMFECHA = PATH + "output/resultados_fechas/"

if not es_hdfs and not eslocal:	 # Si es hdfs ja es fa en el run.sh aixo
	listdirleyes = listdir(INPUTNORMLEYES)
	listdirfechas = listdir(INPUTNORMFECHA)

	for filename in files2Eval:
		aux_name = filename.lower().replace(".pdf", "")
		[remove(INPUTNORMLEYES + file_del) for file_del in listdirleyes if aux_name in file_del]
		[remove(INPUTNORMFECHA + file_del) for file_del in listdirfechas if aux_name in file_del]

# if not es_hdfs:
#		execfile(PATH + "syntax/scripts/NER/funciones_elastic.py")
#		execfile(PATH + "syntax/scripts/NER/funciones_lectura_texto.py")

if not eslocal:
	for filename in files2Eval:
		eliminaDoc(filename, INDEX_NAME = indicewrite, TYPE_NAME =	"doc", newesconn = Elasticsearch([ipwrite]))

# ================== #
# === Parametros === #
# ================== #

guardaResultados = True
muestraHTML = True
guardaES = True


from os.path import exists
from os import listdir
from numpy import array
from numpy import ndarray
from datetime import datetime
import pandas as pd
from nltk.corpus import stopwords
from copy import deepcopy
from math import factorial
import json
from pattern.es import parsetree

if eslocal:
	INPUTREAD = PATH + "input/documentos_expedientes/"
	files2Eval = listdir(INPUTREAD)
	files2Eval = [filename for filename in files2Eval if ".pdf" in filename]
else:
	try:
		files2Eval
	except NameError:
		files2Eval = list_docs(criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)

# Proceso
for filename in files2Eval:

	ha_fallado = False
	print("Procesando " + filename + "...")

	try:
	
		# ================================ #
		# === 019_Implementacion_Leyes === #
		# ================================ #

		print "Procesando leyes..."

		# Current directories
		OUTPUTDIR = PATH + "output/resultados_leyes/"

		# Diccionarios
		DictPaisCompleteStrSearch = dict(
			es = dict(
				esp =		[
					dict(contiene = ['[\s|\n]art\.', '[\s|\n]ley[\s|\n]', 'articulo'], inicios = ['parrafo', 'ley', '[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado'], finales = [' en ', '\(', '((?<!art)\.[\s|\n])', ',[\s|\n]']),
		#			dict(contiene = ['[\s|\n]art\.', '[\s|\n]ley[\s|\n]', 'articulo'], inicios = ['parrafo', 'ley', '[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado'], finales = [' en ', '\(', '((?<!art)\.[\s|\n])', ',[\s|\n]', '[\s|\n]constitucion[\s|\n]', '[\s|\n]ce[\s|\n]']),
					dict(contiene = ['[\s|\n]acuerdo[\s|\n]', '[\s|\n]boe[\s|\n]'], inicios = ['acuerdo', '[\s|\n]boe[\s|\n]'], finales = ['[\s|\n]boe[\s|\n]', ' en ', '\(', '((?<!art)\.[\s|\n])', ',[\s|\n]']),
					dict(contiene = ['[\s|\n]real[\s|\n]decreto[\s|\n]', '[\s|\n]decreto[\s|\n]ley[\s|\n]', '[\s|\n]decreto-ley[\s|\n]', '[\s|\n]real-decreto[\s|\n]'], inicios = ['[\s|\n]real[\s|\n]decreto'], finales = [' en ', '\(', '((?<!art)\.[\s|\n])', ',[\s|\n]']),
					dict(contiene = ['reglamento'], inicios = ['[\s|\n][0-9]', '[\s|\n]art\.', '[\s|\n]articulo'], finales = [' en ', '\(', '((?<!art)\.[\s|\n])', ',[\s|\n]']),
					dict(contiene = ['\(art\.', '\(articulo'], inicios = ['\(art\.', '\(articulo'], finales = ["\)"])
				]
			),
			pt = dict(
				bra = [
					dict(contiene = ['[\s|\n]art\.', '[\s|\n]lei[\s|\n]', 'artigo'], inicios = ['lei', '[\s|\n]art\.', '[\s|\n]artigo', '[\s|\n]paragrafo'], finales = [',[\s|\n]', ';', '\-\\b']),
					dict(contiene = ['[\s|\n]decretos[\s|\n]lei[\s|\n]', '[\s|\n]decreto[\s|\n]lei[\s|\n]', '[\s|\n]decreto-lei[\s|\n]', '[\s|\n]decretos-lei[\s|\n]'], inicios = ['[\s|\n]decretos[\s|\n]lei[\s|\n]', '[\s|\n]decreto[\s|\n]lei[\s|\n]', '[\s|\n]decreto-lei[\s|\n]', '[\s|\n]decretos-lei[\s|\n]'], finales = [',[\s|\n]', ';', '[0-9][0-9][0-9][0-9]']),
					dict(contiene = ['regulacao'], inicios = ['[\s|\n][0-9]', '[\s|\n]art\.', '[\s|\n]artigo'], finales = [',[\s|\n]', ';', '\-\\b']),
					dict(contiene = ['\(art\.', '\(artigo'], inicios = ['\(art\.', '\(artigo'], finales = ["\)"])
				]
			)
		)

		CompleteStrSearch = DictPaisCompleteStrSearch[idioma][pais]
		CompleteStrSearchLeyes = deepcopy(CompleteStrSearch)

		DictPaisSearchMaxEnrere = dict(
			es = dict(
				esp = [
					dict(contiene = ['[\s|\n]art\.', '[\s|\n]ley[\s|\n]', 'articulo'], inicios = ['ley', '[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado', '[\s|\n]parrafo'], finales = ['[0-9][0-9][0-9][0-9]', ' en ', '\(', '((?<!art)\.[\s|\n])', ',[\s|\n]']),
					dict(contiene = ['[\s|\n]art\.', '[\s|\n]ley[\s|\n]', 'articulo'], inicios = ['ley', '[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado', '[\s|\n]parrafo'], finales = ['codigo penal', 'cp', '^l[a-z]+', '[\s|\n]constitucion[\s|\n]', '[\s|\n]ce[\s|\n]']),
					dict(contiene = ['[\s|\n]presupuestos[\s|\n]generales[\s|\n]del[\s|\n]estado[\s|\n]'], inicios = ['ley', '[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado', '[\s|\n]parrafo'], finales = ['[\s|\n]presupuestos[\s|\n]generales[\s|\n]del[\s|\n]estado[\s|\n]']),
					dict(contiene = ['[\s|\n]art\.', 'articulo'], inicios = ['[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado', '[\s|\n]parrafo'], finales = ['[\s|\n]constitucion[\s|\n]espanola[\s|\n]', '[\s|\n]constitucion[\s|\n]', '[\s|\n]ce[\s|\n]'])
				]
			),
			pt = dict(
				bra = [
					dict(contiene = ['[\s|\n]art\.', '[\s|\n]lei[\s|\n]', 'artigo'], inicios = ['lei', '[\s|\n]art\.', '[\s|\n]artigo'], finales = ['[0-9][0-9][0-9][0-9]']),
					dict(contiene = ['[\s|\n]art\.', '[\s|\n]lei[\s|\n]', 'artigo'], inicios = ['lei', '[\s|\n]art\.', '[\s|\n]artigo'], finales = ['\\bcodigo\\b', '[\s|\n]constituicao[\s|\n]', '[\s|\n]ce[\s|\n]', '\.o', 'lei'])
				]
			)
		)

		SearchMaxEnrere = DictPaisSearchMaxEnrere[idioma][pais]

		if eslocal:
			RawReadedFile = pdf2txt(INPUTREAD + filename)
		else:
			RawReadedFile = import_doc(filename, criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)
		readedFile = remove_accents(RawReadedFile)
		readedFile = readedFile.lower()
		
		# ================== #
		# === Script 010 === #
		# ================== #
		
		# Buscamos posicion strings
		index_points = []
		for dictstr2search in CompleteStrSearch:
			idxAct = []
			for str2search in dictstr2search['contiene']:
				idxAct.extend(buscaPosicionRegexTexto(str2search, readedFile))
				
			index_points.append(idxAct)
			
		# Buscamos inicios y finales de las posiciones encontradas
		leyes = []
		for i in range(len(CompleteStrSearch)):
			dictAct = CompleteStrSearch[i]
			for item in index_points[i]:
				# Preparamos textos de inicios y finales
				aux_texto_ini = readedFile[:item[1]]
				aux_texto_fin = readedFile[item[0]:]
				# Buscamos inicios y fines
				listInicios = []
				listFinal = []
				for i_inicio in dictAct['inicios']:
					listInicios.extend(buscaPosicionRegexTexto(i_inicio, aux_texto_ini))
				for i_final in dictAct['finales']:
					listFinal.extend(buscaPosicionRegexTexto(i_final, aux_texto_fin))
				listInicios = array(listInicios)
				listFinal = array(listFinal)
				for iitem in listFinal:
					if iitem[1] - iitem[0] <= 10:
						iitem[0] = iitem[1]
				if len(listInicios) > 0 and len(listFinal) > 0:
					if item[0] + min(listFinal[:, 0]) - max(listInicios[:, 0]) <= 500:
						leyes.append((max(listInicios[:, 0]), item[0] + min(listFinal[:, 0])))

		index_points_max_enrere = []
		for dictstr2search in SearchMaxEnrere:
			idxAct = []
			for str2search in dictstr2search['contiene']:
				idxAct.extend(buscaPosicionRegexTexto(str2search, readedFile))

			index_points_max_enrere.append(idxAct)

		for i in range(len(SearchMaxEnrere)):
			dictAct = SearchMaxEnrere[i]
			for item in index_points_max_enrere[i]:
				# Preparamos textos de inicios y finales
				aux_texto_ini = readedFile[:item[1]]
				aux_texto_fin = readedFile[item[0]:]
				# Buscamos inicios y fines
				listInicios = []
				listFinal = []
				for i_inicio in dictAct['inicios']:
					listInicios.extend(buscaPosicionRegexTexto(i_inicio, aux_texto_ini))
				for i_final in dictAct['finales']:
					listFinal.extend(buscaPosicionRegexTexto(i_final, aux_texto_fin))
				if len(listInicios) == 0 or len(listFinal) == 0:
					continue
				listInicios = array(listInicios)
				listFinal = array(listFinal)
				sel = abs(listInicios[:, 0] - len(aux_texto_ini)) < 75
				if any(sel):
					if min(listFinal[:, 1]) < 100:
						leyes.append((min(listInicios[sel, 0]), item[0] + min(listFinal[:, 1])))
					
		# Buscamos registros unicos
		leyes = list(set(leyes))
		
		if len(leyes) == 0:
			print "No tenemos leyes para este documento"
		else:
			
			# Regulamos que no se incluyan unos dentro de otros los textos
			leyes = pd.DataFrame(leyes)
			leyes = leyes.sort_values(0)
			leyes = leyes.values.tolist()
			jresta = 0
			for i in range(1, len(leyes)):
				if leyes[i - jresta][0] < leyes[i - 1 - jresta][1] and (leyes[i - 1 - jresta][1] - leyes[i - 1 - jresta][0]) < 200:
					leyes[i - jresta] = [leyes[i - 1 - jresta][0], max(leyes[i - 1 - jresta][1], leyes[i - jresta][1])]
					leyes.pop(i - 1 - jresta)
					jresta += 1

			# Unimos los que estan muy cerca
			lenMuyCerca = 20
			jresta = 0
			leyes = array(leyes)
			for i in range(1, len(leyes)):
				if (leyes[i - jresta, 0] - leyes[(i - 1 - jresta), 1]) <= lenMuyCerca:
					leyes[i - jresta, 0] = leyes[(i - 1 - jresta), 0]
					leyes = np.delete(leyes, i - 1 - jresta, 0)
					jresta += 1
			leyes = ndarray.tolist(leyes)
			
			# En caso de que el texto sea muy largo, cogemos solo el principio
			lenTextoMuyLargo = 200
			for item in leyes:
				if item[1] - item[0] > lenTextoMuyLargo:
					textocambia = readedFile[item[0]:item[1]]
					simbolospunc = [".", ",", ":", ";", ")"]
						
					for isimbol in simbolospunc:
						textocambia = textocambia.replace(isimbol, ".")
						
					textocambia = textocambia.split(".")
					aturat = False
					i = 0
					while not aturat:
						i += 1
						if len(textocambia) > (i + 1):
							aux_text = textocambia[i].strip()
							if aux_text == '':
								textocambia[i] = " "
							if len(re.findall("[0-9]", textocambia[i - 1][-1])) > 0 and len(re.findall("[0-9]", aux_text[0])) > 0:
								resultado = reduce_concat(textocambia[:(i + 1)], sep = ".")
							else:
								resultado = reduce_concat(textocambia[:i], sep = ".")
								aturat = True
						else:
							resultado = reduce_concat(textocambia, sep = ".")
							aturat = True

					item[1] = item[0] + len(resultado)
					
			# Tratamos de manera especial los que acaban en ","
			for item in leyes:
				textocambia = readedFile[item[0]:item[1]]
				simbolospunc = [".", ",", ":", ";", ")", "....", "...", ".."]
				if pais == "bra" and idioma == "pt":
					simbolospunc.append("-")
					
				for isimbol in simbolospunc:
					textocambia = textocambia.replace(isimbol, ".")
				
				textocambia = textocambia.split(".")
				aturat = False
				i = 0
				while not aturat:
					i += 1
					if len(textocambia) > (i + 1):
						aux_text = textocambia[i].strip()
						while len(aux_text) < 8 and len(textocambia) > (i + 1):
								i += 1
								aux_text = textocambia[i].strip()
								
						if aux_text == '':
							textocambia[i] = " "
							aux_text = " "
						if (len(re.findall("[0-9]", textocambia[i - 1][-1])) > 0 and len(re.findall("[0-9]", aux_text[0])) > 0) or ('art' in textocambia[i - 1] and len(re.findall("[0-9]", aux_text[0])) > 0):
							resultado = reduce_concat(textocambia[:(i + 1)], sep = ".")
						elif pais == "esp" and idioma == "es" and ('ley' in textocambia[i] or 'real decreto' in textocambia[i] or 'articulo' in textocambia[i] or 'acuerdo' in textocambia[i] or 'presupuesto' in textocambia[i] or any([ikey in textocambia[i] for ikey in DictSubsMeses[idioma][pais].keys()])):
							resultado = reduce_concat(textocambia[:(i + 1)], sep = ".")
						elif pais == "bra" and idioma == "pt" and ('lei' in textocambia[i] or 'artigos' in textocambia[i] or 'constituicao' in textocambia[i] or 'codigo' in textocambia[i] or len(re.findall('^o\\b', textocambia[i])) > 0 or len(re.findall('\\b[0-9][0-9]/[0-9][0-9]\\b', textocambia[i])) > 0 or len(re.findall('/[0-9][0-9][0-9][0-9]\\b', textocambia[i])) > 0 or any([ikey in textocambia[i] for ikey in DictSubsMeses[idioma][pais].keys()])):
							resultado = reduce_concat(textocambia[:(i + 1)], sep = ".")
						else:
							resultado = reduce_concat(textocambia[:i], sep = ".")
							aturat = True
					else:
						resultado = reduce_concat(textocambia, sep = ".")
						aturat = True
				
				item[1] = item[0] + len(resultado)
			
			
			# ================== #
			# === Script 011 === #
			# ================== #
			
			# === #
			# Canvi respecte script 011!!
			listRefs = deepcopy(leyes)
			listRefs = pd.DataFrame(listRefs)
			listRefs.columns = ["PosInicio", "PosFin"]
			listRefs["Referencia"] = ""
			listRefs["RefNorm"] = ""
			for irow in range(listRefs.shape[0]):
				listRefs["Referencia"][irow] = readedFile[listRefs["PosInicio"][irow]:listRefs["PosFin"][irow]]
				stop_words = stopwords.words('spanish')
				for i in range(len(stop_words)):
					stop_words[i] = remove_accents(stop_words[i])
				
				reference_wd = listRefs["Referencia"][irow].split(" ")
				
				reference_norm = [wr for wr in reference_wd if not wr in stop_words]
				reference_norm = reduce_concat(reference_norm, sep = " ")
				reference_norm = reference_norm.strip()
				punct_symb = [".", ",", ":", ";", "!", "?"]
				if reference_norm[0] in punct_symb:
					reference_norm = reference_norm[1:]
				if reference_norm[-1] in punct_symb:
					reference_norm = reference_norm[:-1]
					
				reference_norm = reference_norm.strip()
				reference_norm = reference_norm.replace("art.", "art").replace("art ", "articulo ").replace("\n", "")
				while "	 " in reference_norm:
					reference_norm = reference_norm.replace("	 ", " ")
				reference_norm = reference_norm.strip()
				listRefs["RefNorm"][irow] = reference_norm
			# === #
			
			# Validamos que los articulos y leyes tengan algun numero
			for i in range(len(listRefs) - 1, -1, -1):
				if len(re.findall("[0-9]", listRefs["Referencia"][i])) == 0:
					listRefs = listRefs[listRefs.index != i]
			
			listRefs = listRefs.reset_index()
			
			# Buscamos coincidencias dentro de los textos
			listRefs["AuxRefNorm"] = listRefs["RefNorm"]
			for i in range(len(listRefs)):
				for j in range(len(listRefs)):
					if i != j:
						tocat = False
						item1 = listRefs["RefNorm"][i]
						item2 = listRefs["RefNorm"][j]
						if len(item1) > len(item2):
							if item2 in item1:
								listRefs["RefNorm"][i] = item2
								tocat = True
						elif len(item1) < len(item2):
							if item1 in item2:
								listRefs["RefNorm"][j] = item1
								tocat = True
						
						if not tocat:
							auxit1 = item1.split(" ")
							auxit2 = item2.split(" ")
							auxin1 = reduce(lambda x, y: x + y, [1*(x in auxit2) for x in auxit1])
							auxin2 = reduce(lambda x, y: x + y, [1*(x in auxit1) for x in auxit2])
							if auxin1 > 4 and auxin2 > 4 and (auxin1/len(auxit1) > 0.5 or auxin2/len(auxit2) > 0.5):
								listRefs["RefNorm"][i] = reduce_concat([x for x in auxit1 if x in auxit2], sep = " ")
								listRefs["RefNorm"][j] = reduce_concat([x for x in auxit1 if x in auxit2], sep = " ")

			# Comprobamos que no hayamos eliminado nada que nos pueda ser util
			regUnics = pd.unique(listRefs["RefNorm"])
			for registre in regUnics:
				sel = listRefs["RefNorm"] == registre
				totesParaules = []
				for x in listRefs["AuxRefNorm"][sel]:
					totesParaules.extend(x.split(" "))
				totesParaules = list(set(totesParaules))
				resParaules = []
				# Veiem en quants textos es repeteix cada paraula i si es mes del 50% la tenim en compte
				for paraula in totesParaules:
					val = 0.
					for x in listRefs["AuxRefNorm"][sel]:
						if paraula in x:
							val += 1./sum(sel)
					
					if val > 0.5:
						resParaules.append(paraula)
				
				# Incorporamos las palabras que hemos visto que se repiten
				auxl = [[x, len(x)] for x in listRefs["AuxRefNorm"][sel] if all([paraula in x for paraula in resParaules])]
				auxl = np.array(auxl)
				sent = str(auxl[auxl[:, 1].astype(int) == max(auxl[:, 1].astype(int)), 0][0])
				auxIdx = np.array([(sent.index(paraula), sent.index(paraula) + len(paraula)) for paraula in resParaules])
				sent = sent[min(auxIdx[:,0]):max(auxIdx[:,1])]
				listRefs["RefNorm"][sel] = sent
			
			
			# ============================= #
			# === Escribimos resultados === #
			# ============================= #
			
			# Guardamos outputs y los dejamos en ES
			if guardaResultados:
				if es_hdfs:
					fileSave = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_info_pos"
					guardaDFHDFS(listRefs, fileSave, sc)
					
				else:
					listRefs.to_csv(path_or_buf = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_info_pos.csv", sep = "|")
			
			if muestraHTML and not es_hdfs:
				# Ordenamos para que los que estan mas repetidos tengan colores
				listRefs['order'] = [sum(listRefs['RefNorm'] == listRefs['RefNorm'][i]) for i in xrange(len(listRefs))]
				listRefs = listRefs.sort_values("order", ascending = False)
				regUnics = pd.unique(listRefs["RefNorm"])
				listRefs["id_reg"] = ""
				compta = 0
				for registre in regUnics:
					sel = listRefs["RefNorm"] == registre
					listRefs["id_reg"][sel] = compta
					compta += 1
				
				# Escribimos html
				listRefs['mark'] = 'mark09'
				for i in range(1, 9):
					sel = listRefs["id_reg"] == (i - 1)
					listRefs["mark"][sel] = 'mark0' + str(i)
				
				textohtml = "<head> <link rel = 'stylesheet' type = 'text/css' href = 'styles_css.css'> </head>"
				i_suma = 0
				for i in range(listRefs.shape[0]):
					readedFile = readedFile[:(int(listRefs["PosInicio"][i]) + i_suma)] + "<" + listRefs["mark"][i] + ">" + readedFile[(int(listRefs["PosInicio"][i]) + i_suma):(int(listRefs["PosFin"][i]) + i_suma)] + "</" + listRefs["mark"][i] + ">" + readedFile[(int(listRefs["PosFin"][i]) + i_suma):]
					i_suma += 17
				textohtml += "<p>" + readedFile + "</p>"
				
				filehtml = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_muestra_leyes.html"
				filehtml = open(filehtml, mode = "w+")
				filehtml.write(textohtml)
				filehtml.close()
				
			if guardaES:
				pass

		print "Leyes finalizado."

		# ================================= #
		# === 039_Implementacion_fechas === #
		# ================================= #

		print "Procesando fechas"

		# Current directories
		OUTPUTDIR = PATH + "output/resultados_fechas/"

		# Diccionarios
		DictPaisCompleteStrSearch = dict(
			es = dict(
				esp = [
					dict(contiene = ['\\b[0-3][0-9][\/\-][0-1][0-9][\/\-][1-2][0-9][0-9][0-9]\\b|\\b[1-2][0-9][0-9][0-9][\/\-][0-1][0-9][\/\-][0-3][0-9]\\b|\\b[0-9][0-9][\/\-][0-1][0-9][\/\-][0-9][0-9]\\b'], 
							 inicios = ['\\b[0-3][0-9][\/\-][0-1][0-9][\/\-][1-2][0-9][0-9][0-9]\\b|\\b[1-2][0-9][0-9][0-9][\/\-][0-1][0-9][\/\-][0-3][0-9]\\b|\\b[0-9][0-9][\/\-][0-1][0-9][\/\-][0-9][0-9]\\b'],
							 finales = ['\\b[0-3][0-9][\/\-][0-1][0-9][\/\-][1-2][0-9][0-9][0-9]\\b|\\b[1-2][0-9][0-9][0-9][\/\-][0-1][0-9][\/\-][0-3][0-9]\\b|\\b[0-9][0-9][\/\-][0-1][0-9][\/\-][0-9][0-9]\\b']
					),
					dict(contiene = ['[^0-9/:]([12][890][0-9][0-9])[^0-9/:]'], 
							 inicios = ['[^0-9/:]([12][890][0-9][0-9])[^0-9/:]'],
							 finales = ['[^0-9/:]([12][890][0-9][0-9])[^0-9/:]']
					),
					dict(contiene = ['\\benero\\b', '\\bfebrero\\b', '\\bmarzo\\b', '\\babril\\b', '\\bmayo\\b', '\\bjunio\\b', '\\bjulio\\b', '\\bagosto\\b', '\\bseptiembre\\b', '\\bsetiembre\\b', '\\boctubre\\b', '\\bnoviembre\\b', '\\bdiciembre\\b'], 
							 inicios = ['[0-3][0-9]', '\\b[0-9]', "\\buno", "\\bdos", "\\btres", "\\bcuatro", "\\bcinco", "\\bseis", "\\bsiete", "\\bocho", "\\bnueve", "\\bdiez", "\\bonce", "\\bdoce", "\\btrece", "\\bcatorce", "\\bquince", "\\bdieciseis", "\\bdiecisiete", "\\bdieciocho", "\\bdiecinueve", "\\bveinte", "\\bventi", "\\bveinti", "\\btreinta", "\\btrenta"],
							 finales = ["\,", "\.(?![0-9])", "\:", "\)", "\(", "[1-2][0-9][0-9][0-9]", "[1-2]\.[0-9][0-9][0-9]", "uno", "(?<! de) dos", "tres", "cuatro", "cinco", "seis", "siete", "ocho", "nueve", "diez", "once", "doce", "trece", "catorce", "quince", "dieciseis", "diecisiete", "dieciocho", "diecinueve", "veinte", "treinta", "cuarenta", "cincuenta", "sesenta", "setenta", "ochenta", "noventa", "cien", "cientos"]
					)
				]
			),
			pt = dict(
				bra = [
					dict(contiene = ['\\b[0-3][0-9][\/\-\.][0-1][0-9][\/\-\.][1-2][0-9][0-9][0-9]\\b|\\b[1-2][0-9][0-9][0-9][\/\-\.][0-1][0-9][\/\-\.][0-3][0-9]\\b|\\b[0-9][0-9][\/\-\.][0-1][0-9][\/\-\.][0-9][0-9]\\b'], 
							 inicios = ['\\b[0-3][0-9][\/\-\.][0-1][0-9][\/\-\.][1-2][0-9][0-9][0-9]\\b|\\b[1-2][0-9][0-9][0-9][\/\-\.][0-1][0-9][\/\-\.][0-3][0-9]\\b|\\b[0-9][0-9][\/\-\.][0-1][0-9][\/\-\.][0-9][0-9]\\b'],
							 finales = ['\\b[0-3][0-9][\/\-\.][0-1][0-9][\/\-\.][1-2][0-9][0-9][0-9]\\b|\\b[1-2][0-9][0-9][0-9][\/\-\.][0-1][0-9][\/\-\.][0-3][0-9]\\b|\\b[0-9][0-9][\/\-\.][0-1][0-9][\/\-\.][0-9][0-9]\\b']
					),
					dict(contiene = ['[^0-9/:]([12][890][0-9][0-9])[^0-9/:]'], 
							 inicios = ['[^0-9/:]([12][890][0-9][0-9])[^0-9/:]'],
							 finales = ['[^0-9/:]([12][890][0-9][0-9])[^0-9/:]']
					),
					dict(contiene = ['\\bjaneiro\\b', '\\bfevereiro\\b', '\\bmarco\\b', '\\babril\\b', '\\bmaio\\b', '\\bjunho\\b', '\\bjulho\\b', '\\bagosto\\b', '\\bsetembro\\b', '\\boutubro\\b', '\\bnovembro\\b', '\\bdezembro\\b'], 
							 inicios = ['[0-3][0-9]', '\\b[0-9]', "\\bum\\b", "\\bdois\\b", "\\btres\\b", "\\bquatro\\b", "\\bcinco\\b", "\\bseis\\b", "\\bsete\\b", "\\boito\\b", "\\bnove\\b", "\\bdez\\b", "\\bonze\\b", "\\bdoze\\b", "\\btreze\\b", "\\bcatorze\\b", "\\bquinze\\b", "\\bdezesseis\\b", "\\bdezessete\\b", "\\bdezoito\\b", "\\bdezenove\\b", "\\bvinte\\b", "\\btrinta\\b"],
							 finales = ["\,", "\.(?![0-9])", "\:", "\)", "\(", "[1-2][0-9][0-9][0-9]", "[1-2]\.[0-9][0-9][0-9]", "\\bum\\b", "\\bdois\\b", "\\btres\\b", "\\bquatro\\b", "\\bcinco\\b", "\\bseis\\b", "\\bsete\\b", "\\boito\\b", "\\bnove\\b", "\\bdez\\b", "\\bonze\\b", "\\bdoze\\b", "\\btreze\\b", "\\bcatorze\\b", "\\bquinze\\b", "\\bdezesseis\\b", "\\bdezessete\\b", "\\bdezoito\\b", "\\bdezenove\\b", "\\bvinte\\b", "\\btrinta\\b", "\\bquarenta\\b", "\\bcinquenta\\b", "\\bsessenta\\b", "\\bsetenta\\b", "\\boitenta\\b", "\\bnoventa\\b", "\\bcem\\b", "\\bduzentos\\b", "\\btrezentos\\b", "\\bquatrocentos\\b", "\\bquinhentos\\b", "\\bseiscentos\\b", "\\bsetecentos\\b", "\\boitocentos\\b", "\\bnovecentos\\b"]
					)
				]
			)
		)

		CompleteStrSearch = DictPaisCompleteStrSearch[idioma][pais]

		limSimFrases = 0.15	 # Limit a partir del qual diem que 2 frases s'assemblen

		# Leemos fichero
		if eslocal:
			RawReadedFile = pdf2txt(INPUTREAD + filename)
		else:
			RawReadedFile = import_doc(filename, criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)
		readedFile = remove_accents(RawReadedFile)
		readedFile = readedFile.lower()
		
		# ================== #
		# === Script 030 === #
		# ================== #
		
		# Buscamos posicion strings
		index_points = []
		for dictstr2search in CompleteStrSearch:
			idxAct = []
			for str2search in dictstr2search['contiene']:
				idxAct.extend(buscaPosicionRegexTexto(str2search, readedFile))
				
			index_points.append(idxAct)
		
		# Buscamos inicios y finales de las posiciones encontradas
		fechas = []
		for i in range(len(CompleteStrSearch)):
			dictAct = CompleteStrSearch[i]
			for item in index_points[i]:
				# Preparamos textos de inicios y finales
				aux_texto_ini = readedFile[:item[1]]
				aux_texto_fin = readedFile[item[0]:]
				# Buscamos inicios y fines
				listInicios = []
				listFinal = []
				for i_inicio in dictAct['inicios']:
					listInicios.extend(buscaPosicionRegexTexto(i_inicio, aux_texto_ini))
				for i_final in dictAct['finales']:
					listFinal.extend(buscaPosicionRegexTexto(i_final, aux_texto_fin))
				listInicios = array(listInicios)
				listFinal = array(listFinal)
				fechas.append((max(listInicios[:, 0]), item[0] + min(listFinal[:, 1])))
				if (fechas[len(fechas) - 1][1] - fechas[len(fechas) - 1][0]) > 60:	# Si hi ha molta distancia es perque no ho ha agafat be, modifiquem
					lletresFinsInici = len(aux_texto_ini) - fechas[len(fechas) - 1][0]
					lletresFinsFinal = min(listFinal[:, 1])
					if lletresFinsInici > lletresFinsFinal:
						fechas[len(fechas) - 1] = (item[0], fechas[len(fechas) - 1][1])
					else:
						fechas[len(fechas) - 1] = (fechas[len(fechas) - 1][0], item[1])
						

		# Buscamos registros unicos
		fechas = list(set(fechas))

		if len(fechas) == 0:
			print "No tenemos fechas para este documento"
		else:
			# Regulamos que no se incluyan unos dentro de otros los textos
			fechas = pd.DataFrame(fechas)
			fechas = fechas.sort_values(0)
			fechas = fechas.values.tolist()
			jresta = 0
			for i in range(1, len(fechas)):
				if fechas[i - jresta][0] < fechas[i - 1 - jresta][1]:
					fechas[i - jresta] = [fechas[i - 1 - jresta][0], max(fechas[i - 1 - jresta][1], fechas[i - jresta][1])]
					fechas.pop(i - 1 - jresta)
					jresta += 1

			# ================== #
			# === Script 031 === #
			# ================== #

			# === #
			# Canvi respecte script 031!!
			listRefs = deepcopy(fechas)
			listRefs = pd.DataFrame(listRefs)
			listRefs.columns = ["PosInicio", "PosFin"]
			listRefs["Referencia"] = ""
			for i in range(listRefs.shape[0]):
				listRefs["Referencia"][i] = readedFile[listRefs["PosInicio"][i]:listRefs["PosFin"][i]]
			# === #
			
			# Extraemos las referencias mediante las posiciones
			listRefs["Ref_Orig"] = listRefs["Referencia"]
			
			# Hacemos modificaciones en las referencias
			caracteres_elimina = [":", ".", "(", ")", ",", ";"]
			caracteres_espacio = ["\n", "/", "-"]
			for ce in caracteres_elimina:
				listRefs["Referencia"] = listRefs["Referencia"].str.replace(ce, "")
			for ce in caracteres_espacio:
				listRefs["Referencia"] = listRefs["Referencia"].str.replace(ce, " ")

			for i in range(listRefs.shape[0]):
				listRefs["Referencia"][i] = quitaDoblesEspacios(listRefs["Referencia"][i])
			listRefs["Referencia"] = listRefs["Referencia"].str.strip()
			
			# Normalizamos los campos
			totali = listRefs.shape[0]
			listRefs["Referencia_Normalizada"] = ''
			dictRBind = dict(PosInicio = [], PosFin = [], Referencia = [], Ref_Orig = [], Referencia_Normalizada = [])
			idrop = []
			for i in range(totali):
				normaliza = listRefs["Referencia"][i]
				normalizado = normaliza
				hihastrings = len(re.findall('[a-z]', normaliza)) > 0
				if hihastrings:
					normalizado = []
					aux_str = normaliza.split(" de ")
					for item in aux_str:
						item = item.strip()
						if len(re.findall('[a-z]', item)) > 0 and len(re.findall('[0-9]', item)) > 0:
							aux_item = item.split(" ")
							item = []
							for i_item in aux_item:
								if len(i_item) == 1:
									i_item = '0' + i_item
								elif i_item in DictSubsMeses[idioma][pais].keys():
									i_item = DictSubsMeses[idioma][pais][i_item]
								item.append(i_item)
							item = reduce_concat(item, sep = " ")
						elif item in DictSubsMeses[idioma][pais].keys():
							item = DictSubsMeses[idioma][pais][item]
						else:
							if len(re.findall('[a-z]', item)) > 0:
								try:
									item = transformaNumsString(item)
								except:
									if any([ikey in item for ikey in DictSubsMeses[idioma][pais].keys()]):
										for ikey in DictSubsMeses[idioma][pais].keys():
											item = item.replace(ikey, DictSubsMeses[idioma][pais][ikey])
										item = re.sub('[a-z]', "", item)
										item = item.replace(" ", "")
							if len(item) == 1 and len(re.findall('[a-z]', item)) == 0:
								item = '0' + item
						normalizado.append(item)
					
					normalizado = reduce_concat(normalizado, sep = " ")
					
					if len(re.findall('[a-z]', normalizado)) > 0:
						# En este caso significa que no hemos detectado bien la entidad fecha, tenemos que modificarla
						# Separamos todas las fechas y creamos de nuevo las entidades por separado
						aux_norm = re.sub("[0-9]", "9", re.sub("[0-9] [0-9]", "9", normalizado))
						validaauxnorm = ''
						while aux_norm != validaauxnorm:
							validaauxnorm = deepcopy(aux_norm)
							aux_norm = aux_norm.replace("99", "9")
						
						aux_text = readedFile[listRefs["PosInicio"][i]:listRefs["PosFin"][i]]
						splitea = [item for item in aux_norm.split("9") if item not in ["", " "]]
						
						# Hacemos lo mismo para el original
						aux_normorig = listRefs["Ref_Orig"][i]
						vecValida = [itext in aux_normorig for itext in splitea]
						if all(vecValida):
							spliteaorig = splitea
						else:
							for ikey in DictSubsMeses[idioma][pais].keys():
								aux_normorig = aux_normorig.replace(ikey, DictSubsMeses[idioma][pais][ikey])
							aux_normorig = re.sub("[0-9]", "9", re.sub("[0-9] [0-9]", "9", aux_normorig))
							validaauxnorm = ''
							while aux_normorig != validaauxnorm:
								validaauxnorm = deepcopy(aux_normorig)
								aux_normorig = aux_normorig.replace("99", "9")
								
							spliteaorig = [item for item in aux_normorig.split("9") if item not in ["", " "]]
							if len(splitea) < len(spliteaorig):
								lenVecs = [len(isplit) for isplit in spliteaorig]
								while len(splitea) < len(spliteaorig):
									spliteaorig.pop(lenVecs.index(min(lenVecs)))
									lenVecs = [len(isplit) for isplit in spliteaorig]
							
						for isplit in range(len(splitea)):
							splitact = splitea[isplit]
							splitactorig = spliteaorig[isplit]
							auxref = listRefs["Referencia"][i].split(splitact)
							if len(auxref) > 1:
								listRefs["Referencia"][i] = reduce_concat(auxref[1:], sep = splitact)
							auxref[0] = auxref[0].strip()
							dictRBind["Referencia"].append(auxref[0])
							auxreforig = listRefs["Ref_Orig"][i].split(splitactorig)
							if len(auxref) > 1:
								listRefs["Ref_Orig"][i] = reduce_concat(auxreforig[1:], sep = splitactorig)
							auxreforig[0] = auxreforig[0].strip()
							dictRBind["Ref_Orig"].append(auxreforig[0])
							auxnormref = normalizado.split(splitact)
							if len(auxref) > 1:
								normalizado = reduce_concat(auxnormref[1:], sep = splitact).strip()
							auxnormref[0] = auxnormref[0].strip()
							dictRBind["Referencia_Normalizada"].append(auxnormref[0])
							auxpos = aux_text.index(auxreforig[0]) + listRefs["PosInicio"][i]
							dictRBind["PosInicio"].append(auxpos)
							dictRBind["PosFin"].append(auxpos + len(auxreforig[0]))
							if isplit == (len(splitea) - 1) and len(auxref) > 1:
								auxref[1] = auxref[1].strip()
								dictRBind["Referencia"].append(auxref[1])
								auxreforig[1] = auxreforig[1].strip()
								dictRBind["Ref_Orig"].append(auxreforig[1])
								auxnormref[1] = auxnormref[1].strip()
								dictRBind["Referencia_Normalizada"].append(auxnormref[1])
								auxpos = aux_text.index(auxreforig[1]) + listRefs["PosInicio"][i]
								dictRBind["PosInicio"].append(auxpos)
								dictRBind["PosFin"].append(auxpos + len(auxreforig[1]))
						
						idrop.append(i)
						
				listRefs["Referencia_Normalizada"][i] = normalizado

			# Unimos los nuevos datos y borramos los antiguos
			dictRBind = pd.DataFrame(dictRBind)
			if dictRBind.shape[0] > 0:
				idrop.reverse()
				for i in idrop:
					listRefs = listRefs.drop(listRefs.index[i])
					listRefs = listRefs.append(dictRBind)
					listRefs = listRefs[["PosInicio", "PosFin", "Referencia", "Ref_Orig", "Referencia_Normalizada"]]
				
			# Prevenim errors
			listRefs = listRefs.reset_index()
			listRefs = listRefs[["PosInicio", "PosFin", "Referencia", "Ref_Orig", "Referencia_Normalizada"]]
			listRefs = listRefs.drop_duplicates()
			listRefs = listRefs[listRefs["Referencia_Normalizada"] != ""]
			listRefs = listRefs[listRefs["Referencia"] != ""]
			listRefs = listRefs.reset_index()
			listRefs = listRefs[["PosInicio", "PosFin", "Referencia", "Ref_Orig", "Referencia_Normalizada"]]
			listRefs = listRefs.sort_values(by = "PosInicio")
			
			# Eliminamos fechas mas cortas que 4 caracteres (1 solo año no entraria)
			listRefs["Eliminam"] = 0
			for i in range(listRefs.shape[0]):
				if len(listRefs["Referencia_Normalizada"][i]) <= 4:
					listRefs["Eliminam"][i] = 1
					
			listRefs = listRefs[listRefs["Eliminam"] == 0]
			listRefs = listRefs.reset_index()
			listRefs = listRefs[["PosInicio", "PosFin", "Referencia", "Ref_Orig", "Referencia_Normalizada"]]
			
			# ================== #
			# === Script 032 === #
			# ================== #
			
			# Montamos un string con la misma estructura y con acentos
			try:
				RawReadedFileAcc = str(RawReadedFile.decode("utf8").encode("latin1", 'ignore'))
			except:
				RawReadedFileAcc = str(RawReadedFile.encode("latin1", "ignore"))
			
			i = 0
			while i < len(RawReadedFileAcc):
				letterAcc = RawReadedFileAcc[i]
				letterra = readedFile[i]
				if remove_accents(letterAcc, enc = "latin1").lower() != letterra:
					if letterAcc == readedFile[i + 1]:
						readedFile = readedFile[:i] + readedFile[(i + 1):]
					else:
						RawReadedFileAcc = RawReadedFileAcc[:i] + RawReadedFileAcc[(i + 1):]
				else:
					i += 1

			if len(RawReadedFileAcc) != len(readedFile):
				raise Exception("Tienen que tener siempre la misma longitud!!")
			
			# Cogemos los textos que hay antes y despues de cada fecha
			ddNorm = deepcopy(listRefs)
			ddNorm["Antes"] = ""
			ddNorm["Despues"] = ""
			x = 1000
			for irow in range(ddNorm.shape[0]):
				ddNorm["Antes"][irow] = RawReadedFileAcc[(ddNorm["PosInicio"][irow] - x):ddNorm["PosInicio"][irow]]
				aux_text = RawReadedFileAcc[(ddNorm["PosFin"][irow] - 1):(ddNorm["PosFin"][irow] + x)]
				if len(aux_text) > 0:
					if aux_text[0] == ddNorm["Referencia"][irow][-1]:
						ddNorm["Despues"][irow] = aux_text[1:]
					else:
						ddNorm["Despues"][irow] = aux_text
				else:
					ddNorm["Despues"][irow] = ""
			
			# Pasamos a frases los textos de antes y despues
			for irow in range(ddNorm.shape[0]):
				ddNorm["Antes"][irow] = ddNorm["Antes"][irow].replace("\n", " ")
				ddNorm["Despues"][irow] = ddNorm["Despues"][irow].replace("\n", " ")
				auxa = sentences(ddNorm["Antes"][irow])
				auxa = auxa.split(" .\n")
				ddNorm["Antes"][irow] = auxa[len(auxa) - 1]
				auxd = sentences(ddNorm["Despues"][irow])
				auxd = auxd.split(" .\n")
				ddNorm["Despues"][irow] = auxd[0]
			
			# Vemos que relevancia tiene cada elemento
			aux_tt = ddNorm["Referencia_Normalizada"].value_counts()
			aux_tt2 = aux_tt/max(aux_tt)

			aux_tt = pd.DataFrame(data = dict(Referencia_Normalizada = list(aux_tt.index), Apariciones = list(aux_tt.values), Relevancia = list(aux_tt2.values)))
			ddNorm = ddNorm.merge(aux_tt, 'left')
			
			# Normalizamos las sentencias
			result = dict()
			for val in ddNorm["Referencia_Normalizada"].unique():
				if len(val) <= 4:
					# Aqui tractem cada un dels valors per separat perque fan referencia a anys (fem un bucle per cadascun dels valors que siguin aquest)
					sel = ddNorm["Referencia_Normalizada"] == val
					selNum = np.array(np.where(sel))
					selNum = np.ndarray.tolist(selNum)[0]
					selNum = [int(x) for x in selNum]
					aux_data = ddNorm[sel]
					aux_data = aux_data.reset_index()
					for irow in range(aux_data.shape[0]):
						texto = aux_data["Antes"][irow] + aux_data["Ref_Orig"][irow] + aux_data["Despues"][irow]
						result[val + "_" + str(irow)] = dict(descripcion = texto.strip())
						result[val + "_" + str(irow)]["posiciones"] = dict(inicio = list(aux_data["PosInicio"]), fin = list(aux_data["PosFin"])) 
						result[val + "_" + str(irow)]["referencias"] = list(aux_data["Ref_Orig"])
						ddNorm["Referencia_Normalizada"][selNum[irow]] = val + "_" + str(irow)
				elif int(aux_tt["Apariciones"][aux_tt["Referencia_Normalizada"] == val]) == 1:
					# Aqui posem les frases que apareixen a pinyo
					sel = ddNorm["Referencia_Normalizada"] == val
					aux_data = ddNorm[sel]
					aux_data = aux_data.reset_index()
					texto = str(aux_data["Antes"][0]) + str(aux_data["Ref_Orig"][0]) + str(aux_data["Despues"][0])
					result[val] = dict(descripcion = texto.strip())
					result[val]["posiciones"] = dict(inicio = list(aux_data["PosInicio"]), fin = list(aux_data["PosFin"])) 
					result[val]["referencias"] = list(aux_data["Ref_Orig"])
				else:
					# Aqui fem l'analisis per mes d'una frase, si son iguals intentem fer una frase descriptiva, sino doncs per cada item 
					frases = []
					aux_data = ddNorm[ddNorm["Referencia_Normalizada"] == val]
					aux_data = aux_data.reset_index()
					for irow in range(aux_data.shape[0]):
						texto = aux_data["Antes"][irow] + aux_data["Ref_Orig"][irow] + aux_data["Despues"][irow]
						frases.append(texto)

					# Comparem frases i veiem quines s'assemblen a quines
					auxRes = comparaFrases(frases)
					gruposUnidos = unirGrupos(list(auxRes["item1"]), list(auxRes["item2"]), list(auxRes["valor_comp"] > limSimFrases))

					# Extraiem les frases mes rellevants per a cada grup
					frasesGrup = []
					for grupo in gruposUnidos:
						frases_grupo_act = [""]
						for element in grupo:
							frases_grupo_act[0] = frases_grupo_act[0] + " " + frases[element]
							frases_grupo_act.append(frases[element])
						
						auxRes = comparaFrases(frases_grupo_act, totesContraTotes = False)
						auxResSel = auxRes[(auxRes["item1"] == 0) | (auxRes["item2"] == 0)]
						auxResSel = auxResSel[auxResSel["valor_comp"] == max(auxResSel["valor_comp"])].reset_index()
						auxResSel = auxResSel.loc[0, ["item1", "item2"]]
						i = int(auxResSel[auxResSel != 0]) - 1
						frasesGrup.append(frases[grupo[i]])
						
					if len(frasesGrup) == 1:
						result[val] = dict(descripcion = frasesGrup[0])
						result[val]["posiciones"] = dict(inicio = list(aux_data["PosInicio"]), fin = list(aux_data["PosFin"])) 
						result[val]["referencias"] = list(aux_data["Ref_Orig"])
					else:
						for igrupo in range(len(gruposUnidos)):
							result[val + "_" + str(igrupo + 1)] = dict(descripcion = frasesGrup[igrupo])
							result[val + "_" + str(igrupo + 1)]["posiciones"] = dict(inicio = list(aux_data.loc[(gruposUnidos[igrupo])]["PosInicio"]), fin = list(aux_data.loc[(gruposUnidos[igrupo])]["PosFin"]))
							result[val + "_" + str(igrupo + 1)]["referencias"] = list(aux_data.loc[(gruposUnidos[igrupo])]["Ref_Orig"])
							for ielement in range(len(gruposUnidos[igrupo])):
								ddNorm["Referencia_Normalizada"][aux_data["index"][gruposUnidos[igrupo][ielement]]] = val + "_" + str(igrupo + 1)
			
			# Pasamos el diccionario a formato string para escribirlo en json
			for ikey in result.keys():
				result[ikey]["descripcion"] = result[ikey]["descripcion"].decode("latin1")
				for i in range(len(result[ikey]["referencias"])):
					result[ikey]["referencias"][i] = result[ikey]["referencias"][i].decode("latin1")
					result[ikey]["posiciones"]["inicio"][i] = str(result[ikey]["posiciones"]["inicio"][i])
					result[ikey]["posiciones"]["fin"][i] = str(result[ikey]["posiciones"]["fin"][i])
					
			# ================== #
			# === Script 034 === #
			# ================== #
			
			# ATENCIO!! Falta incorporar aquesta part a la resta de tipologies!!
			
			# Cogemos los textos que hay antes y despues de cada fecha
			ddNorm["Antes"] = ""
			ddNorm["Despues"] = ""
			x = 1000
			for irow in range(ddNorm.shape[0]):
				ddNorm["Antes"][irow] = readedFile[max(ddNorm["PosInicio"][irow] - x, 0):ddNorm["PosInicio"][irow]]
				aux_text = readedFile[(ddNorm["PosFin"][irow] - 1):min(ddNorm["PosFin"][irow] + x, (len(readedFile) - 1))]
				if len(aux_text) > 0:
					if aux_text[0] == ddNorm["Referencia"][irow][-1]:
						ddNorm["Despues"][irow] = aux_text[1:]
					else:
						ddNorm["Despues"][irow] = aux_text
				else:
					ddNorm["Despues"][irow] = ""
				
				
			# Pasamos a frases los textos de antes y despues
			minText = 15
			ddNorm["Contexto"] = ""
			for irow in range(ddNorm.shape[0]):
				ddNorm["Antes"][irow] = ddNorm["Antes"][irow].replace("\n", " ")
				ddNorm["Despues"][irow] = ddNorm["Despues"][irow].replace("\n", " ")
				auxa = sentences(ddNorm["Antes"][irow])
				auxa = auxa.split(" .\n")
				textant = auxa[len(auxa) - 1]
				if len(textant) < minText:
					textant = reduce_concat(auxa[(len(auxa) - 2):(len(auxa) - 1)], sep = ". ")
				ddNorm["Antes"][irow] = textant
				
				auxd = sentences(ddNorm["Despues"][irow])
				auxd = auxd.split(" .\n")
				textdsp = auxd[0]
				if len(textdsp) < minText:
					textdsp = reduce_concat(auxd[:2], sep = ". ")
				ddNorm["Despues"][irow] = textdsp
				
				ddNorm["Contexto"][irow] = ddNorm["Antes"][irow] + ddNorm["Referencia"][irow] + ddNorm["Despues"][irow]
			
			
			# Normalizamos frases contexto
			ddNorm["ContextoNorm"] = ""
			for irow in range(ddNorm.shape[0]):
				aux = preProcesoTexto(ddNorm["Contexto"][irow], enc = "latin1")
				aux = parsetree(aux, lemmata = True)
				auxl = []
				for item in aux:
					auxl.extend(item.lemma)
				aux = [item for item in auxl if len(item) > 2 and len(re.findall("[A-z]", item)) > 0]
				ddNorm["ContextoNorm"][irow] = reduce_concat(aux, sep = " ")
				ddNorm["ContextoNorm"][irow] = remove_accents(ddNorm["ContextoNorm"][irow])
			
			# Vemos la frecuencia de todas las palabras del texto
			aux = preProcesoTexto(readedFile)
			aux = parsetree(aux, lemmata = True)
			auxl = []
			for item in aux:
				auxl.extend(item.lemma)
			auxreadedFile = [item for item in auxl if len(item) > 2 and len(re.findall("[A-z]", item)) > 0]
			auxtt = table(auxreadedFile)
			auxMean = [auxtt[ikey] for ikey in auxtt.keys()]
			valMax = np.mean(auxMean) + np.var(auxMean)**(float(1)/2)
			
			# Vemos la frecuencia de las palabras en el texto por cada contexto y decidimos los tags
			ddNorm["tags"] = ''
			for irow in range(ddNorm.shape[0]):
				paraules = ddNorm["ContextoNorm"][irow].split(" ")
				paraules = list(set(paraules))
				paraules_tags = [paraula for paraula in paraules if paraula in auxtt.keys() if auxtt[paraula] <= valMax]
				if len(paraules_tags) > 0:
					ddNorm["tags"][irow] = reduce_concat(paraules_tags, sep = "|")
				else:
					ddNorm["tags"][irow] = reduce_concat(paraules, sep = "|")
				
			# Sacamos los tags para los contextos unicos
			for ikey in result.keys():
				aux = preProcesoTexto(result[ikey]['descripcion'], enc = "latin1")
				aux = parsetree(aux, lemmata = True)
				auxl = []
				for item in aux:
					auxl.extend(item.lemma)
				aux = [item for item in auxl if len(item) > 2 and len(re.findall("[A-z]", item)) > 0]
				result[ikey]['tags'] = reduce_concat(aux, sep = ", ")
				
			for ikey in result.keys():
				result[ikey]["tags"] = remove_accents(result[ikey]["tags"])
				

			# ============================= #
			# === Escribimos resultados === #
			# ============================= #
			
			# Guardamos outputs y los dejamos en ES
			if guardaResultados:
				if es_hdfs:

					fileSave = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_info_pos"
					guardaDFHDFS(ddNorm, fileSave, sc)
					
					fileentities = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_entities"
					guardarDictHDFS(result, fileentities, sc)
					
					
				else:
					ddNorm.to_csv(path_or_buf = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_info_pos.csv", sep = "|")
					fileOut = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_entities.json"
					with open(fileOut, 'w') as fp:
						json.dump(result, fp)
			
			if muestraHTML and not es_hdfs:
				dataJSON = deepcopy(result)
				name_file = filename.lower().replace(".pdf", "")
				
				ddNorm['order'] = [sum(ddNorm['Referencia_Normalizada'] == ddNorm['Referencia_Normalizada'][i]) for i in xrange(len(ddNorm))]
				ddNorm = ddNorm.sort_values("order", ascending = False)
				regUnics = pd.unique(ddNorm["Referencia_Normalizada"])
				ddNorm["id_reg"] = ""
				compta = 0
				for registre in regUnics:
					sel = ddNorm["Referencia_Normalizada"] == registre
					ddNorm["id_reg"][sel] = compta
					compta += 1
				
				# Definimos las marcas
				ddNorm['mark'] = 'mark09'
				for i in range(1, 9):
					sel = ddNorm["id_reg"] == (i - 1)
					ddNorm["mark"][sel] = 'mark0' + str(i)
				
				# Escribimos html's con descripciones y definimos href
				ddNorm['href'] = ''
				for ikey in dataJSON.keys():
					hrefFile = "href " + name_file + " " + ikey.replace('"', '').replace("'", "") + ".html"
					textohref = "Fecha: " + str(ikey) + "<br>Contexto: " + dataJSON[ikey]["descripcion"].encode("latin1") + "<br>Apariciones: " + str(len(dataJSON[ikey]["referencias"])) + "<br>Tags: " + str(result[ikey]['tags'])
					filehtml = open(OUTPUTDIR + hrefFile, mode = "w+")
					filehtml.write(textohref)
					filehtml.close()
					# sel = [x in dataJSON[ikey]["referencias"] for x in ddNorm["Ref_Orig"]]
					sel = ddNorm['Referencia_Normalizada'] == ikey
					ddNorm['href'][sel] = hrefFile
				
				# Escribimos html
				textohtml = "<head> <link rel = 'stylesheet' type = 'text/css' href = 'styles_css.css'> </head>"
				i_suma = 0
				for i in range(ddNorm.shape[0]):
					before = "<a href = '" + ddNorm["href"][i] + "'>" + "<" + ddNorm["mark"][i] + ">"
					after = "</" + ddNorm["mark"][i] + "></a>"
					readedFile = readedFile[:(int(ddNorm["PosInicio"][i]) + i_suma)] + before + readedFile[(int(ddNorm["PosInicio"][i]) + i_suma):(int(ddNorm["PosFin"][i]) + i_suma)] + after + readedFile[(int(ddNorm["PosFin"][i]) + i_suma):]
					i_suma += len(before) + len(after)
				textohtml += "<p>" + readedFile + "</p>"
				
				filehtml = OUTPUTDIR + name_file + "_muestra_fechas.html"
				filehtml = open(filehtml, mode = "w+")
				filehtml.write(textohtml)
				filehtml.close()
				
			if guardaES:
				pass

		print "Fechas finalizado."

		# ===================================== #
		# === 049_Implementacion_cantidades === #
		# ===================================== #

		print "Procesando cantidades..."

		# Current directories
		OUTPUTDIR = PATH + "output/resultados_cant/"

		# Diccionarios
		DictPaisCompleteStrSearch = dict(
			es = dict(
				esp = [
					dict(contiene = ['\\beur\\b', '\\beuro\\b', '\\beuros\\b', '€\\b', '\\bpts\\b', '\\bpesetas\\b', '\\beur\.\\b', '[0-9]eur\\b', '[0-9]eur\.\\b'], 
							 inicios = ['[^0-9]\\b[0-9\,\.\' ]+'],
							 finales = ['eur', 'euros', '€', 'pts', 'pesetas']
					)
				]
			),
			pt = dict(
				bra = [
					dict(contiene = ['\\breais\\b', '\\breai\\b', 'R$\\b', '[0-9]reai\\b'], 
							 inicios = ['[^0-9]\\b[0-9\,\.\' ]+'],
							 finales = ['reai', 'reais', 'R$']
					)
				]
			)
		)

		CompleteStrSearch = DictPaisCompleteStrSearch[idioma][pais]

		DictPaisSearchMaxEnrere = dict(
			es = dict(
				esp = [
					dict(contiene = ['\\beur\\b', '\\beuro\\b', '\\beuros\\b', '€\\b', '\\bpts\\b', '\\bpesetas\\b', '\\beur\.\\b', '[0-9]eur\\b', '[0-9]eur\.\\b'], 
							 inicios = ["\\buno", "\\bdos", "\\btres", "\\bcuatro", "\\bcinco", "\\bseis", "\\bsiete", "\\bocho", "\\bnueve", "\\bdiez", "\\bonce", "\\bdoce", "\\btrece", "\\bcatorce", "\\bquince", "\\bdieciseis", "\\bdiecisiete", "\\bdieciocho", "\\bdiecinueve", "\\bveinte", "\\bventi", "\\bveinti", "\\btreinta", "\\btrenta", "\\bcuarenta", "\\bcincuenta", "\\bsesenta", "\\bochenta", "\\bnoventa", "\\bcien", "\\bdoscientos", "\\btrescientos", "\\bcuatrocientos", "\\bquinientos", "\\bseiscientos", "\\bsetecientos", "\\bochocientos"],
							 finales = ['eur', 'euros', '€', 'pts', 'pesetas']
					)
				]
			),
			pt = dict(
				bra = [
					dict(contiene = ['\\breai\\b', '\\breais\\b', 'R$\\b', '[0-9]reai\\b'], 
							 inicios = ["\\bum", "\\bdois", "\\btres", "\\bquatro", "\\bcinco", "\\bseis", "\\bsete", "\\boito", "\\bnove", "\\bdez", "\\bonze", "\\bdoze", "\\btreze", "\\bcatorze", "\\bquinze", "\\bdezesseis", "\\bdezessete", "\\bdezoito", "\\bdezenove", "\\bvinte", "\\btrinta"	, "\\bquarenta", "\\bcinquenta", "\\bsessenta", "\\bsetenta", "\\boitenta", "\\bnoventa", "\\bcem", "\\bduzentos", "\\btrezentos", "\\bquatrocentos", "\\bquinhentos", "\\bseiscentos", "\\bsetecentos", "\\boitocentos", "\\bnovecentos"],
							 finales = ['reai', 'reais', 'R$']
					)
				]
			)
		)

		SearchMaxEnrere = DictPaisSearchMaxEnrere[idioma][pais]

		limSimFrases = 0.05	# Limit a partir del qual diem que 2 frases s'assemblen

		guardaES = guardaES and not eslocal	 # Solo guardamos a elastic si no se ejecuta desde local

		if eslocal:
			RawReadedFile = pdf2txt(INPUTREAD + filename)
			RawReadedFile = RawReadedFile.replace("\xe2\x82\xac", "euro")	# OJO! Tenir en compte aixo abans de tornar-lis les dades!!
		else:
			RawReadedFile = import_doc(filename, criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)
		readedFile = remove_accents(RawReadedFile)
		readedFile = readedFile.lower()
		
		
		# ================== #
		# === Script 040 === #
		# ================== #
		
		# Buscamos posicion strings
		index_points = []
		for dictstr2search in CompleteStrSearch:
			idxAct = []
			for str2search in dictstr2search['contiene']:
				idxAct.extend(buscaPosicionRegexTexto(str2search, readedFile))
				
			index_points.append(idxAct)
		
		# Buscamos inicios y finales de las posiciones encontradas
		cantidades = []
		for i in range(len(CompleteStrSearch)):
			dictAct = CompleteStrSearch[i]
			for item in index_points[i]:
				# Preparamos textos de inicios y finales
				aux_texto_ini = readedFile[:item[1]]
				aux_texto_fin = readedFile[item[0]:]
				# Buscamos inicios y fines
				listInicios = []
				listFinal = []
				for i_inicio in dictAct['inicios']:
					listInicios.extend(buscaPosicionRegexTexto(i_inicio, aux_texto_ini))
				for i_final in dictAct['finales']:
					listFinal.extend(buscaPosicionRegexTexto(i_final, aux_texto_fin))
				if len(listInicios) == 0 or len(listFinal) == 0:
					print "No tenemos cantidades en este documento"
					continue
				listInicios = array(listInicios)
				listFinal = array(listFinal)
				cantidades.append((max(listInicios[:, 0]), item[0] + min(listFinal[:, 1])))
				if (cantidades[len(cantidades) - 1][1] - cantidades[len(cantidades) - 1][0]) > 40:
					lletresFinsInici = len(aux_texto_ini) - cantidades[len(cantidades) - 1][0]
					lletresFinsFinal = min(listFinal[:, 1])
					if lletresFinsInici > lletresFinsFinal:
						cantidades[len(cantidades) - 1] = (item[0], cantidades[len(cantidades) - 1][1])
					else:
						cantidades[len(cantidades) - 1] = (cantidades[len(cantidades) - 1][0], item[1])
		
		
		# Buscamos posiciones teniendo en cuenta lo maximo para atras
		index_points_max_enrere = []
		for dictstr2search in SearchMaxEnrere:
			idxAct = []
			for str2search in dictstr2search['contiene']:
				idxAct.extend(buscaPosicionRegexTexto(str2search, readedFile))
		
			index_points_max_enrere.append(idxAct)

		for i in range(len(SearchMaxEnrere)):
			dictAct = SearchMaxEnrere[i]
			for item in index_points_max_enrere[i]:
				# Preparamos textos de inicios y finales
				aux_texto_ini = readedFile[:item[1]]
				aux_texto_fin = readedFile[item[0]:]
				# Buscamos inicios y fines
				listInicios = []
				listFinal = []
				for i_inicio in dictAct['inicios']:
					listInicios.extend(buscaPosicionRegexTexto(i_inicio, aux_texto_ini))
				for i_final in dictAct['finales']:
					listFinal.extend(buscaPosicionRegexTexto(i_final, aux_texto_fin))
				if len(listInicios) == 0 or len(listFinal) == 0:
					continue
				listInicios = array(listInicios)
				listFinal = array(listFinal)
				sel = abs(listInicios[:, 0] - len(aux_texto_ini)) < 70
				if any(sel):
					cantidades.append((min(listInicios[sel, 0]), item[0] + min(listFinal[:, 1])))
		
		# Buscamos registros unicos
		cantidades = list(set(cantidades))
		
		
		if len(cantidades) == 0:
			print "No tenemos cantidades en este documento"
		else:
				
			# Regulamos que no se incluyan unos dentro de otros los textos
			cantidades = pd.DataFrame(cantidades)
			cantidades = cantidades.sort_values(0)
			cantidades = cantidades.values.tolist()
			jresta = 0
			for i in range(1, len(cantidades)):
				if cantidades[i - jresta][0] < cantidades[i - 1 - jresta][1]:
					cantidades[i - jresta] = [cantidades[i - 1 - jresta][0], cantidades[i - jresta][1]]
					cantidades.pop(i - 1 - jresta)
					jresta += 1
			
			# ================== #
			# === Script 041 === #
			# ================== #

			# === #
			# Canvi respecte script 041!!
			listRefs = deepcopy(cantidades)
			listRefs = pd.DataFrame(listRefs)
			listRefs.columns = ["PosInicio", "PosFin"]
			listRefs["Referencia"] = ""
			for i in range(listRefs.shape[0]):
				listRefs["Referencia"][i] = readedFile[listRefs["PosInicio"][i]:(listRefs["PosFin"][i] + 2)]
			# === #

			listRefs["Ref_Orig"] = listRefs["Referencia"]
			
			# Hacemos modificaciones en las referencias
			caracteres_elimina = [":", "(", ")", ";"]
			caracteres_espacio = ["\n", "/", "-"]
			for ce in caracteres_elimina:
				listRefs["Referencia"] = listRefs["Referencia"].str.replace(ce, "")
			for ce in caracteres_espacio:
				listRefs["Referencia"] = listRefs["Referencia"].str.replace(ce, " ")
			
			for i in range(listRefs.shape[0]):
				listRefs["Referencia"][i] = quitaDoblesEspacios(listRefs["Referencia"][i])
				
			listRefs["Referencia"] = listRefs["Referencia"].str.strip()

			# Limpiamos las cantidades
			for i in range(listRefs.shape[0]):
			
				aux_text = listRefs["Referencia"][i]
				aux_text = aux_text.split(" ")
				
				# Quitamos letras que puedan aparecer al principio
				suma_principi = 0
				ipar = 0
				acabat = False
				while ipar < len(aux_text) and not acabat:
					if len(re.findall("[0-9]", aux_text[ipar])) == 0:
						suma_principi += len(aux_text[ipar])
					else:
						acabat = True
					ipar += 1
					
				# Tambien texto que puede sobrar al final
				suma_final = 0
				ipar = len(aux_text) - 1
				acabat = False
				while ipar > 0 and not acabat:
					if not 'eur' in aux_text[ipar]:
						suma_final += len(aux_text[ipar])
					else:
						acabat = True
					ipar -= 1
				
				listRefs["Referencia"][i] = listRefs["Referencia"][i][suma_principi:(len(listRefs["Referencia"][i]) - suma_final)]
				listRefs["PosInicio"][i] += suma_principi
				listRefs["PosFin"][i] += suma_final + 2
			
			
			# Normalizamos los campos
			listRefs["Referencia_Normalizada"] = ''
			for i in range(listRefs.shape[0]):
				normaliza = listRefs["Referencia"][i]
				aux_norm = normaliza.split(" eur")
				
				if len(aux_norm) > 2:
					aux_norm = reduce_concat(aux_norm, sep = " ")
				else:
					aux_norm = aux_norm[0]
				
				if len(re.findall("[0-9]", aux_norm)) == 0:
					aux_norm = remove_accents(aux_norm)
					result = transformaGrandesNumsString(aux_norm)
				else:
					# Treiem caracters que no interessen
					aux_norm = remove_accents(aux_norm)
					aux_norm = re.sub("[A-z]", "", aux_norm)
					aux_norm = aux_norm.replace(" ", "")
					
					# Normalitzem decimals i milers
					aux_norm = aux_norm.replace(",", ".")
					aux_norm = aux_norm.replace("'", ".")
					
					aux_norm = aux_norm.split(".")
					if len(aux_norm) == 1:
						result = aux_norm[0]
						result = result + ".00"
					elif len(aux_norm[len(aux_norm) - 1]) >= 3:
						result = reduce_concat(aux_norm, sep = "")
						result = result + ".00"
					else:
						result = reduce_concat(aux_norm[:-1], sep = "")
						result = result + "." + aux_norm[-1]
				
				try:
					result = float(result)
				except:
					result = None
				
				listRefs["Referencia_Normalizada"][i] = result
				
			
			# Comprobamos si las cantidades son muy similares
			for i in range(listRefs.shape[0]):
				for j in range(listRefs.shape[0]):
					if listRefs["Referencia_Normalizada"][i] != listRefs["Referencia_Normalizada"][j] and not (listRefs["Referencia_Normalizada"][i] is None or listRefs["Referencia_Normalizada"][j] is None):
						maxnum = listRefs["Referencia_Normalizada"][i]*1.0001
						minnum = listRefs["Referencia_Normalizada"][i]*0.9999
						if listRefs["Referencia_Normalizada"][j] <= maxnum and listRefs["Referencia_Normalizada"][j] >= minnum:
							mostfreq = listRefs["Referencia_Normalizada"][i]
							if sum(listRefs["Referencia_Normalizada"] == listRefs["Referencia_Normalizada"][i]) < sum(listRefs["Referencia_Normalizada"] == listRefs["Referencia_Normalizada"][j]):
								mostfreq = listRefs["Referencia_Normalizada"][j]
							
							listRefs["Referencia_Normalizada"][i] = mostfreq
							listRefs["Referencia_Normalizada"][j] = mostfreq
			
			
			# ================== #
			# === Script 042 === #
			# ================== #
			
			# Montamos un string con la misma estructura y con acentos
			try:
				RawReadedFileAcc = str(RawReadedFile.decode("utf8").encode("latin1", 'ignore'))
			except:
				RawReadedFileAcc = str(RawReadedFile.encode("latin1", "ignore"))
				
			i = 0
			while i < len(RawReadedFileAcc):
				letterAcc = RawReadedFileAcc[i]
				letterra = readedFile[i]
				if remove_accents(letterAcc, enc = "latin1").lower() != letterra:
					if letterAcc == readedFile[i + 1]:
						readedFile = readedFile[:i] + readedFile[(i + 1):]
					else:
						RawReadedFileAcc = RawReadedFileAcc[:i] + RawReadedFileAcc[(i + 1):]
				else:
					i += 1

			if len(RawReadedFileAcc) != len(readedFile):
				raise Exception("No pot ser!!")
			
			# Leemos archivo posiciones fechas normalizadas
			ddNorm = deepcopy(listRefs)
			
			# Cogemos los textos que hay antes y despues de cada fecha
			ddNorm["Antes"] = ""
			ddNorm["Despues"] = ""
			x = 1000
			for irow in range(ddNorm.shape[0]):
				ddNorm["Antes"][irow] = RawReadedFileAcc[max(ddNorm["PosInicio"][irow] - x, 0):ddNorm["PosInicio"][irow]]
				aux_text = RawReadedFileAcc[(ddNorm["PosFin"][irow] - 1):min(ddNorm["PosFin"][irow] + x, (len(readedFile) - 1))]
				if len(aux_text) > 0:
					if aux_text[0] == ddNorm["Referencia"][irow][-1]:
						ddNorm["Despues"][irow] = aux_text[1:]
					else:
						ddNorm["Despues"][irow] = aux_text
				else:
					ddNorm["Despues"][irow] = ""
				
			# Pasamos a frases los textos de antes y despues
			minText = 15
			ddNorm["Contexto"] = ""
			for irow in range(ddNorm.shape[0]):
				ddNorm["Antes"][irow] = ddNorm["Antes"][irow].replace("\n", " ")
				ddNorm["Despues"][irow] = ddNorm["Despues"][irow].replace("\n", " ")
				auxa = sentences(ddNorm["Antes"][irow])
				auxa = auxa.split(" .\n")
				textant = auxa[len(auxa) - 1]
				if len(textant) < minText:
					textant = reduce_concat(auxa[(len(auxa) - 2):(len(auxa) - 1)], sep = ". ")
				ddNorm["Antes"][irow] = textant
				
				auxd = sentences(ddNorm["Despues"][irow])
				auxd = auxd.split(" .\n")
				textdsp = auxd[0]
				if len(textdsp) < minText:
					textdsp = reduce_concat(auxd[:2], sep = ". ")
				ddNorm["Despues"][irow] = textdsp
				
				ddNorm["Contexto"][irow] = ddNorm["Antes"][irow] + ddNorm["Referencia"][irow] + ddNorm["Despues"][irow]
			
			# Vemos que relevancia tiene cada elemento
			aux_tt = ddNorm["Referencia_Normalizada"].value_counts()
			aux_tt2 = aux_tt/max(aux_tt)
			
			aux_tt = pd.DataFrame(data = dict(Referencia_Normalizada = list(aux_tt.index), Apariciones = list(aux_tt.values), Relevancia = list(aux_tt2.values)))
			ddNorm = ddNorm.merge(aux_tt, 'left')
			

			# ============================= #
			# === Escribimos resultados === #
			# ============================= #
			
			# Guardamos outputs y los dejamos en ES
			if guardaResultados:
				if es_hdfs:

					fileSave = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_info_pos"
					guardaDFHDFS(ddNorm, fileSave, sc)
					
				else:
					ddNorm.to_csv(path_or_buf = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_info_pos.csv", sep = "|")
							
			
			if muestraHTML and not es_hdfs:
				# Normalizamos las sentencias
				result = dict()
				for val in ddNorm["Referencia_Normalizada"].unique():
					if pd.isnull(val):
						continue
						
					if int(aux_tt["Apariciones"][aux_tt["Referencia_Normalizada"] == val]) == 1:
						# Aqui posem les frases que apareixen a pinyo
						sel = ddNorm["Referencia_Normalizada"] == val
						aux_data = ddNorm[sel]
						aux_data = aux_data.reset_index()
						texto = str(aux_data["Antes"][0]) + str(aux_data["Ref_Orig"][0]) + str(aux_data["Despues"][0])
						val = str(val)
						val = val.replace(".", "_")
						result[val] = dict(descripcion = texto.strip())
						result[val]["posiciones"] = dict(inicio = list(aux_data["PosInicio"]), fin = list(aux_data["PosFin"])) 
						result[val]["referencias"] = list(aux_data["Ref_Orig"])
					else:
						# Aqui fem l'analisis per mes d'una frase, si son iguals intentem fer una frase descriptiva, sino doncs per cada item 
						# posem el que s'assembli mes (PROVAR AMB GENSIM!!)
						frases = []
						aux_data = ddNorm[ddNorm["Referencia_Normalizada"] == val]
						aux_data = aux_data.reset_index()
						for irow in range(aux_data.shape[0]):
							texto = aux_data["Antes"][irow] + aux_data["Ref_Orig"][irow] + aux_data["Despues"][irow]
							frases.append(texto)

						# Comparem frases i veiem quines s'assemblen a quines
						auxRes = comparaFrases(frases)
						for i in range(len(frases)):
							auxRes = auxRes.append(pd.DataFrame(dict(item1 = [i], item2 = [i], valor_comp = [1])))
						gruposUnidos = unirGrupos(list(auxRes["item1"]), list(auxRes["item2"]), list(auxRes["valor_comp"] > limSimFrases))
						
						# Extraiem les frases mes rellevants per a cada grup
						frasesGrup = []
						for grupo in gruposUnidos:
							frases_grupo_act = [""]
							for element in grupo:
								frases_grupo_act[0] = frases_grupo_act[0] + " " + frases[element]
								frases_grupo_act.append(frases[element])

							auxRes = comparaFrases(frases_grupo_act, totesContraTotes = False)
							auxResSel = auxRes[(auxRes["item1"] == 0) | (auxRes["item2"] == 0)]
							auxResSel = auxResSel[auxResSel["valor_comp"] == max(auxResSel["valor_comp"])].reset_index()
							auxResSel = auxResSel.loc[0, ["item1", "item2"]]
							i = int(auxResSel[auxResSel != 0]) - 1
							frasesGrup.append(frases[grupo[i]])
						
						val = str(val)
						val = val.replace(".", "_")
						if len(frasesGrup) == 1:
							result[val] = dict(descripcion = frasesGrup[0])
							result[val]["posiciones"] = dict(inicio = list(aux_data["PosInicio"]), fin = list(aux_data["PosFin"])) 
							result[val]["referencias"] = list(aux_data["Ref_Orig"])
						else:
							for igrupo in range(len(gruposUnidos)):
								result[val + "_" + str(igrupo + 1)] = dict(descripcion = frasesGrup[igrupo])
								result[val + "_" + str(igrupo + 1)]["posiciones"] = dict(inicio = list(aux_data.loc[(gruposUnidos[igrupo])]["PosInicio"]), fin = list(aux_data.loc[(gruposUnidos[igrupo])]["PosFin"]))
								result[val + "_" + str(igrupo + 1)]["referencias"] = list(aux_data.loc[(gruposUnidos[igrupo])]["Ref_Orig"])
								for ielement in range(len(gruposUnidos[igrupo])):
									ddNorm["Referencia_Normalizada"][aux_data["index"][gruposUnidos[igrupo][ielement]]] = val + "_" + str(igrupo + 1)


				# Limpiamos de caracteres raros
				for ikey in result.keys():
					result[ikey]["descripcion"] = result[ikey]["descripcion"].decode("latin1")
					for i in range(len(result[ikey]["referencias"])):
						result[ikey]["referencias"][i] = result[ikey]["referencias"][i].decode("latin1")
						result[ikey]["posiciones"]["inicio"][i] = str(result[ikey]["posiciones"]["inicio"][i])
						result[ikey]["posiciones"]["fin"][i] = str(result[ikey]["posiciones"]["fin"][i])
				
				dataJSON = deepcopy(result)
				name_file = filename.lower().replace(".pdf", "")
				
				ddNorm['order'] = [sum(ddNorm['Referencia_Normalizada'] == ddNorm['Referencia_Normalizada'][i]) for i in xrange(len(ddNorm))]
				ddNorm = ddNorm.sort_values("order", ascending = False)
				regUnics = pd.unique(ddNorm["Referencia_Normalizada"])
				ddNorm["id_reg"] = ""
				compta = 0
				for registre in regUnics:
					sel = ddNorm["Referencia_Normalizada"] == registre
					ddNorm["id_reg"][sel] = compta
					compta += 1
				
				# Definimos las marcas
				ddNorm['mark'] = 'mark09'
				for i in range(1, 9):
					sel = ddNorm["id_reg"] == (i - 1)
					ddNorm["mark"][sel] = 'mark0' + str(i)
				
				# Escribimos html's con descripciones y definimos href
				ddNorm['href'] = ''
				for ikey in dataJSON.keys():
					hrefFile = "href " + name_file + " " + ikey.replace('"', '').replace("'", "") + ".html"
					textohref = "Fecha: " + str(ikey) + "<br>Contexto: " + dataJSON[ikey]["descripcion"].encode("latin1") + "<br>Apariciones: " + str(len(dataJSON[ikey]["referencias"]))
					filehtml = open(OUTPUTDIR + hrefFile, mode = "w+")
					filehtml.write(textohref)
					filehtml.close()
					# sel = [x in dataJSON[ikey]["referencias"] for x in ddNorm["Ref_Orig"]]
					sel = ddNorm['Referencia_Normalizada'] == ikey
					ddNorm['href'][sel] = hrefFile
				
				# Escribimos html
				textohtml = "<head> <link rel = 'stylesheet' type = 'text/css' href = 'styles_css.css'> </head>"
				i_suma = 0
				for i in range(ddNorm.shape[0]):
					before = "<a href = '" + ddNorm["href"][i] + "'>" + "<" + ddNorm["mark"][i] + ">"
					after = "</" + ddNorm["mark"][i] + "></a>"
					readedFile = readedFile[:(int(ddNorm["PosInicio"][i]) + i_suma)] + before + readedFile[(int(ddNorm["PosInicio"][i]) + i_suma):(int(ddNorm["PosFin"][i]) + i_suma)] + after + readedFile[(int(ddNorm["PosFin"][i]) + i_suma):]
					i_suma += len(before) + len(after)
				textohtml += "<p>" + readedFile + "</p>"
				
				filehtml = OUTPUTDIR + name_file + "_muestra_cantidades.html"
				filehtml = open(filehtml, mode = "w+")
				filehtml.write(textohtml)
				filehtml.close()
				
			if guardaES:
			
				# Sacamos las posiciones de cada pagina
				perPageDoc = read_file_pagebypage(filename, criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)
				count_len = 0
				listpos = []
				for item in perPageDoc:
					listpos.append((count_len, count_len + len(item) - 1))
					count_len += len(item)
				
				rawDoc = reduce_concat(perPageDoc)
				
				# Subimos a ES
				
				# Buscamos el texto en el rawDoc
				ddNorm["posRawDocIni"] = -1
				ddNorm["posRawDocFin"] = -1
				for i in range(ddNorm.shape[0]):
					texto = ddNorm["Referencia"][i]
					text2search = rawDoc[ddNorm["PosInicio"][i]:]
					text2search = remove_accents(text2search)
					text2search = text2search.lower()
					if texto in text2search:
						sumposini = text2search.index(texto)
					else:
						caracteres_elimina = [":", ".", "(", ")", ",", ";"]
						caracteres_espacio = ["\n", "/", "-"]
						for cel in caracteres_elimina:
							text2search = text2search.replace(cel, "")
							texto = texto.replace(cel, "")
						for ces in caracteres_espacio:
							text2search = text2search.replace(ces, " ")
							texto = texto.replace(ces, " ")
							
						text2search = quitaDoblesEspacios(text2search)
						sumposini = text2search.index(texto)
						
					ddNorm["posRawDocIni"][i] = ddNorm["PosInicio"][i] + sumposini
					ddNorm["posRawDocFin"][i] = ddNorm["posRawDocIni"][i] + len(texto)
					
				# Vemos en que pagina y que posicion ocupa cada entidad
				ddNorm["pagina"] = '-1'
				for i in range(ddNorm.shape[0]):
					posBusca = ddNorm.loc[[i], ["posRawDocIni", "posRawDocFin"]]
					posBusca = posBusca.values.tolist()[0]
					encontrada = False
					ipag = 0
					while not encontrada and ipag < len(listpos):
						ipag += 1
						if listpos[ipag - 1][0] <= posBusca[0] and listpos[ipag - 1][1] >= posBusca[1]:
							ddNorm["pagina"][i] = str(ipag)
							encontrada = True
						elif len(listpos) > ipag:
							if listpos[ipag - 1][1] >= posBusca[0] and listpos[ipag][0] <= posBusca[1]:
								ddNorm["pagina"][i] = str(ipag - 1) + "," + str(ipag)
								encontrada = True
				
				ddNorm[ddNorm["pagina"] == -1]["pagina"] = len(listpos)
				ddNorm = posicionporpagina(ddNorm, listpos)
				
				# Subimos diccionarios a ES
				for i in range(ddNorm.shape[0]):
					
					# Creamos diccionario
					dictloadES = {
						"Doc_cantidad_monetaria_" + filename.lower().replace(".pdf", "") + "_" + str(i): dict(
							tipo = "cantidad_monetaria",
							documento = filename,
							pagina = ddNorm["pagina"][i],
							pos_inicial = int(ddNorm["posRawDocIni"][i]),
							pos_final = int(ddNorm["posRawDocFin"][i]),
							texto = ddNorm["Referencia"][i],
							texto_norm = ddNorm["Referencia_Normalizada"][i],
							contexto = ddNorm["Contexto"][i]
						)
					}

					load2Elastic(dictloadES, INDEX_NAME = indicewrite, TYPE_NAME =	"doc", newesconn = Elasticsearch([ipwrite]))
		

		print "Cantidades finalizado."


		# ================================= #
		# === 059_Implementacion_plazos === #
		# ================================= #

		print "Procesando plazos..."

		# Current directories
		OUTPUTDIR = PATH + "output/resultados_plazos/"

		# Diccionarios
		DictPaisSearchMaxEnrere = dict(
			es = dict(
				esp = [ 
					dict(contiene = ['\\bmes\\b', '\\bmeses\\b', '\\bdia\\b', '\\bdias\\b', '\\bano\\b', '\\banos\\b', '\\bsemana'], 
							 # inicios = ['[0-9]+', "\\buno", "\\bdos", "\\btres", "\\bcuatro", "\\bcinco", "\\bseis", "\\bsiete", "\\bocho", "\\bnueve", "\\bdiez", "\\bonce", "\\bdoce", "\\btrece", "\\bcatorce", "\\bquince", "\\bdieciseis", "\\bdiecisiete", "\\bdieciocho", "\\bdiecinueve", "\\bveinte", "\\bventi", "\\bveinti", "\\btreinta", "\\btrenta", "\\bcuarenta", "\\bcincuenta", "\\bsesenta", "\\bochenta", "\\bnoventa", "\\bcien", "\\bdoscientos", "\\btrescientos", "\\bcuatrocientos", "\\bquinientos", "\\bseiscientos", "\\bsetecientos", "\\bochocientos"],
							 inicios = ['pena', "plazo", "condena", "multa", "beneficio de la comunidad", "beneficio a la comunidad", "sancion"],
							 finales = ['\\bmes\\b', '\\bmeses\\b', '\\bdia\\b', '\\bdias\\b', '\\bano\\b', '\\banos\\b', '\\bsemana']
					),
					dict(contiene = ['\\bmes\\b', '\\bmeses\\b', '\\bdia\\b', '\\bdias\\b', '\\bano\\b', '\\banos\\b', '\\bsemana'], 
							 inicios = ["\\buno", "\\bdos", "\\btres", "\\bcuatro", "\\bcinco", "\\bseis", "\\bsiete", "\\bocho", "\\bnueve", "\\bdiez", "\\bonce", "\\bdoce", "\\btrece", "\\bcatorce", "\\bquince", "\\bdieciseis", "\\bdiecisiete", "\\bdieciocho", "\\bdiecinueve", "\\bveinte", "\\bventi", "\\bveinti", "\\btreinta", "\\btrenta", "\\bcuarenta", "\\bcincuenta", "\\bsesenta", "\\bochenta", "\\bnoventa", "\\bcien", "\\bdoscientos", "\\btrescientos", "\\bcuatrocientos", "\\bquinientos", "\\bseiscientos", "\\bsetecientos", "\\bochocientos"],
							 finales = ['\\bmes\\b', '\\bmeses\\b', '\\bdia\\b', '\\bdias\\b', '\\bano\\b', '\\banos\\b', '\\bsemana']
					)
				]
			),
			pt = dict(
				bra = [ 
					dict(contiene = ['\\bmes\\b', '\\bmeses\\b', '\\bdia\\b', '\\bdias\\b', '\\bano\\b', '\\banos\\b', '\\bsemana'], 
							 inicios = ['pena', "prazo", "conviccao", "fino", "sancao", "sancoes"],
							 finales = ['\\bmes\\b', '\\bmeses\\b', '\\bdia\\b', '\\bdias\\b', '\\bano\\b', '\\banos\\b', '\\bsemana']
					)
				]
			)
		)

		SearchMaxEnrere = DictPaisSearchMaxEnrere[idioma][pais]
			
		limSimFrases = 0.15	# Limit a partir del qual diem que 2 frases s'assemblen

		guardaES = guardaES and not eslocal	 # Solo guardamos a elastic si no se ejecuta desde local

		if eslocal:
			RawReadedFile = pdf2txt(INPUTREAD + filename)
		else:
			RawReadedFile = import_doc(filename, criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)
		readedFile = remove_accents(RawReadedFile)
		readedFile = readedFile.lower()
		
		# ================== #
		# === Script 050 === #
		# ================== #
		
		# Buscamos posiciones teniendo en cuenta lo maximo para atras
		index_points_max_enrere = []
		for dictstr2search in SearchMaxEnrere:
			idxAct = []
			for str2search in dictstr2search['contiene']:
				idxAct.extend(buscaPosicionRegexTexto(str2search, readedFile))
		
			index_points_max_enrere.append(idxAct)
		
		plazos = []
		for i in range(len(SearchMaxEnrere)):
			dictAct = SearchMaxEnrere[i]
			for item in index_points_max_enrere[i]:
				# Preparamos textos de inicios y finales
				aux_texto_ini = readedFile[:item[1]]
				aux_texto_fin = readedFile[item[0]:]
				# Buscamos inicios y fines
				listInicios = []
				listFinal = []
				for i_inicio in dictAct['inicios']:
					listInicios.extend(buscaPosicionRegexTexto(i_inicio, aux_texto_ini))
				for i_final in dictAct['finales']:
					listFinal.extend(buscaPosicionRegexTexto(i_final, aux_texto_fin))
				if len(listInicios) == 0 or len(listFinal) == 0:
					continue
				listInicios = np.array(listInicios)
				listFinal = np.array(listFinal)
				sel = abs(listInicios[:, 0] - len(aux_texto_ini)) < 50
				if any(sel):
					selFin = listFinal[:, 1] < 30
					if any(selFin):
						plazos.append((min(listInicios[sel, 0]), item[0] + max(listFinal[selFin, 1])))
					else:
						plazos.append((min(listInicios[sel, 0]), item[0] + min(listFinal[:, 1])))
					
			
		# Buscamos registros unicos
		plazos = list(set(plazos))
		
		if len(plazos) == 0:
			print "No hemos encontrado plazos en este documento"
		else:
				
			# Regulamos que no se incluyan unos dentro de otros los textos
			plazos = pd.DataFrame(plazos)
			plazos = plazos.sort_values(0)
			plazos = plazos.values.tolist()
			jresta = 0
			for i in range(1, len(plazos)):
				if plazos[i - jresta][0] < plazos[i - 1 - jresta][1]:
					plazos[i - jresta] = [plazos[i - 1 - jresta][0], plazos[i - jresta][1]]
					plazos.pop(i - 1 - jresta)
					jresta += 1
			
			# ================== #
			# === Script 051 === #
			# ================== #

			# === #
			# Canvi respecte script 051!!
			listRefs = deepcopy(plazos)
			listRefs = pd.DataFrame(listRefs)
			listRefs.columns = ["PosInicio", "PosFin"]
			listRefs["Referencia"] = ""
			for i in range(listRefs.shape[0]):
				listRefs["Referencia"][i] = readedFile[listRefs["PosInicio"][i]:listRefs["PosFin"][i]]
			# === #
			
			listRefs["Ref_Orig"] = listRefs["Referencia"]
			
			# Hacemos modificaciones en las referencias
			caracteres_elimina = [":", ".", "(", ")", ",", ";"]
			caracteres_espacio = ["\n", "/", "-"]
			for ce in caracteres_elimina:
				listRefs["Referencia"] = listRefs["Referencia"].str.replace(ce, "")
			for ce in caracteres_espacio:
				listRefs["Referencia"] = listRefs["Referencia"].str.replace(ce, " ")
			
			for i in range(listRefs.shape[0]):
				listRefs["Referencia"][i] = quitaDoblesEspacios(listRefs["Referencia"][i])
				
			listRefs["Referencia"] = listRefs["Referencia"].str.strip()
			
			# Normalizamos los campos
			listRefs["Referencia_Normalizada"] = ''
			for i in range(listRefs.shape[0]):
				normaliza = listRefs["Referencia"][i]
				normaliza = normaliza.replace(" un ", " uno ")
				aux_str = normaliza.split(" de ")
				aux_norm = aux_str[len(aux_str) - 1]
				aux_norm2 = aux_norm.split(" y ")
				resNums = []
				for aux_norm in aux_norm2:
					aux_str = aux_norm.split(" ")
					unidades = aux_str[len(aux_str) - 1]
					number = reduce_concat(aux_str[:-1], sep = " ")
					try:
						number = transformaNumsString(number)
					except:
						pass
					resNums.append(number + " " + unidades)
					
				listRefs["Referencia_Normalizada"][i] = reduce_concat(resNums, sep = " y ")
			
			# ================== #
			# === Script 052 === #
			# ================== #
			
			# Montamos un string con la misma estructura y con acentos
			try:
				RawReadedFileAcc = str(RawReadedFile.decode("utf8").encode("latin1", 'ignore'))
			except:
				RawReadedFileAcc = str(RawReadedFile.encode("latin1", "ignore"))

			i = 0
			while i < len(RawReadedFileAcc):
				letterAcc = RawReadedFileAcc[i]
				letterra = readedFile[i]
				if remove_accents(letterAcc, enc = "latin1").lower() != letterra:
					if letterAcc == readedFile[i + 1]:
						readedFile = readedFile[:i] + readedFile[(i + 1):]
					else:
						RawReadedFileAcc = RawReadedFileAcc[:i] + RawReadedFileAcc[(i + 1):]
				else:
					i += 1

			if len(RawReadedFileAcc) != len(readedFile):
				raise Exception("No pot ser!!")
			
			# Leemos archivo posiciones fechas normalizadas
			ddNorm = deepcopy(listRefs)
			
			# Cogemos los textos que hay antes y despues de cada fecha
			ddNorm["Antes"] = ""
			ddNorm["Despues"] = ""
			x = 1000
			for irow in range(ddNorm.shape[0]):
				ddNorm["Antes"][irow] = RawReadedFileAcc[max(ddNorm["PosInicio"][irow] - x, 0):ddNorm["PosInicio"][irow]]
				aux_text = RawReadedFileAcc[(ddNorm["PosFin"][irow] - 1):min(ddNorm["PosFin"][irow] + x, (len(readedFile) - 1))]
				if len(aux_text) > 0:
					if aux_text[0] == ddNorm["Referencia"][irow][-1]:
						ddNorm["Despues"][irow] = aux_text[1:]
					else:
						ddNorm["Despues"][irow] = aux_text
				else:
					ddNorm["Despues"][irow] = ""
				
				
			# Pasamos a frases los textos de antes y despues
			minText = 15
			ddNorm["Contexto"] = ""
			for irow in range(ddNorm.shape[0]):
				ddNorm["Antes"][irow] = ddNorm["Antes"][irow].replace("\n", " ")
				ddNorm["Despues"][irow] = ddNorm["Despues"][irow].replace("\n", " ")
				auxa = sentences(ddNorm["Antes"][irow])
				auxa = auxa.split(" .\n")
				textant = auxa[len(auxa) - 1]
				if len(textant) < minText:
					textant = reduce_concat(auxa[(len(auxa) - 2):(len(auxa) - 1)], sep = ". ")
				ddNorm["Antes"][irow] = textant
				
				auxd = sentences(ddNorm["Despues"][irow])
				auxd = auxd.split(" .\n")
				textdsp = auxd[0]
				if len(textdsp) < minText:
					textdsp = reduce_concat(auxd[:2], sep = ". ")
				ddNorm["Despues"][irow] = textdsp
				
				ddNorm["Contexto"][irow] = ddNorm["Antes"][irow] + ddNorm["Referencia"][irow] + ddNorm["Despues"][irow]
				
			# Vemos que relevancia tiene cada elemento
			aux_tt = ddNorm["Referencia_Normalizada"].value_counts()
			aux_tt2 = aux_tt/max(aux_tt)
			
			aux_tt = pd.DataFrame(data = dict(Referencia_Normalizada = list(aux_tt.index), Apariciones = list(aux_tt.values), Relevancia = list(aux_tt2.values)))
			ddNorm = ddNorm.merge(aux_tt, 'left')
			
			# ============================= #
			# === Escribimos resultados === #
			# ============================= #
			
			# Guardamos outputs y los dejamos en ES
			if guardaResultados:
				if es_hdfs:
					fileSave = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_info_pos"
					guardaDFHDFS(ddNorm, fileSave, sc)
					
				else:
					ddNorm.to_csv(path_or_buf = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_info_pos.csv", sep = "|")
			
			if muestraHTML and not es_hdfs:
			
				# Normalizamos las sentencias
				result = dict()
				for val in ddNorm["Referencia_Normalizada"].unique():
					if int(aux_tt["Apariciones"][aux_tt["Referencia_Normalizada"] == val]) == 1:
						# Aqui posem les frases que apareixen a pinyo
						sel = ddNorm["Referencia_Normalizada"] == val
						aux_data = ddNorm[sel]
						aux_data = aux_data.reset_index()
						texto = str(aux_data["Antes"][0]) + str(aux_data["Ref_Orig"][0]) + str(aux_data["Despues"][0])
						result[val] = dict(descripcion = texto.strip())
						result[val]["posiciones"] = dict(inicio = list(aux_data["PosInicio"]), fin = list(aux_data["PosFin"])) 
						result[val]["referencias"] = list(aux_data["Ref_Orig"])
					else:
						# Aqui fem l'analisis per mes d'una frase, si son iguals intentem fer una frase descriptiva, sino doncs per cada item 
						# posem el que s'assembli mes (PROVAR AMB GENSIM!!)
						frases = []
						aux_data = ddNorm[ddNorm["Referencia_Normalizada"] == val]
						aux_data = aux_data.reset_index()
						for irow in range(aux_data.shape[0]):
							texto = aux_data["Antes"][irow] + aux_data["Ref_Orig"][irow] + aux_data["Despues"][irow]
							frases.append(texto)

						# Comparem frases i veiem quines s'assemblen a quines
						auxRes = comparaFrases(frases)
						gruposUnidos = unirGrupos(list(auxRes["item1"]), list(auxRes["item2"]), list(auxRes["valor_comp"] > limSimFrases))
						
						# Extraiem les frases mes rellevants per a cada grup
						frasesGrup = []
						for grupo in gruposUnidos:
							frases_grupo_act = [""]
							for element in grupo:
								frases_grupo_act[0] = frases_grupo_act[0] + " " + frases[element]
								frases_grupo_act.append(frases[element])
								
							auxRes = comparaFrases(frases_grupo_act, totesContraTotes = False)
							auxResSel = auxRes[(auxRes["item1"] == 0) | (auxRes["item2"] == 0)]
							auxResSel = auxResSel[auxResSel["valor_comp"] == max(auxResSel["valor_comp"])].reset_index()
							auxResSel = auxResSel.loc[0, ["item1", "item2"]]
							i = int(auxResSel[auxResSel != 0]) - 1
							frasesGrup.append(frases[grupo[i]])
						
						if len(frasesGrup) == 1:
							result[val] = dict(descripcion = frasesGrup[0])
							result[val]["posiciones"] = dict(inicio = list(aux_data["PosInicio"]), fin = list(aux_data["PosFin"])) 
							result[val]["referencias"] = list(aux_data["Ref_Orig"])
						else:
							for igrupo in range(len(gruposUnidos)):
								result[val + "_" + str(igrupo + 1)] = dict(descripcion = frasesGrup[igrupo])
								result[val + "_" + str(igrupo + 1)]["posiciones"] = dict(inicio = list(aux_data.loc[(gruposUnidos[igrupo])]["PosInicio"]), fin = list(aux_data.loc[(gruposUnidos[igrupo])]["PosFin"]))
								result[val + "_" + str(igrupo + 1)]["referencias"] = list(aux_data.loc[(gruposUnidos[igrupo])]["Ref_Orig"])
								for ielement in range(len(gruposUnidos[igrupo])):
									ddNorm["Referencia_Normalizada"][aux_data["index"][gruposUnidos[igrupo][ielement]]] = val + "_" + str(igrupo + 1)
								
				# Reescribimos fichero posiciones con las referencias normalizadas
				for ikey in result.keys():
					result[ikey]["descripcion"] = result[ikey]["descripcion"].decode("latin1")
					for i in range(len(result[ikey]["referencias"])):
						result[ikey]["referencias"][i] = result[ikey]["referencias"][i].decode("latin1")
						result[ikey]["posiciones"]["inicio"][i] = str(result[ikey]["posiciones"]["inicio"][i])
						result[ikey]["posiciones"]["fin"][i] = str(result[ikey]["posiciones"]["fin"][i])
						
				dataJSON = deepcopy(result)
				name_file = filename.lower().replace(".pdf", "")
				
				ddNorm['order'] = [sum(ddNorm['Referencia_Normalizada'] == ddNorm['Referencia_Normalizada'][i]) for i in xrange(len(ddNorm))]
				ddNorm = ddNorm.sort_values("order", ascending = False)
				regUnics = pd.unique(ddNorm["Referencia_Normalizada"])
				ddNorm["id_reg"] = ""
				compta = 0
				for registre in regUnics:
					sel = ddNorm["Referencia_Normalizada"] == registre
					ddNorm["id_reg"][sel] = compta
					compta += 1
				
				# Definimos las marcas
				ddNorm['mark'] = 'mark09'
				for i in range(1, 9):
					sel = ddNorm["id_reg"] == (i - 1)
					ddNorm["mark"][sel] = 'mark0' + str(i)
				
				# Escribimos html's con descripciones y definimos href
				ddNorm['href'] = ''
				for ikey in dataJSON.keys():
					hrefFile = "href " + name_file + " " + ikey.replace('"', '').replace("'", "") + ".html"
					textohref = "Fecha: " + str(ikey) + "<br>Contexto: " + dataJSON[ikey]["descripcion"].encode("latin1") + "<br>Apariciones: " + str(len(dataJSON[ikey]["referencias"]))
					filehtml = open(OUTPUTDIR + hrefFile, mode = "w+")
					filehtml.write(textohref)
					filehtml.close()
					# sel = [x in dataJSON[ikey]["referencias"] for x in ddNorm["Ref_Orig"]]
					sel = ddNorm['Referencia_Normalizada'] == ikey
					ddNorm['href'][sel] = hrefFile
				
				# Escribimos html
				textohtml = "<head> <link rel = 'stylesheet' type = 'text/css' href = 'styles_css.css'> </head>"
				i_suma = 0
				for i in range(ddNorm.shape[0]):
					before = "<a href = '" + ddNorm["href"][i] + "'>" + "<" + ddNorm["mark"][i] + ">"
					after = "</" + ddNorm["mark"][i] + "></a>"
					readedFile = readedFile[:(int(ddNorm["PosInicio"][i]) + i_suma)] + before + readedFile[(int(ddNorm["PosInicio"][i]) + i_suma):(int(ddNorm["PosFin"][i]) + i_suma)] + after + readedFile[(int(ddNorm["PosFin"][i]) + i_suma):]
					i_suma += len(before) + len(after)
				textohtml += "<p>" + readedFile + "</p>"
				
				filehtml = OUTPUTDIR + name_file + "_muestra_plazos.html"
				filehtml = open(filehtml, mode = "w+")
				filehtml.write(textohtml)
				filehtml.close()
				
			if guardaES:
			
				# Sacamos las posiciones de cada pagina
				perPageDoc = read_file_pagebypage(filename, criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)
				count_len = 0
				listpos = []
				for item in perPageDoc:
					listpos.append((count_len, count_len + len(item) - 1))
					count_len += len(item)
				
				rawDoc = reduce_concat(perPageDoc)
				
				# Subimos a ES
				
				# Buscamos el texto en el rawDoc
				ddNorm["posRawDocIni"] = -1
				ddNorm["posRawDocFin"] = -1
				for i in range(ddNorm.shape[0]):
					texto = ddNorm["Referencia"][i]
					text2search = rawDoc[ddNorm["PosInicio"][i]:]
					text2search = remove_accents(text2search)
					text2search = text2search.lower()
					if texto in text2search:
						sumposini = text2search.index(texto)
					else:
						caracteres_elimina = [":", ".", "(", ")", ",", ";"]
						caracteres_espacio = ["\n", "/", "-"]
						for cel in caracteres_elimina:
							text2search = text2search.replace(cel, "")
						for ces in caracteres_espacio:
							text2search = text2search.replace(ces, " ")
							
						text2search = quitaDoblesEspacios(text2search)
						sumposini = text2search.index(texto)
						
					ddNorm["posRawDocIni"][i] = ddNorm["PosInicio"][i] + sumposini
					ddNorm["posRawDocFin"][i] = ddNorm["posRawDocIni"][i] + len(texto)
				
				# Vemos en que pagina y que posicion ocupa cada entidad
				ddNorm["pagina"] = '-1'
				for i in range(ddNorm.shape[0]):
					posBusca = ddNorm.loc[[i], ["posRawDocIni", "posRawDocFin"]]
					posBusca = posBusca.values.tolist()[0]
					encontrada = False
					ipag = 0
					while not encontrada and ipag < len(listpos):
						ipag += 1
						if listpos[ipag - 1][0] <= posBusca[0] and listpos[ipag - 1][1] >= posBusca[1]:
							ddNorm["pagina"][i] = str(ipag)
							encontrada = True
						elif len(listpos) > ipag:
							if listpos[ipag - 1][1] >= posBusca[0] and listpos[ipag][0] <= posBusca[1]:
								ddNorm["pagina"][i] = str(ipag - 1) + "," + str(ipag)
								encontrada = True
				
				ddNorm[ddNorm["pagina"] == -1]["pagina"] = len(listpos)
				ddNorm = posicionporpagina(ddNorm, listpos)
				
				# Subimos diccionarios a ES
				for i in range(ddNorm.shape[0]):

					# Creamos diccionario
					dictloadES = {
						"Doc_plazo_" + filename.lower().replace(".pdf", "") + "_" + str(i): dict(
							tipo = "plazo",
							documento = filename,
							pagina = ddNorm["pagina"][i],
							pos_inicial = int(ddNorm["posRawDocIni"][i]),
							pos_final = int(ddNorm["posRawDocFin"][i]),
							texto = ddNorm["Referencia"][i],
							texto_norm = ddNorm["Referencia_Normalizada"][i],
							contexto = ddNorm["Contexto"][i]
						)
					}

					load2Elastic(dictloadES, INDEX_NAME = indicewrite, TYPE_NAME =	"doc", newesconn = Elasticsearch([ipwrite]))
		

		print "Plazos finalizado."


		# ============================== #
		# === 020_Union_fechas_leyes === #
		# ============================== #

		print "Procesando union fechas y leyes..."

		# Current directories
		OUTPUTWRITE = PATH + "output/uni_fechas_ley/"
		INPUTNORMLEYES = PATH + "output/resultados_leyes/"
		INPUTNORMFECHA = PATH + "output/resultados_fechas/"

		limLenDist = 20
		guardaES = guardaES and not eslocal	 # Solo guardamos a elastic si no se ejecuta desde local
		
		
		articulosDict = dict(
			es = dict(
				esp = ['[\s|\n]art\.', 'articulo', '\(art\.', '\(articulo']
			),
			pt = dict(
				bra = ['[\s|\n]art\.', 'artigo', '\(art\.', '\(artigo']
			)
		)
		
		articulos = articulosDict[idioma][pais]

		if eslocal:
			RawReadedFile = pdf2txt(INPUTREAD + filename)
		else:
			RawReadedFile = import_doc(filename, criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)
		readedFile = remove_accents(RawReadedFile)
		readedFile = readedFile.lower()
		
		if ".pdf" in filename.lower():
			if es_hdfs:
				filejsonfecha = INPUTNORMFECHA + filename.lower().replace(".pdf", "_entities")
				filenormleyes = INPUTNORMLEYES + filename.lower().replace(".pdf", "_info_pos")
				filenormfecha = INPUTNORMFECHA + filename.lower().replace(".pdf", "_info_pos")
			else:
				filejsonfecha = INPUTNORMFECHA + filename.lower().replace(".pdf", "_entities.json")
				filenormleyes = INPUTNORMLEYES + filename.lower().replace(".pdf", "_info_pos.csv")
				filenormfecha = INPUTNORMFECHA + filename.lower().replace(".pdf", "_info_pos.csv")
			
		else:
			if es_hdfs:
				filejsonfecha = INPUTNORMFECHA + filename.lower() + "_entities"
				filenormleyes = INPUTNORMLEYES + filename.lower() + "_info_pos"
				filenormfecha = INPUTNORMFECHA + filename.lower() + "_info_pos"
			else:
				filejsonfecha = INPUTNORMFECHA + filename.lower() + "_entities.json"
				filenormleyes = INPUTNORMLEYES + filename.lower() + "_info_pos.csv"
				filenormfecha = INPUTNORMFECHA + filename.lower() + "_info_pos.csv"

		faltanLeyes = False
		faltanFechas = False
		
		if (not exists(filenormleyes) or not exists(filenormfecha)) and not es_hdfs:
			print "No tenim detectada la informacio pel document " + filename + ", per tant no el processem."
			if not exists(filenormleyes):
				faltanLeyes = True
			if not exists(filenormfecha):
				faltanFechas = True
		
		if not es_hdfs:
			if not faltanFechas:
				dataNormFecha = pd.read_csv(filenormfecha, sep = "|")
			if not faltanLeyes:
				dataNormLeyes = pd.read_csv(filenormleyes, sep = "|")
				
		elif es_hdfs:
			try:
				dataNormLeyes = readDFHDFS(filenormleyes, sc)
			except Exception as e:
				print "No tenemos detectada la informacion para el documento " + filename + " de leyes, por tanto no la procesamos."
				faltanLeyes = True
				
			try:
				dataNormFecha = readDFHDFS(filenormfecha, sc)
			except Exception as e:
				print "No tenemos detectada la informacion para el documento " + filename + " de fechas, por tanto no la procesamos."
				faltanFechas = True
		
		# Si solo tenemos uno de los dos archivos (fechas o leyes) lo subimos directamente a Elastic
		if not faltanLeyes and not faltanFechas:
			# Comprovem que no estiguin unes dins d'altres
			for i in range(dataNormLeyes.shape[0]):
				sel1 = (dataNormFecha["PosInicio"] >= dataNormLeyes["PosInicio"][i]) & (dataNormFecha["PosInicio"] <= dataNormLeyes["PosFin"][i])
				sel2 = (dataNormFecha["PosFin"] >= dataNormLeyes["PosInicio"][i]) & (dataNormFecha["PosFin"] <= dataNormLeyes["PosFin"][i])
				sel = sel1 | sel2
				if any(sel):
					aux_data = dataNormLeyes[["PosInicio", "PosFin"]][dataNormLeyes.index == i]
					aux_data = aux_data.append(dataNormFecha[["PosInicio", "PosFin"]][sel])
					dataNormLeyes["PosInicio"][dataNormLeyes.index == i] = min(aux_data["PosInicio"])
					dataNormLeyes["PosFin"][dataNormLeyes.index == i] = max(aux_data["PosFin"])
					dataNormFecha = dataNormFecha[~sel]
					
			dataNormFecha = dataNormFecha.reset_index()
			
			# Vemos si estan cerca o no las leyes y las fechas
			dictVincles = dict()
			fechas_elimina = []
			for i in range(dataNormLeyes.shape[0]):
				for j in range(dataNormFecha.shape[0]):
					allNums = [
						abs(dataNormLeyes["PosInicio"][i] - dataNormFecha["PosInicio"][j]), 
						abs(dataNormLeyes["PosFin"][i] - dataNormFecha["PosInicio"][j]),
						abs(dataNormLeyes["PosInicio"][i] - dataNormFecha["PosFin"][j]), 
						abs(dataNormLeyes["PosFin"][i] - dataNormFecha["PosFin"][j])
					]
					if min(allNums) <= limLenDist:
						if i in dictVincles.keys():
							dictVincles[i].append(j)
						else:
							dictVincles[i] = [j]
						
						fechas_elimina.append(j)

			fechas_elimina = list(set(fechas_elimina))
			
			# Unificamos las entidades
			for ikey in dictVincles.keys():
				aux_data = dataNormLeyes.loc[[ikey], ["PosInicio", "PosFin"]]
				for j in dictVincles[ikey]:
					aux_data = aux_data.append(dataNormFecha.loc[[j], ["PosInicio", "PosFin"]])
			
				dataNormLeyes.loc[[ikey], ["PosInicio", "PosFin"]] = [min(aux_data["PosInicio"]), max(aux_data["PosFin"])]
				aux_text = readedFile[min(aux_data["PosInicio"]):max(aux_data["PosFin"])]
				dataNormLeyes.loc[[ikey], ["Referencia"]] = aux_text
				dataNormLeyes.loc[[ikey], ["RefNorm"]] = aux_text
			
			
			# Eliminamos los duplicados en posiciones y las fechas que en realidad son leyes
			dataNormLeyes = dataNormLeyes[~dataNormLeyes[["PosInicio", "PosFin"]].duplicated()]
			fechas_elimina.sort()
			fechas_elimina.reverse()
			for ielimina in fechas_elimina:
				sel = dataNormFecha.index != ielimina
				dataNormFecha = dataNormFecha[sel]
				
			dataNormFecha = dataNormFecha.reset_index()

			
			
		# ====================================== #
		# === Sacamos contexto para cada ley === #
		# ====================================== #
		
		if not faltanLeyes:
			# Sacamos frases que buscar a tirant lo blanc
			palabras_busca = []
			for item in CompleteStrSearchLeyes:
				palabras_busca.extend(item['contiene'])
			palabras_busca.extend(DictSubsMeses[idioma][pais].keys())
			palabras_busca = list(set(palabras_busca))
			for i in range(len(palabras_busca)):
				palabras_busca[i] = palabras_busca[i].replace("[\s|\n]", "\\b")
			
			dataNormLeyes["RefBusca"] = ""
			for irow in range(dataNormLeyes.shape[0]):
				trobat = False
				aux_text = quitaDoblesEspacios(dataNormLeyes["Referencia"][irow])
				if " de la " in aux_text:
					text = aux_text.split(" de la ")
					text = text[len(text) - 1]
					if any([len(re.findall(palabra, text)) > 0 for palabra in palabras_busca]):
						trobat = True
						dataNormLeyes["RefBusca"][irow] = text
				
				elif " del " in aux_text:
					text = aux_text.split(" del ")
					text = text[len(text) - 1]
					if any([len(re.findall(palabra, text)) > 0 for palabra in palabras_busca]):
						trobat = True
						dataNormLeyes["RefBusca"][irow] = text
						
				if not trobat and any([len(re.findall(regexpr, aux_text)) > 0 for regexpr in articulos]):
					exprBusca = [regexpr for regexpr in articulos if len(re.findall(regexpr, aux_text)) > 0]
					exprBusca = exprBusca[0]
					dataNormLeyes["RefBusca"][irow] = aux_text[buscaPosicionRegexTexto(exprBusca, aux_text)[0][0]:]
					trobat = True
				
				if not trobat:
					aux_text = aux_text.split(",")
					
					parat = False
					i = 0
					while not parat and i < len(aux_text):
						if not any([len(re.findall(palabra, aux_text[i])) > 0 for palabra in palabras_busca]):
							parat = True
							
						i += 1
					
					dataNormLeyes["RefBusca"][irow] = reduce_concat(aux_text[:i], ",")
			
			
			# Cogemos los textos que hay antes y despues de cada fecha
			dataNormLeyes["Antes"] = ""
			dataNormLeyes["Despues"] = ""
			x = 150
			for irow in range(dataNormLeyes.shape[0]):
				dataNormLeyes["Antes"][irow] = readedFile[(dataNormLeyes["PosInicio"][irow] - x):dataNormLeyes["PosInicio"][irow]]
				aux_text = readedFile[(dataNormLeyes["PosFin"][irow] - 1):(dataNormLeyes["PosFin"][irow] + x)]
				if len(aux_text) > 0:
					if aux_text[0] == dataNormLeyes["Referencia"][irow][-1]:
						dataNormLeyes["Despues"][irow] = aux_text[1:]
					else:
						dataNormLeyes["Despues"][irow] = aux_text
				else:
					dataNormLeyes["Despues"][irow] = ""
			
			
			# ========================================= #
			# === Tenemos en cuenta leyes validadas === #
			# ========================================= #
			
			# Escribimos archivo con nuevas leyes por validar
		#		if not es_hdfs:

			
			filenameLeyesValidas = PATH + "data/leyes_validas.csv"
			filenameleyes = PATH + "data/leyes_valida.csv"
			
			if not es_hdfs and not eslocal:
				with open(filenameleyes, mode = "a+") as fw:
					for irow in range(dataNormLeyes.shape[0]):
						fw.write(
							'"' + dataNormLeyes['Antes'][irow].replace(";", ",").replace('"', '').replace('\n', '') + '";"' +
							dataNormLeyes['Referencia'][irow].replace(";", ",").replace('"', '').replace('\n', '') + '";"' + 
							dataNormLeyes['Despues'][irow].replace(";", ",").replace('"', '').replace('\n', '') + '";0;' + "\n"
						)
			elif es_hdfs:
				pass
				# Escriure dataNormLeyes en hdfs en mode a+
			
			
			if nuevas_leyes and not eslocal and not es_hdfs:	# El ultimo and not es_hdfs se tendra que borrar cuando este todo en hdfs
				# Si se han validado nuevas leyes
				if not es_hdfs and not eslocal:
					with open(filenameleyes, mode = "r+") as fr:
						todas_leyes = fr.readlines()
						newDictLeyes = dict()
						todasKeys = todas_leyes[0].replace("\n", "").split(";")
						for ikey in todasKeys:
							newDictLeyes[ikey] = []
						for i in range(1, len(todas_leyes)):
							item = todas_leyes[i].replace("\n", "").split(";")
							for ikey in range(len(todasKeys)):
								newDictLeyes[todasKeys[ikey]].append(item[ikey])
								
					todas_leyes = pd.DataFrame(newDictLeyes)
					
				elif es_hdfs:
					pass
					# Llegir filenameleyes com a DataFrame en hdfs
				
				# Cogemos el texto que es válido
				todas_leyes["nuevo_texto"] = todas_leyes["texto_valido"]
				todas_leyes["nuevo_texto"][todas_leyes["texto_valido"] == ""] = todas_leyes["texto_ley"][todas_leyes["texto_valido"] == ""]
				
				NuevasLeyes = todas_leyes["nuevo_texto"][todas_leyes["validado"] == "1"]	# Conservamos las nuevas leyes
				
				# Reescribimos fichero y lo vaciamos un poco
				leyes_pendientes = todas_leyes[todas_leyes["validado"] == "0"]
				leyes_pendientes = leyes_pendientes.reset_index()
				del leyes_pendientes["index"]
				del leyes_pendientes["nuevo_texto"]
				for irow in range(leyes_pendientes.shape[0]):
					for ikey in todasKeys:
						leyes_pendientes[ikey][irow] = leyes_pendientes[ikey][irow].replace('"', '')
				
				leyes_pendientes = leyes_pendientes[todasKeys]
				if not es_hdfs:
					leyes_pendientes.to_csv(filenameleyes, sep = ";", index = False)
				
					# Leemos todas las leyes validadas y sobreescribimos el fichero con las nuevas
					with open(filenameLeyesValidas, mode = "r+") as fr:
						leyes_validadas = fr.readlines()
				
				else:
					pass
					# Escribir dataframe y leer leyes validadas
				
				leyes_validadas.extend(list(NuevasLeyes))
				for i in range(len(leyes_validadas)):
					leyes_validadas[i] = leyes_validadas[i].replace('"', '').replace("\n", "")
				
				leyes_validadas = list(set(leyes_validadas))
				
				ordenallista = [len(item) for item in leyes_validadas]
				llistaEscriu = [x for _, x in sorted(zip(ordenallista, leyes_validadas), reverse = True)]
				
				if not es_hdfs:
					with open(filenameLeyesValidas, mode = "w+") as fw:
						for item in llistaEscriu:
							fw.write(item + "\n")
				else:
					pass
					# Escribir archivo de leyes validadas
			
			# Llegim el fitxer escrit previament i modifiquem lleis
			if not es_hdfs:	 # Aquest if s'haura de treure quan estigui tot implementat en HDFS!!
				if not es_hdfs and not eslocal:
					with open(filenameLeyesValidas, mode = "r+") as fr:
						leyes_validadas = fr.readlines()
						
				elif es_hdfs:
					pass
					# Leer archivo de leyes validadas
				
				if not eslocal:
					for i in range(len(leyes_validadas)):
						leyes_validadas[i] = leyes_validadas[i].replace("\n", "")
					
					dataNormLeyes["Referencia_ant"] = dataNormLeyes["Referencia"]
					for irow in range(dataNormLeyes.shape[0]):
						encontrado = False
						i = 0
						while not encontrado and i < len(leyes_validadas):
							
							if leyes_validadas[i] in dataNormLeyes['Antes'][irow] + dataNormLeyes["Referencia"][irow] + dataNormLeyes['Despues'][irow]:
								dataNormLeyes["Referencia"][irow] = leyes_validadas[i]
								encontrado = True
						
							i += 1
							
					# Volvemos a escribir los que han cambiado
					dataNormLeyesW = dataNormLeyes[dataNormLeyes["Referencia"] != dataNormLeyes["Referencia_ant"]]
					dataNormLeyesW = dataNormLeyesW.reset_index()
					
					if not es_hdfs:
						with open(filenameleyes, mode = "a+") as fw:
							for irow in range(dataNormLeyesW.shape[0]):
								fw.write(
									'"' + dataNormLeyesW['Antes'][irow].replace(";", ",").replace('"', '').replace('\n', '') + '";"' +
									dataNormLeyesW['Referencia'][irow].replace(";", ",").replace('"', '').replace('\n', '') + '";"' + 
									dataNormLeyesW['Despues'][irow].replace(";", ",").replace('"', '').replace('\n', '') + '";0;' + "\n"
								)
					else:
						pass
						# Escribir en modo a+ dataNormLeyesW
			
		# ============================= #
		# === Escribimos resultados === #
		# ============================= #
		
		if guardaResultados and not es_hdfs:
			
			if not faltanLeyes and not faltanFechas:
			
				# Escribimos archivos
				if ".pdf" in filename.lower():
					fileOut = OUTPUTWRITE + filename.lower().replace(".pdf", "_fechas.csv")
				else:
					fileOut = OUTPUTWRITE + filename.lower() + "_fechas.csv"
				dataNormFecha.to_csv(fileOut, sep = "|")
				if ".pdf" in filename.lower():
					fileOut = OUTPUTWRITE + filename.lower().replace(".pdf", "_leyes.csv")
				else:
					fileOut = OUTPUTWRITE + filename.lower() + "_leyes.csv"
				dataNormLeyes.to_csv(fileOut, sep = "|")
				
				# Modificamos el fichero json segun los registros que queden en fecha
				with open(filejsonfecha, 'r') as fj:
					datafecha = json.load(fj)
					
				for ikey in datafecha.keys():
					if ikey not in dataNormFecha["Referencia_Normalizada"]:
						datafecha[ikey] = None
				
				if ".pdf" in filename.lower():
					fileOut = OUTPUTWRITE + filename.lower().replace(".pdf", "_fechas_json.json")
				else:
					fileOut = OUTPUTWRITE + filename.lower() + "_fechas_json.json"
				with open(fileOut, 'w') as fj:
					json.dump(datafecha, fj)
				
			if muestraHTML and not es_hdfs and not faltanLeyes and not faltanFechas:
				dataNormLeyes["mark"] = 'mark01'
				dataNormFecha["mark"] = 'mark02'
				dataWriteHTML = dataNormLeyes[["PosInicio", "PosFin", "mark"]]
				dataWriteHTML = dataWriteHTML.append(dataNormFecha[["PosInicio", "PosFin", "mark"]])
				dataWriteHTML = dataWriteHTML.sort_values(["PosInicio"])
				dataWriteHTML = dataWriteHTML.reset_index()
				
				textohtml = "<head> <link rel = 'stylesheet' type = 'text/css' href = 'styles_css.css'> </head>"
				i_suma = 0
				for i in range(dataWriteHTML.shape[0]):
					before = "<" + dataWriteHTML["mark"][i] + ">"
					after = "</" + dataWriteHTML["mark"][i] + ">"
					readedFile = readedFile[:(int(dataWriteHTML["PosInicio"][i]) + i_suma)] + before + readedFile[(int(dataWriteHTML["PosInicio"][i]) + i_suma):(int(dataWriteHTML["PosFin"][i]) + i_suma)] + after + readedFile[(int(dataWriteHTML["PosFin"][i]) + i_suma):]
					i_suma += len(before) + len(after)
				textohtml += "<p>" + readedFile + "</p>"
				
				filehtml = OUTPUTWRITE + filename.lower().replace(".pdf", "") + "_muestra_fec_ley.html"
				filehtml = open(filehtml, mode = "w+")
				filehtml.write(textohtml)
				filehtml.close()
			
		if guardaES:
			
			if any([not faltanFechas, not faltanLeyes]):
				
				# Sacamos las posiciones de cada pagina
				perPageDoc = read_file_pagebypage(filename, criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)
				count_len = 0
				listpos = []
				for item in perPageDoc:
					listpos.append((count_len, count_len + len(item) - 1))
					count_len += len(item)
				
				rawDoc = reduce_concat(perPageDoc)
			
			# ============= #
			# === leyes === #
			# ============= #
			
			if not faltanLeyes:
				
				# Buscamos el texto en el rawDoc
				dataNormLeyes["posRawDocIni"] = -1
				dataNormLeyes["posRawDocFin"] = -1
				for i in range(dataNormLeyes.shape[0]):
					texto = dataNormLeyes["Referencia"][i]
					text2search = rawDoc[dataNormLeyes["PosInicio"][i]:]
					text2search = remove_accents(text2search)
					text2search = text2search.lower()
					if texto in text2search:
						sumposini = text2search.index(texto)
					elif i == 0:
						sumposini = 0	 # Nomes quan es la primera la igualem a 0, en els altres casos es millor que es quedi amb el valor de la iteracio anterior
						
					dataNormLeyes["posRawDocIni"][i] = dataNormLeyes["PosInicio"][i] + sumposini
					dataNormLeyes["posRawDocFin"][i] = dataNormLeyes["posRawDocIni"][i] + len(texto)
					
				# Vemos en que pagina y que posicion ocupa cada entidad
				dataNormLeyes["pagina"] = '-1'
				for i in range(dataNormLeyes.shape[0]):
					posBusca = dataNormLeyes.loc[[i], ["posRawDocIni", "posRawDocFin"]]
					posBusca = posBusca.values.tolist()[0]
					encontrada = False
					ipag = 0
					while not encontrada and ipag < len(listpos):
						ipag += 1
						if listpos[ipag - 1][0] <= posBusca[0] and listpos[ipag - 1][1] >= posBusca[1]:
							dataNormLeyes["pagina"][i] = str(ipag)
							encontrada = True
						elif len(listpos) > ipag:
							if listpos[ipag - 1][1] >= posBusca[0] and listpos[ipag][0] <= posBusca[1]:
								dataNormLeyes["pagina"][i] = str(ipag - 1) + "," + str(ipag)
								encontrada = True
				
				dataNormLeyes[dataNormLeyes["pagina"] == -1]["pagina"] = len(listpos)
				dataNormLeyes = posicionporpagina(dataNormLeyes, listpos)
				
				# Subimos diccionarios a ES
				for i in range(dataNormLeyes.shape[0]):

					# Creamos diccionario
					ley2loadES = {
						"Doc_ley_" + filename.lower().replace(".pdf", "") + "_" + str(i): dict(
							tipo = "LRR",
							documento = filename,
							pagina = dataNormLeyes["pagina"][i],
							pos_inicial = int(dataNormLeyes["posRawDocIni"][i]),
							pos_final = int(dataNormLeyes["posRawDocFin"][i]),
							texto = dataNormLeyes["Referencia"][i],
							texto_busca = dataNormLeyes["RefBusca"][i],
							contexto = str(dataNormLeyes["Antes"][i]) + str(dataNormLeyes["Referencia"][i]) + str(dataNormLeyes["Despues"][i])
						)
					}
					
					load2Elastic(ley2loadES, INDEX_NAME = indicewrite, TYPE_NAME =	"doc", newesconn = Elasticsearch([ipwrite]))
				
			
			# ============== #
			# === fechas === #
			# ============== #
			
			if not faltanFechas:

				# Buscamos el texto en el rawDoc
				dataNormFecha["posRawDocIni"] = -1
				dataNormFecha["posRawDocFin"] = -1
				for i in range(dataNormFecha.shape[0]):
					texto = dataNormFecha["Referencia"][i]
					text2search = rawDoc[dataNormFecha["PosInicio"][i]:]
					text2search = remove_accents(text2search)
					text2search = text2search.lower()
					if texto in text2search:
						sumposini = text2search.index(texto)
					else:
						caracteres_elimina = [":", ".", "(", ")", ",", ";"]
						caracteres_espacio = ["\n", "/", "-"]
						for cel in caracteres_elimina:
							text2search = text2search.replace(cel, "")
						for ces in caracteres_espacio:
							text2search = text2search.replace(ces, " ")
						
						text2search = quitaDoblesEspacios(text2search)
						sumposini = text2search.index(texto)
						
					dataNormFecha["posRawDocIni"][i] = dataNormFecha["PosInicio"][i] + sumposini
					dataNormFecha["posRawDocFin"][i] = dataNormFecha["posRawDocIni"][i] + len(texto)
					
				
				# Vemos en que pagina y que posicion ocupa cada entidad
				dataNormFecha["pagina"] = '-1'
				for i in range(dataNormFecha.shape[0]):
					posBusca = dataNormFecha.loc[[i], ["posRawDocIni", "posRawDocFin"]]
					posBusca = posBusca.values.tolist()[0]
					encontrada = False
					ipag = 0
					while not encontrada and ipag < len(listpos):
						ipag += 1
						if listpos[ipag - 1][0] <= posBusca[0] and listpos[ipag - 1][1] >= posBusca[1]:
							dataNormFecha["pagina"][i] = str(ipag)
							encontrada = True
						elif len(listpos) > ipag:
							if listpos[ipag - 1][1] >= posBusca[0] and listpos[ipag][0] <= posBusca[1]:
								dataNormFecha["pagina"][i] = str(ipag - 1) + "," + str(ipag)
								encontrada = True
				
				dataNormFecha[dataNormFecha["pagina"] == -1]["pagina"] = len(listpos)
				dataNormFecha = posicionporpagina(dataNormFecha, listpos)
				
				# Subimos diccionarios a ES
				for i in range(dataNormFecha.shape[0]):
				
					# Creamos diccionario
					fecha2loadES = {
						"Doc_fecha_" + filename.lower().replace(".pdf", "") + "_" + str(i): dict(
							tipo = "fecha",
							documento = filename,
							pagina = dataNormFecha["pagina"][i],
							pos_inicial = int(dataNormFecha["posRawDocIni"][i]),
							pos_final = int(dataNormFecha["posRawDocFin"][i]),
							texto = dataNormFecha["Referencia"][i],
							texto_norm = dataNormFecha["Referencia_Normalizada"][i].split("_")[0],
							contexto = dataNormFecha["Contexto"][i],
							tags = dataNormFecha["tags"][i]
						)
					}

					load2Elastic(fecha2loadES, INDEX_NAME = indicewrite, TYPE_NAME =	"doc", newesconn = Elasticsearch([ipwrite]))
		
		
		print "Union fechas y leyes finalizado."

	except Exception as e:
		print e
		ha_fallado = True

	import psycopg2 as p

	# Parametros db
	aux_pars_db = pars_db.split("/")
	dbname= aux_pars_db[0]
	dbuser= aux_pars_db[1]
	dbpass= aux_pars_db[2]
	host= aux_pars_db[3]
	port = aux_pars_db[4]
	con_str = str('dbname='+'\''+dbname+'\''+' user='+'\''+dbuser+'\''+' host='+'\''+host+'\''+' password='+'\''+dbpass+'\''+' port='+'\''+port+'\'')
	connection = p.connect(con_str)
	connection.autocommit = True
	cursor = connection.cursor()

	resultado = 200
	if ha_fallado:
		resultado = 500

	# Hacemos update en posgre
	queryUpdate = "update imp_request set nerstate = " + str(resultado) + ", lastchange = localtimestamp where id in (select id from content where " + criterion + " = '" + filename + "')"
	cursor.execute(queryUpdate)
	
	print(filename + " procesado!!")


if utiliza_logs:
	sys.stdout = old_stdout
	log_file.close()

