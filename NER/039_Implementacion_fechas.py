#!/usr/bin/env python
# -*- coding: utf-8 -*-

# execfile(PATH + "syntax/03_Deteccion_info/039_Implementacion_fechas.py")
# execfile(PATH + "syntax/99_Implementacion/039_Implementacion_fechas.py")

if __name__ == "__main__":

	# Parametros
	guardaResultados = True
	muestraHTML = True
	guardaES = True
	es_hdfs = False

	import platform
	import sys

	if platform.system() == "Windows":
		PATH = "D:/Judicial/"
		sys.path.append('C:/Python27/Lib/site-packages')
		eslocal = True
		try:
			pais
		except NameError:
			pais = "esp"
	else:
		PATH = "/home/dcortiada/"
		eslocal = False
		if len(sys.argv) < 7:
			raise Exception("Tiene que haber como minimo 6 argumentos!!\nEjemplo llamada:\n	 python 019_Implementacion_leyes.py ip_donde_leer(172.22.248.206:9229) ip_donde_escribir(172.22.248.206:9229) indice_donde_leer indice_donde_escribir criterio(name) fichero_a_procesar1 fichero_a_procesar2")
		else:
			ipread = sys.argv[1]
			ipwrite = sys.argv[2]
			indiceread = sys.argv[3]
			indicewrite = sys.argv[4]
			criterion = sys.argv[5]
			files2Eval = sys.argv[6].split("/")
#			pais = sys.argv[7]
#			idioma = sys.argv[8]
#			es_hdfs = sys.argv[9]
			pais = "esp"
			idioma = "es"
			es_hdfs = False
			print "Los archivos a evaluar son los siguientes:\n	 '" + reduce(lambda x, y: x + "', '" + y, files2Eval) + "'."

		if es_hdfs == "True":
			es_hdfs = True
		else:
			es_hdfs = False
			
		if es_hdfs:
			PATH = "hdfs:///user/ijdocs/"
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
		
	# Cargamos funciones
	if not es_hdfs:
		execfile(PATH + "syntax/scripts/NER/carga_todas_funciones.py")
	else:
		filename = PATH + "syntax/scripts/NER/funciones_comparacion_texto.py"
		sc.addPyFile(filename)
		from funciones_comparacion_texto import *
		filename = PATH + "syntax/scripts/NER/funciones_elastic.py"
		sc.addPyFile(filename)
		from funciones_elastic import *
		filename = PATH + "syntax/scripts/NER/funciones_transforma_refs.py"
		sc.addPyFile(filename)
		from funciones_transforma_refs import *
		filename = PATH + "syntax/scripts/NER/funciones_busqueda.py"
		sc.addPyFile(filename)
		from funciones_busqueda import *
		filename = PATH + "syntax/scripts/NER/funciones_preprocesado.py"
		sc.addPyFile(filename)
		from funciones_preprocesado import *
		filename = PATH + "syntax/scripts/NER/funciones_tratamiento_texto.py"
		sc.addPyFile(filename)
		from funciones_tratamiento_texto import *
		filename = PATH + "syntax/scripts/NER/funciones_lectura_datos.py"
		sc.addPyFile(filename)
		from funciones_lectura_datos import *

	# Cargamos librerías
	from os import listdir
	from os.path import exists
	from numpy import array
	from numpy import ndarray
	import pandas as pd
	from copy import deepcopy
	from math import factorial
	import gensim
	import json
	from pattern.es import parsetree

	# Listamos ficheros a tratar
	if eslocal:
		INPUTREAD = PATH + "input/documentos_expedientes/"
		files2Eval = listdir(INPUTREAD)
	else:
		try:
			files2Eval
		except NameError:
			files2Eval = list_docs(criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)

	OUTPUTDIR = PATH + "output/resultados_fechas/"

	# Definimos parámetros
	DictPaisCompleteStrSearch = dict(
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
		],
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

	CompleteStrSearch = DictPaisCompleteStrSearch[pais]

	limSimFrases = 0.15	 # Limit a partir del qual diem que 2 frases s'assemblen


	for filename in files2Eval:

		print("Procesando " + filename + "...")

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

		# Regulamos que no se incluyan unos dentro de otros los textos
		fechas = pd.DataFrame(fechas)
		fechas = fechas.sort(0)
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
							elif i_item in DictSubsMeses[pais].keys():
								i_item = DictSubsMeses[pais][i_item]
							item.append(i_item)
						item = reduce_concat(item, sep = " ")
					elif item in DictSubsMeses[pais].keys():
						item = DictSubsMeses[pais][item]
					else:
						if len(re.findall('[a-z]', item)) > 0:
							try:
								item = transformaNumsString(item)
							except:
								if any([ikey in item for ikey in DictSubsMeses[pais].keys()]):
									for ikey in DictSubsMeses[pais].keys():
										item = item.replace(ikey, DictSubsMeses[pais][ikey])
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
						for ikey in DictSubsMeses[pais].keys():
							aux_normorig = aux_normorig.replace(ikey, DictSubsMeses[pais][ikey])
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
				auxRes = comparaFrases(frases, fileMod = PATH + "data/modelos_NER/DS_RNN_2014")
				gruposUnidos = unirGrupos(list(auxRes["item1"]), list(auxRes["item2"]), list(auxRes["valor_comp"] > limSimFrases))

				# Extraiem les frases mes rellevants per a cada grup
				frasesGrup = []
				for grupo in gruposUnidos:
					frases_grupo_act = [""]
					for element in grupo:
						frases_grupo_act[0] = frases_grupo_act[0] + " " + frases[element]
						frases_grupo_act.append(frases[element])
					
					auxRes = comparaFrases(frases_grupo_act, fileMod = PATH + "data/modelos_NER/DS_RNN_2014", totesContraTotes = False)
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
			ddNorm["Antes"][irow] = readedFile[(ddNorm["PosInicio"][irow] - x):ddNorm["PosInicio"][irow]]
			aux_text = readedFile[(ddNorm["PosFin"][irow] - 1):(ddNorm["PosFin"][irow] + x)]
			if len(aux_text) > 0:
				if aux_text[0] == ddNorm["Referencia"][irow][-1]:
					ddNorm["Despues"][irow] = aux_text[1:]
				else:
					ddNorm["Despues"][irow] = aux_text
			else:
				ddNorm["Despues"][irow] = ""
			
			
		# Pasamos a frases los textos de antes y despues
		ddNorm["Contexto"] = ""
		for irow in range(ddNorm.shape[0]):
			ddNorm["Antes"][irow] = ddNorm["Antes"][irow].replace("\n", " ")
			ddNorm["Despues"][irow] = ddNorm["Despues"][irow].replace("\n", " ")
			auxa = sentences(ddNorm["Antes"][irow])
			auxa = auxa.split(" .\n")
			ddNorm["Antes"][irow] = auxa[len(auxa) - 1]
			auxd = sentences(ddNorm["Despues"][irow])
			auxd = auxd.split(" .\n")
			ddNorm["Despues"][irow] = auxd[0]
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
			ddNorm.to_csv(path_or_buf = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_info_pos.csv", sep = "|")
			fileOut = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_entities.json"
			with open(fileOut, 'w') as fp:
					json.dump(result, fp)
		
		if muestraHTML:
			dataJSON = deepcopy(result)
			name_file = filename.lower().replace(".pdf", "")
			
			ddNorm['order'] = [sum(ddNorm['Referencia_Normalizada'] == ddNorm['Referencia_Normalizada'][i]) for i in xrange(len(ddNorm))]
			ddNorm = ddNorm.sort("order", ascending = False)
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
		
		print(filename + " procesado!!")

