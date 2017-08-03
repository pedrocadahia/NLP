#!/usr/bin/env python
# -*- coding: utf-8 -*-

if __name__ == "__main__":

	# execfile(PATH + "syntax/03_Deteccion_info/019_Implementacion_leyes.py")
	# execfile(PATH + "syntax/99_Implementacion/019_Implementacion_leyes.py")
	
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
'''
New
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
				PATH = currDir

		if not tenimDirectori:
			auxdir = os.getcwd().split("/")
			vecLog = [item == "syntax" for item in auxdir]
			
			if any(vecLog):
				i = 0
				vecDir = []
				while not vecLog[i]:
					vecDir.append(auxdir[i])
					i += 1
					
				PATH = reduce_concat(vecDir, sep = "/")
			
			else:
				PATH = os.getcwd()
'''
		eslocal = False
		if len(sys.argv) < 7:
			raise Exception("Tiene que haber como minimo 6 argumentos!!\nEjemplo llamada:\n  python 019_Implementacion_leyes.py ip_donde_leer(172.22.248.206:9229) ip_donde_escribir(172.22.248.206:9229) indice_donde_leer indice_donde_escribir criterio(name) fichero_a_procesar1 fichero_a_procesar2")
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
# ipread = '172.22.248.206:9229'
# ipwrite = '172.22.248.206:9229'
# indiceread = 'aeacus_kdd'
# indicewrite = 'pruebas_ner'
# criterion = 'name'
# files2Eval = '04_019-025_14.PDF/04_032-033_2.PDF'.split("/")
# es_hdfs = False
# pais = "es"
			print "Los archivos a evaluar son los siguientes:\n  '" + reduce(lambda x, y: x + "', '" + y, files2Eval) + "'."
		
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
		

	# Cargamos librerias y funciones
	from os.path import exists
	from os import listdir
	from numpy import array
	from numpy import ndarray
	from datetime import datetime
	import pandas as pd
	from nltk.corpus import stopwords
	from copy import deepcopy

	# Listamos ficheros a tratar
	if eslocal:
		INPUTREAD = PATH + "input/documentos_expedientes/"
		files2Eval = listdir(INPUTREAD)
	else:
		try:
			files2Eval
		except NameError:
			files2Eval = list_docs(criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)

	OUTPUTDIR = PATH + "output/resultados_leyes/"
	
	# Prova1: No funciona
	
	# try:
	# 	from hdfs import InsecureClient
	# 	client = InsecureClient('http://localhost:22', user='ijdocs')
	# 	from hdfs import Config
	# 	client = Config().get_client('dev')
	# 	
	# 	with client.write(PATH + "output/prova.json", encoding='utf-8') as writer:
	# 		from json import dump
	# 		dump(dict(a=1, b=2, c=3), writer)
	# 	
	# 	print 1
	# 	
	# except Exception as e:
	# 	print e

	# Prova2: Funciona escriptura però no lectura
	
	#	try:
	#		rdd = sc.parallelize([("d", 4), ("c", 5), ("k", 6)])
	#		filename = PATH + "output_test/prueba.txt"
	#		rdd.saveAsTextFile(filename)
	#	except Exception as e:
	#		print e
	#		# tempFile3 = NamedTemporaryFile(delete=True)
	#		# tempFile3.close()
	#		# print tempFile3.name
	#		# codec = "org.apache.hadoop.io.compress.GzipCodec"
	#		# sc.parallelize(['foo', 'bar']).saveAsTextFile(tempFile3.name, codec)
	#
	#	try:
	#		loadedFile = sc.hadoopFile(filename, "org.apache.hadoop.mapred.TextInputFormat", "org.apache.hadoop.io.Text", "org.apache.hadoop.io.LongWritable")
	#		print loadedFile
	#		print loadedFile["d"]
	#	except Exception as e:
	#		print e
	

	# artigo 3.o o artigo 28.o do codigo do imposto sobre o valor acrescentado

	DictPaisCompleteStrSearch = dict(
		esp = [
			dict(contiene = ['[\s|\n]art\.', '[\s|\n]ley[\s|\n]', 'articulo'], inicios = ['parrafo', 'ley', '[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado'], finales = [' en ', '\(', '((?<!art)\.[\s|\n])', ',[\s|\n]']),
	#			dict(contiene = ['[\s|\n]art\.', '[\s|\n]ley[\s|\n]', 'articulo'], inicios = ['parrafo', 'ley', '[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado'], finales = [' en ', '\(', '((?<!art)\.[\s|\n])', ',[\s|\n]', '[\s|\n]constitucion[\s|\n]', '[\s|\n]ce[\s|\n]']),
			dict(contiene = ['[\s|\n]acuerdo[\s|\n]', '[\s|\n]boe[\s|\n]'], inicios = ['acuerdo', '[\s|\n]boe[\s|\n]'], finales = ['[\s|\n]boe[\s|\n]', ' en ', '\(', '((?<!art)\.[\s|\n])', ',[\s|\n]']),
			dict(contiene = ['[\s|\n]real[\s|\n]decreto[\s|\n]', '[\s|\n]decreto[\s|\n]ley[\s|\n]', '[\s|\n]decreto-ley[\s|\n]', '[\s|\n]real-decreto[\s|\n]'], inicios = ['[\s|\n]real[\s|\n]decreto'], finales = [' en ', '\(', '((?<!art)\.[\s|\n])', ',[\s|\n]']),
			dict(contiene = ['reglamento'], inicios = ['[\s|\n][0-9]', '[\s|\n]art\.', '[\s|\n]articulo'], finales = [' en ', '\(', '((?<!art)\.[\s|\n])', ',[\s|\n]']),
			dict(contiene = ['\(art\.', '\(articulo'], inicios = ['\(art\.', '\(articulo'], finales = ["\)"])
		],
		bra = [
			dict(contiene = ['[\s|\n]art\.', '[\s|\n]lei[\s|\n]', 'artigo'], inicios = ['lei', '[\s|\n]art\.', '[\s|\n]artigo', '[\s|\n]paragrafo'], finales = [',[\s|\n]', ';', '\-\\b']),
			dict(contiene = ['[\s|\n]decretos[\s|\n]lei[\s|\n]', '[\s|\n]decreto[\s|\n]lei[\s|\n]', '[\s|\n]decreto-lei[\s|\n]', '[\s|\n]decretos-lei[\s|\n]'], inicios = ['[\s|\n]decretos[\s|\n]lei[\s|\n]', '[\s|\n]decreto[\s|\n]lei[\s|\n]', '[\s|\n]decreto-lei[\s|\n]', '[\s|\n]decretos-lei[\s|\n]'], finales = [',[\s|\n]', ';', '[0-9][0-9][0-9][0-9]']),
			dict(contiene = ['regulacao'], inicios = ['[\s|\n][0-9]', '[\s|\n]art\.', '[\s|\n]artigo'], finales = [',[\s|\n]', ';', '\-\\b']),
			dict(contiene = ['\(art\.', '\(artigo'], inicios = ['\(art\.', '\(artigo'], finales = ["\)"])
		]
	)

	CompleteStrSearch = DictPaisCompleteStrSearch[pais]


	DictPaisSearchMaxEnrere = dict(
    esp = [
        dict(contiene = ['[\s|\n]art\.', '[\s|\n]ley[\s|\n]', 'articulo'], inicios = ['ley', '[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado', '[\s|\n]parrafo'], finales = ['[0-9][0-9][0-9][0-9]']),
        dict(contiene = ['[\s|\n]art\.', '[\s|\n]ley[\s|\n]', 'articulo'], inicios = ['ley', '[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado', '[\s|\n]parrafo'], finales = ['codigo penal', 'cp', '^l[a-z]+', '[\s|\n]constitucion[\s|\n]', '[\s|\n]ce[\s|\n]']),
        dict(contiene = ['[\s|\n]presupuestos[\s|\n]generales[\s|\n]del[\s|\n]estado[\s|\n]'], inicios = ['ley', '[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado', '[\s|\n]parrafo'], finales = ['[\s|\n]presupuestos[\s|\n]generales[\s|\n]del[\s|\n]estado[\s|\n]']),
        dict(contiene = ['[\s|\n]art\.', 'articulo'], inicios = ['[\s|\n]art\.', '[\s|\n]articulo', '[\s|\n]apartado', '[\s|\n]parrafo'], finales = ['[\s|\n]constitucion[\s|\n]espanola[\s|\n]', '[\s|\n]constitucion[\s|\n]', '[\s|\n]ce[\s|\n]'])
    ],
		bra = [
			dict(contiene = ['[\s|\n]art\.', '[\s|\n]lei[\s|\n]', 'artigo'], inicios = ['lei', '[\s|\n]art\.', '[\s|\n]artigo'], finales = ['[0-9][0-9][0-9][0-9]']),
			dict(contiene = ['[\s|\n]art\.', '[\s|\n]lei[\s|\n]', 'artigo'], inicios = ['lei', '[\s|\n]art\.', '[\s|\n]artigo'], finales = ['\\bcodigo\\b', '[\s|\n]constituicao[\s|\n]', '[\s|\n]ce[\s|\n]', '\.o', 'lei'])
		]
	)

	SearchMaxEnrere = DictPaisSearchMaxEnrere[pais]

	# Leemos y buscamos en los archivos
	for filename in files2Eval:

		print("Procesando " + filename + "...")
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
		
		# Regulamos que no se incluyan unos dentro de otros los textos
		leyes = pd.DataFrame(leyes)
		leyes = leyes.sort(0)
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
			if pais == "bra":
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
					elif pais == "esp" and ('ley' in textocambia[i] or 'real decreto' in textocambia[i] or 'articulo' in textocambia[i] or 'acuerdo' in textocambia[i] or 'presupuesto' in textocambia[i] or any([ikey in textocambia[i] for ikey in DictSubsMeses[pais].keys()])):
						resultado = reduce_concat(textocambia[:(i + 1)], sep = ".")
					elif pais == "bra" and ('lei' in textocambia[i] or 'artigos' in textocambia[i] or 'constituicao' in textocambia[i] or 'codigo' in textocambia[i] or len(re.findall('^o\\b', textocambia[i])) > 0 or len(re.findall('\\b[0-9][0-9]/[0-9][0-9]\\b', textocambia[i])) > 0 or len(re.findall('/[0-9][0-9][0-9][0-9]\\b', textocambia[i])) > 0 or any([ikey in textocambia[i] for ikey in DictSubsMeses[pais].keys()])):
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
			listRefs.to_csv(path_or_buf = OUTPUTDIR + filename.lower().replace(".pdf", "") + "_info_pos.csv", sep = "|")
		
		if muestraHTML:
			# Ordenamos para que los que estan mas repetidos tengan colores
			listRefs['order'] = [sum(listRefs['RefNorm'] == listRefs['RefNorm'][i]) for i in xrange(len(listRefs))]
			listRefs = listRefs.sort("order", ascending = False)
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
			
		print(filename + " procesado!!")
	
