#!/usr/bin/env python
# -*- coding: utf-8 -*-

# execfile(PATH + "syntax/04_Unifica_info/020_Union_fechas_leyes.py")
# execfile(PATH + "syntax/99_Implementacion/020_Union_fechas_leyes.py")

if __name__ == "__main__":

	guardaResultados = True
	muestraHTML = True
	guardaES = True
	es_hdfs = False
	nuevas_leyes = False
	
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
		if len(sys.argv) < 6:
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
		
	# Cargamos librerias y funciones
	import json
	from os.path import exists
	import pandas as pd

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

	# Definimos parametros
	OUTPUTWRITE = PATH + "output/uni_fechas_ley/"
	INPUTNORMLEYES = PATH + "output/resultados_leyes/"
	INPUTNORMFECHA = PATH + "output/resultados_fechas/"
	if eslocal:
		from os import listdir
		INPUTREAD = PATH + "input/documentos_expedientes/"
		files2Eval = listdir(INPUTREAD)
	else:
		try:
			files2Eval
		except NameError:
			files2Eval = list_docs(criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)

	limLenDist = 20

	guardaES = guardaES and not eslocal	 # Solo guardamos a elastic si no se ejecuta desde local

	# Leemos y buscamos en los archivos
	for filename in files2Eval:

		print("Procesando " + filename + "...")
		if eslocal:
			RawReadedFile = pdf2txt(INPUTREAD + filename)
		else:
			RawReadedFile = import_doc(filename, criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)
		readedFile = remove_accents(RawReadedFile)
		readedFile = readedFile.lower()
		
		if ".pdf" in filename.lower():
			filejsonfecha = INPUTNORMFECHA + filename.lower().replace(".pdf", "_entities.json")
			filenormleyes = INPUTNORMLEYES + filename.lower().replace(".pdf", "_info_pos.csv")
			filenormfecha = INPUTNORMFECHA + filename.lower().replace(".pdf", "_info_pos.csv")
		else:
			filejsonfecha = INPUTNORMFECHA + filename.lower() + "_entities.json"
			filenormleyes = INPUTNORMLEYES + filename.lower() + "_info_pos.csv"
			filenormfecha = INPUTNORMFECHA + filename.lower() + "_info_pos.csv"
		
		if not exists(filenormleyes) or not exists(filenormfecha):
			print "No tenim detectada la informacio pel document " + filename + ", per tant no el processem."
			continue
			
		dataNormLeyes = pd.read_csv(filenormleyes, sep = "|")
		dataNormFecha = pd.read_csv(filenormfecha, sep = "|")

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
		# === Normalizamos leyes y articulos === #
		# ====================================== #
		
#		char_space = ["\r\n", "\n", ".", ",", "'", ":", '"']
#		for irow in range(dataNormLeyes.shape[0]):
#			for ch_sp in char_space:
#				dataNormLeyes["Referencia"][irow] = dataNormLeyes["Referencia"][irow].replace(ch_sp, " ")
#			dataNormLeyes["Referencia"][irow] = quitaDoblesEspacios(dataNormLeyes["Referencia"][irow])
#		
#		
#		paraules_art = ["art ", "articulo"]
#		regexSegueix = ["[0-9]", "y", "[\b]", "no", "num"]
#		dict_lleis_arts = {}
#		
#		for irow in range(dataNormLeyes.shape[0]):
#			if any([par_art in dataNormLeyes["Referencia"][irow] for par_art in paraules_art]):
#				todas_pos = []
#				for par_art in paraules_art:
#					todas_pos.extend(buscaPosicionRegexTexto(par_art, dataNormLeyes["Referencia"][irow]))
#				
#				nl = []
#				for item in todas_pos:
#					aux_t = dataNormLeyes["Referencia"][irow][item[1]:]
#					posregex = []
#					for regseg in regexSegueix:
#						posregex.extend(buscaPosicionRegexTexto(regseg, aux_t))
#					
#					maxnum = 0
#					for i in range(1, len(posregex))
#					
#					nl.append(item[0], )
		
		
		
		
		
		
		# ====================================== #
		# === Sacamos contexto para cada ley === #
		# ====================================== #
		

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
		
		# Seguir amb aixo!
		
		# # Leemos archivo de leyes
		# if nuevas_leyes:
		# 	pass
		# 
		# # Escribimos archivo con leyes por validar
		# filenameleyes = PATH + "data/leyes_valida.csv"
		# with open(filenameleyes, mode = "a+") as fw:
		# 	for irow in range(dataNormLeyes.shape[0]):
		# 		fw.write(dataNormLeyes['Referencia'][irow].replace(";", ",") + ";" + 
		# 			dataNormLeyes['Despues'][irow].replace(";", ",") + ";" + 
		# 			"-1" + ";" + "\n"
		# 		)
		
		
		
		# ============================= #
		# === Escribimos resultados === #
		# ============================= #
		
		if guardaResultados:
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
			
		if muestraHTML:
			dataNormLeyes["mark"] = 'mark01'
			dataNormFecha["mark"] = 'mark02'
			dataWriteHTML = dataNormLeyes[["PosInicio", "PosFin", "mark"]]
			dataWriteHTML = dataWriteHTML.append(dataNormFecha[["PosInicio", "PosFin", "mark"]])
			dataWriteHTML = dataWriteHTML.sort(["PosInicio"])
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
			
			# Buscamos el texto en el rawDoc
			dataNormLeyes["posRawDocIni"] = -1
			dataNormLeyes["posRawDocFin"] = -1
			for i in range(dataNormLeyes.shape[0]):
				texto = dataNormLeyes["Referencia"][i]
				text2search = rawDoc[dataNormLeyes["PosInicio"][i]:]
				text2search = remove_accents(text2search)
				text2search = text2search.lower()
				sumposini = text2search.index(texto)
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
						pos_inicial = dataNormLeyes["posRawDocIni"][i],
						pos_final = dataNormLeyes["posRawDocFin"][i],
						texto = dataNormLeyes["Referencia"][i],
						contexto = str(dataNormLeyes["Antes"][i]) + str(dataNormLeyes["Referencia"][i]) + str(dataNormLeyes["Despues"][i])
					)
				}
				
				load2Elastic(ley2loadES, INDEX_NAME = indicewrite, TYPE_NAME =  "doc", newesconn = Elasticsearch([ipwrite]))
				
			
			# ============== #
			# === fechas === #
			# ============== #
			
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
						pos_inicial = dataNormFecha["posRawDocIni"][i],
						pos_final = dataNormFecha["posRawDocFin"][i],
						texto = dataNormFecha["Referencia"][i],
						texto_norm = dataNormFecha["Referencia_Normalizada"][i].split("_")[0],
						contexto = dataNormFecha["Contexto"][i],
						tags = dataNormFecha["tags"][i]
					)
				}

				load2Elastic(fecha2loadES, INDEX_NAME = indicewrite, TYPE_NAME =  "doc", newesconn = Elasticsearch([ipwrite]))
		
		print(filename + " procesado!!")
		
		