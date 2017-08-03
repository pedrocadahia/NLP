#!/usr/bin/env python
# -*- coding: utf-8 -*-

# execfile(PATH + "syntax/03_Deteccion_info/059_Implementacion_plazos.py")



if __name__ == "__main__":

	guardaResultados = True
	muestraHTML = True
	guardaES = True
	es_hdfs = False
	
	import sys
	import platform

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
	from os import listdir
	import numpy as np
	from os.path import exists
	import pandas as pd
	from math import factorial
	import gensim
	import json
	from copy import deepcopy

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
	OUTPUTDIR = PATH + "output/resultados_plazos/"
	if eslocal:
		INPUTREAD = PATH + "input/documentos_expedientes/"
		files2Eval = listdir(INPUTREAD)
	else:
		try:
			files2Eval
		except NameError:
			files2Eval = list_docs(criterion = criterion, con = Elasticsearch([ipread]), indice = indiceread)

	# Definimos expresiones regulares de las cantidades
	DictPaisSearchMaxEnrere = dict(
		esp = [ 
			dict(contiene = ['\\bmes\\b', '\\bmeses\\b', '\\bdia\\b', '\\bdias\\b', '\\bano\\b', '\\banos\\b', '\\bsemana'], 
					 # inicios = ['[0-9]+', "\\buno", "\\bdos", "\\btres", "\\bcuatro", "\\bcinco", "\\bseis", "\\bsiete", "\\bocho", "\\bnueve", "\\bdiez", "\\bonce", "\\bdoce", "\\btrece", "\\bcatorce", "\\bquince", "\\bdieciseis", "\\bdiecisiete", "\\bdieciocho", "\\bdiecinueve", "\\bveinte", "\\bventi", "\\bveinti", "\\btreinta", "\\btrenta", "\\bcuarenta", "\\bcincuenta", "\\bsesenta", "\\bochenta", "\\bnoventa", "\\bcien", "\\bdoscientos", "\\btrescientos", "\\bcuatrocientos", "\\bquinientos", "\\bseiscientos", "\\bsetecientos", "\\bochocientos"],
					 inicios = ['pena', "plazo", "condena", "multa", "beneficio de la comunidad", "beneficio a la comunidad", "sancion"],
					 finales = ['\\bmes\\b', '\\bmeses\\b', '\\bdia\\b', '\\bdias\\b', '\\bano\\b', '\\banos\\b', '\\bsemana']
			)
		],
		bra = [ 
			dict(contiene = ['\\bmes\\b', '\\bmeses\\b', '\\bdia\\b', '\\bdias\\b', '\\bano\\b', '\\banos\\b', '\\bsemana'], 
					 # inicios = ['[0-9]+', "\\buno", "\\bdos", "\\btres", "\\bcuatro", "\\bcinco", "\\bseis", "\\bsiete", "\\bocho", "\\bnueve", "\\bdiez", "\\bonce", "\\bdoce", "\\btrece", "\\bcatorce", "\\bquince", "\\bdieciseis", "\\bdiecisiete", "\\bdieciocho", "\\bdiecinueve", "\\bveinte", "\\bventi", "\\bveinti", "\\btreinta", "\\btrenta", "\\bcuarenta", "\\bcincuenta", "\\bsesenta", "\\bochenta", "\\bnoventa", "\\bcien", "\\bdoscientos", "\\btrescientos", "\\bcuatrocientos", "\\bquinientos", "\\bseiscientos", "\\bsetecientos", "\\bochocientos"],
					 inicios = ['pena', "prazo", "conviccao", "fino", "sancao", "sancoes"],
					 finales = ['\\bmes\\b', '\\bmeses\\b', '\\bdia\\b', '\\bdias\\b', '\\bano\\b', '\\banos\\b', '\\bsemana']
			)
		]
	)
	
	SearchMaxEnrere = DictPaisSearchMaxEnrere[pais]
		
	limSimFrases = 0.15	# Limit a partir del qual diem que 2 frases s'assemblen

	guardaES = guardaES and not eslocal  # Solo guardamos a elastic si no se ejecuta desde local
	
	for filename in files2Eval:

		print("Procesando " + filename + "...")
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
					selFin = listFinal[:, 1] < 50
					if any(selFin):
						plazos.append((min(listInicios[sel, 0]), item[0] + max(listFinal[selFin, 1])))
					else:
						plazos.append((min(listInicios[sel, 0]), item[0] + min(listFinal[:, 1])))
					
			
		# Buscamos registros unicos
		plazos = list(set(plazos))
		
		if len(plazos) == 0:
			print "No hemos encontrado plazos en este documento"
			continue
			
		# Regulamos que no se incluyan unos dentro de otros los textos
		plazos = pd.DataFrame(plazos)
		plazos = plazos.sort(0)
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
						
		# Reescribimos fichero posiciones con las referencias normalizadas
		for ikey in result.keys():
			result[ikey]["descripcion"] = result[ikey]["descripcion"].decode("latin1")
			for i in range(len(result[ikey]["referencias"])):
				result[ikey]["referencias"][i] = result[ikey]["referencias"][i].decode("latin1")
				result[ikey]["posiciones"]["inicio"][i] = str(result[ikey]["posiciones"]["inicio"][i])
				result[ikey]["posiciones"]["fin"][i] = str(result[ikey]["posiciones"]["fin"][i])
		
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
						pos_inicial = ddNorm["posRawDocIni"][i],
						pos_final = ddNorm["posRawDocFin"][i],
						texto = ddNorm["Referencia"][i],
						texto_norm = ddNorm["Referencia_Normalizada"][i],
						contexto = result[ddNorm["Referencia_Normalizada"][i]]["descripcion"]
					)
				}

				load2Elastic(dictloadES, INDEX_NAME = indicewrite, TYPE_NAME =  "doc", newesconn = Elasticsearch([ipwrite]))
		
		
		print(filename + " procesado!!")
