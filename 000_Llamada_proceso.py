#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Llamada proceso
# python 000_Llamada_proceso.py 172.22.248.206:9229 172.22.248.206:9229 aeacus_kdd pruebas_ner name 04_019-025_14.PDF/04_032-033_2.PDF/02_002-015_28.PDF/03_016-018_6.PDF/03_002-008_14.PDF/02_029-030_2.PDF
# python 000_Llamada_proceso.py 172.22.248.206:9229 172.22.248.206:9229 aeacus_kdd pruebas_ner name 04_019-025_14.PDF/04_032-033_2.PDF


import platform
import os

pais = "esp"
# Possibles valors: esp, bra

idioma = "es"


def reduce_concat(x, sep = ""):
	return(reduce(lambda x, y: x + sep + y, x))

import sys
if platform.system() == "Windows":
	PATH = "D:/Judicial/"
	sys.path.append('C:/Python27/Lib/site-packages')
	eslocal = True
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
		
	eslocal = False

# Cogemos argumentos del sistema
if len(sys.argv) < 7:
	raise Exception("Tiene que haber como minimo 6 argumentos!!\nEjemplo llamada:\n	 python 000_Llamada_proceso.py ip_donde_leer(172.22.248.206:9229) ip_donde_escribir(172.22.248.206:9229) indice_donde_leer indice_donde_escribir criterio(name) fichero_a_procesar1 fichero_a_procesar2")
else:
	ipread = sys.argv[1]
	ipwrite = sys.argv[2]
	indiceread = sys.argv[3]
	indicewrite = sys.argv[4]
	criterion = sys.argv[5]
	files2Eval = sys.argv[6].split("/")
	es_hdfs = sys.argv[7]
	print "Los archivos a evaluar son los siguientes:\n	 '" + reduce(lambda x, y: x + "', '" + y, files2Eval) + "'."

# ipread = '172.22.248.206:9229'
# ipwrite = '172.22.248.206:9229'
# indiceread = 'aeacus_kdd'
# indicewrite = 'pruebas_ner'
# criterion = 'name'
# files2Eval = '04_019-025_14.PDF/04_032-033_2.PDF'.split("/")

# Listamos directorios donde borrar los filenames que se procesan
from os import listdir
from os import remove
INPUTNORMLEYES = PATH + "output/resultados_leyes/"
INPUTNORMFECHA = PATH + "output/resultados_fechas/"
listdirleyes = listdir(INPUTNORMLEYES)
listdirfechas = listdir(INPUTNORMFECHA)
for filename in files2Eval:
	aux_name = filename.lower().replace(".pdf", "")
	[remove(INPUTNORMLEYES + file_del) for file_del in listdirleyes if aux_name in file_del]
	[remove(INPUTNORMFECHA + file_del) for file_del in listdirfechas if aux_name in file_del]


# Eliminamos los documentos de elastic si existen
execfile(PATH + "syntax/scripts/NER/funciones_elastic.py")
for filename in files2Eval:
	eliminaDoc(filename, INDEX_NAME = indicewrite, TYPE_NAME =  "doc", newesconn = Elasticsearch([ipwrite]))


# Llamamos a todos los procesos
print "# === Procesando leyes === #"
ejecucion = "cd " + PATH + "syntax/03_Deteccion_info/019_Implementacion_leyes/; java -cp exe-1.0.jar com.minsait.Boot --run ef shw uz " + ipread + " " + ipwrite + " " + indiceread + " " + indicewrite + " " + criterion + " " + reduce(lambda x, y: x + "/" + y, files2Eval)
# ejecucion = "cd " + PATH + "syntax/03_Deteccion_info/019_Implementacion_leyes/; python 019_Implementacion_leyes.py " + ipread + " " + ipwrite + " " + indiceread + " " + indicewrite + " " + criterion + " " + reduce(lambda x, y: x + "/" + y, files2Eval) + " " + pais + " " + idioma + " " + "False"
os.system(ejecucion)
print "# === Leyes procesado === #"

print "# === Procesando fechas === #"
ejecucion = "cd " + PATH + "syntax/03_Deteccion_info/039_Implementacion_fechas/; java -cp exe-1.0.jar com.minsait.Boot --run ef shw uz " + ipread + " " + ipwrite + " " + indiceread + " " + indicewrite + " " + criterion + " " + reduce(lambda x, y: x + "/" + y, files2Eval)
# ejecucion = "cd " + PATH + "syntax/03_Deteccion_info/039_Implementacion_fechas/; python 039_Implementacion_fechas.py " + ipread + " " + ipwrite + " " + indiceread + " " + indicewrite + " " + criterion + " " + reduce(lambda x, y: x + "/" + y, files2Eval) + " " + pais + " " + idioma + " " + "False"
os.system(ejecucion)
print "# === Fechas procesado === #"

print "# === Procesando cantidades === #"
ejecucion = "cd " + PATH + "syntax/03_Deteccion_info/049_Implementacion_cantidades/; java -cp exe-1.0.jar com.minsait.Boot --run ef shw uz " + ipread + " " + ipwrite + " " + indiceread + " " + indicewrite + " " + criterion + " " + reduce(lambda x, y: x + "/" + y, files2Eval)
# ejecucion = "cd " + PATH + "syntax/03_Deteccion_info/049_Implementacion_cantidades/; python 049_Implementacion_cantidades.py " + ipread + " " + ipwrite + " " + indiceread + " " + indicewrite + " " + criterion + " " + reduce(lambda x, y: x + "/" + y, files2Eval) + " " + pais + " " + idioma + " " + "False"
os.system(ejecucion)
print "# === Cantidades procesado === #"

print "# === Procesando plazos === #"
ejecucion = "cd " + PATH + "syntax/03_Deteccion_info/059_Implementacion_plazos/; java -cp exe-1.0.jar com.minsait.Boot --run ef shw uz " + ipread + " " + ipwrite + " " + indiceread + " " + indicewrite + " " + criterion + " " + reduce(lambda x, y: x + "/" + y, files2Eval)
# ejecucion = "cd " + PATH + "syntax/03_Deteccion_info/059_Implementacion_plazos/; python 059_Implementacion_plazos.py " + ipread + " " + ipwrite + " " + indiceread + " " + indicewrite + " " + criterion + " " + reduce(lambda x, y: x + "/" + y, files2Eval) + " " + pais + " " + idioma + " " + "False"
os.system(ejecucion)
print "# === Plazos procesado === #"

# Ejecutamos proceso que depende de los anteriores
print "# === Procesando union fechas y leyes === #"
from os.path import exists
import time
n_intents = 0
max_intents = 100
completado = False
total_files = len(files2Eval)
files_processed = []
while n_intents <= max_intents and not completado:

	time.sleep(30)
	n_intents += 1
	
	print "Intento " + str(n_intents)
	for filename in files2Eval:
		if ".pdf" in filename.lower():
			files_exists = [INPUTNORMFECHA + filename.lower().replace(".pdf", "_entities.json"), INPUTNORMFECHA + filename.lower().replace(".pdf", "_info_pos.csv"), INPUTNORMLEYES + filename.lower().replace(".pdf", "_info_pos.csv")]
		else:
			files_exists = [INPUTNORMFECHA + filename.lower() + "_entities.json", INPUTNORMFECHA + filename.lower() + "_info_pos.csv", INPUTNORMLEYES + filename.lower() + "_info_pos.csv"]
		
		if all([exists(file_now) for file_now in files_exists]) and filename not in files_processed:
			ejecucion = "cd " + PATH + "syntax/04_Unifica_info/020_Union_fechas_leyes/; java -cp exe-1.0.jar com.minsait.Boot --run ef shw uz " + ipread + " " + ipwrite + " " + indiceread + " " + indicewrite + " " + criterion + " " + filename
			# ejecucion = "cd " + PATH + "syntax/04_Unifica_info/020_Union_fechas_leyes/; python 020_Union_fechas_leyes.py " + ipread + " " + ipwrite + " " + indiceread + " " + indicewrite + " " + criterion + " " + filename + " " + pais + " " + idioma + " " + "False"
			os.system(ejecucion)
			files_processed.append(filename)
			total_files -= 1
	
	if total_files <= 0:
		completado = True
	
print "# === Union fechas y leyes procesado === #"




