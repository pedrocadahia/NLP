# -*- coding: utf-8 -*-

# Funciones tratamiento de texto
# Llamame:
#		execfile(PATH + 'syntax/scripts/funciones_tratamiento_texto.py')

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

