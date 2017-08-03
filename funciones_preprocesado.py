# -*- coding: utf-8 -*-

# Funciones pre-procesado texto
# Llamame:
#		execfile(PATH + 'syntax/scripts/funciones_preprocesado.py')

# Cargamos funciones y librerias necesarias
# execfile(PATH + 'syntax/scripts/NER/funciones_tratamiento_texto.py')
import unicodedata
from nltk.corpus import stopwords

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

