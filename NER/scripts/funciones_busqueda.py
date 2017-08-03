# -*- coding: utf-8 -*-

# Funciones busqueda texto
# Llamame:
#   execfile(PATH + 'syntax/scripts/funciones_busqueda.py')

# Cargamos funciones y librerias necesarias
# execfile(PATH + 'syntax/scripts/NER/funciones_tratamiento_texto.py')
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

  
def countSepBetweenWords(word1, word2, texto, separador = " "):
  # Esta funcion cuenta la separacion entre 2 palabras en un texto
  auxword1 = word1.replace(separador, "")
  auxword2 = word2.replace(separador, "")
  if word1 != auxword1:
    texto = texto.replace(word1, auxword1)
    word1 = auxword1
  if word2 != auxword2:
    texto = texto.replace(word2, auxword2)
    word2 = auxword2

  totalPos = posWords(texto, word1)
  n1 = len(totalPos)
  posWord2 = posWords(texto, word2)
  n2 = len(posWord2)
  totalPos.extend(posWord2)
  totalPos = np.array(totalPos)

  auxWord1 = np.repeat(word1, n1)
  auxWord2 = np.repeat(word2, n2)
  totalAuxWord = list(auxWord1)
  totalAuxWord.extend(auxWord2)
  totalAuxWord = np.array(totalAuxWord)

  auxOrder = np.argsort(totalPos)
  orderedWords = list(totalAuxWord[auxOrder])
  orderedPos = list(totalPos[auxOrder])

  result = []
  for i in range(1, len(orderedWords)):
    if orderedWords[i] != orderedWords[i - 1]:
      auxText = texto[orderedPos[i - 1]:orderedPos[i]]
      numSeps = len(auxText) - len(auxText.replace(separador, ""))
      result.append(numSeps)
	
  return(result)

  
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

## Ejemplo
## text_minus = texto.lower()
## 
## # Creamos diccionario de palabras interesantes
## dictInterestingWords = dict(
##   ley = dict(complete_word = ['ley']),
##   articulo = dict(complete_word = ['articulo', 'art.'], reg_exp = ['[\s]art.[a-z0-9]+']),
##   jurisprudencia = dict(complete_word = ['jurisprudencia']),
##   fechas_num = dict(reg_exp = ['[\s][0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9][\s]', '[\s][0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9][\s]', '[\s][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9][\s]', '[\s][0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][\s]']),
##   fechas_mes = dict(complete_word = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'setiembre', 'octubre', 'noviembre', 'diciembre']),
##   fechas_vete_a_saber = dict(reg_exp = ['[\s][0-9][0-9].+[\s]']),
##   entidades = dict(reg_exp = ['[\s]entidad[es]?', '[\s]organizacion[es]?', '[\s]asociacion[es]?', '[\s]compania[s]?'])
##   demandante
##   demandado
##   tipo_documento = ['recurso']
## )
## posiciones = buscaDiccionarioTexto(dictInterestingWords, text_minus)

def extraerNombresPropios(texto, lang = 'spanish'):

  # Sacamos lo que es cada palabra
  # prueba = spa_bigram_tagger.tag(texto.split())
  chunked = ne_chunk(pos_tag(word_tokenize(texto)))

  result = []
  for ichunk in chunked:
    if type(ichunk) == Tree:
      result.append(ichunk)

  # Nos creamos vector de tagged words para entrenar, filtramos algunas particularidades para conseguir mas robustez
  stop_words = stopwords.words(lang)
  for i in range(len(stop_words)):
    stop_words[i] = remove_accents(stop_words[i])

  tagged_words = []
  long_total = len(texto)
  for item in result:
    pasa = True
    if len(item) == 1:
      long_sin_word = len(texto.replace(item[0][0], ""))
      if len(re.findall('\.[\s]' + item[0][0], texto)) == (long_total - long_sin_word)/len(item[0][0]):  # Si todas las apariciones de esta palabra son despues de un signo de puntuacion
        pasa = False
      if len(re.findall('[\n]+[\s]?' + item[0][0], texto)) == (long_total - long_sin_word)/len(item[0][0]):  # Si todas las apariciones de esta palabra son despues de un salto de linea
        pasa = False
      if item[0][0].lower() in stop_words:  # Si es un stopword
        pasa = False

    if pasa:
      tagged_words.append([])
      for word in item:
        tagged_words[len(tagged_words) - 1].append(tuple([word[0], item.label()]))

  return tagged_words

  
import itertools

def get_distance(w1, w2, texto):
  words = texto.split(" ")
  if w1 in words and w2 in words:
    w1_indexes = [index for index, value in enumerate(words) if value == w1]    
    w2_indexes = [index for index, value in enumerate(words) if value == w2]    
    distances = [abs(item[0] - item[1]) for item in itertools.product(w1_indexes, w2_indexes)]
    return min(distances)
  
