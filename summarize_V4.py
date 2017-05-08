#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Conexion y Consulta elastisearch
import sys

from elasticsearch import Elasticsearch
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

if len(sys.argv) != 4:
    raise Exception(" No se han introducido todos los argumentos requeridos o se han introducido de manera incorrecta"
                    " 1. Arg 1: Codigo de conexion. IP. Ej: 172.22.248.206:9229"
                    " 2. Arg 2: Indice de donde se carga el archivo"
                    " 3. Arg 3: Nombre del documento o identificador. Ej: 03_002-008_14.PDF"
                    " 4. Arg 4: Nombre del criterio de busqueda, Ej: name, uuid o uri..."
                    )


def conn_query(es__host, indice, doc, criterion):
    # conexion a elasticsearch
    try:
        es = Elasticsearch([es__host])
    except 'connection error':
        print ("CONNECTION ERROR")
    try:
        # revisar indice para consulta. index="aeacus_kdd". uri:aeacus:fs#9e33cbdbd42b4ec98b1e8d2080d64ed4
        r = es.search(index=indice, doc_type="document",
                      body={"size": 1000, "query": {"match": {criterion: doc}}},
                      sort="page")
        txt1 = ""
        txt2 = []
        for doc in r['hits']['hits']:
            txt1 = txt1 + doc['_source']['content']
            txt2.append((doc['_source']['page'], doc['_source']['content']))
        return txt1, txt2
    except 'Warning':
        raise Exception("la consulta no se ha realizado de manera adecuada")


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


def del_page_num(txt):
    import re
    aux = re.findall(r"\b(\d{1,2}/\d{1,2})\b", txt)
    pattern = re.compile(r"\b(" + "|".join(aux) + ")\\W", re.I)
    return pattern.sub("", txt)


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
    return reduce(lambda s, y: s + sep + y, x)


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


def process_text(txt, outstopwords=True, outpunkt=True):
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
        txt = txt.replace('  ', ' ')  # Quitamos dobles espacios
        #    txt = txt.replace('. ', '.')  # Quitamos espacios despues de puntos
        #    txt = txt.replace(' .', '.')  # Quitamos espacios antes de puntos
        txt = txt.replace('\n\n', '\n')  # Quitamos dobles parrafos
        txt = txt.replace('\n \n', '\n')  # Quitamos dobles parrafos con espacio

    # Eliminamos stopwords
    if outstopwords:
        from nltk.corpus import stopwords
        stop_words = stopwords.words('spanish')
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
        text = text.replace(symbol, " ")  # Erease expresion
    return text


def outwords(text):
    # Dada una lista, elimina las palabras en un texto
    # Ampliar lista?
    wordlist = ['ley', 'leyes', 'fj', 'derechos', 'derecho', 'justicia', ' ce ', 'LOTJ']
    for word in wordlist:
        text = text.replace(word, "")  # Erease expresion
    return text


# Consiguiendo una tokenizacion "perfecta"
def sentences(text):
    # split en frases
    import pattern.es as pa
    parsed = pa.parse(text, tokenize=True, tags=0,
                      chunks=0, relations=0, lemmata=False, encoding='utf-8', tagset=None)
    bad_list = parsed.split('\n')
    ulist = [s.replace('&slash;', '/') for s in bad_list]  # edicion por mala codifiacion
    return [x.encode('utf-8') for x in ulist]


def preprosentence(txt):
    # Preprocesamiento previo a la metrica
    return nonum(outwords(process_text(out_art(txt))))


def get_cosine(str1, str2):
    import re
    import math
    from collections import Counter
    comp = re.compile(r'\w+')
    str1 = preprosentence(str1)
    str2 = preprosentence(str2)
    words1 = comp.findall(str1)
    words2 = comp.findall(str2)
    vec1 = Counter(words1)
    vec2 = Counter(words2)
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x] ** 2 for x in vec1.keys()])
    sum2 = sum([vec2[x] ** 2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator


def connect(nodes):
    return [(start, end, get_cosine(start, end))
            for start in nodes
            for end in nodes
            if start is not end]


def rank(nodes, edges):
    import networkx as nx
    # el output es un dict de unos nodes y edges
    graph = nx.DiGraph()
    graph.add_nodes_from(nodes)
    graph.add_weighted_edges_from(edges)
    return nx.pagerank(graph)


def find_phrase(phrase_list, txt_structured):
    # txt_structured es una lista de vectores donde los vector son la pagina y el texto de la pagina.
    # la funcion itera sobre una lista para averiguar las paginas esta cada frase.
    # output= lista de paginas
    ou = []
    try:
        for phrase in phrase_list:
            for i in txt_structured:
                if phrase in sentences(i[1]):
                    ou.append(i[0])
    except UnicodeDecodeError as e:
        print e
    return ou


def summarize(es__host, indice, name, criterion):
    text = ''
    try:
        text = conn_query(es__host, indice, name, criterion)  # en text se guarda txt1 y txt2
    except TypeError as e:
        print (e)
    txt = del_strange_charact(del_page_num(text[0]))
    nodes = sentences(txt)
    edges = connect(nodes)
    scores = rank(nodes, edges)
    phrases = sorted(scores, key=scores.get, reverse=True)
    scroll = [float(x) / len(nodes) * 100 for x in range(len(nodes))]
    vec_list = (phrases, find_phrase(phrases, text[1]), scores, scroll)
    return vec_list


def loadSummaryIntoElastic(name, vec_list):
    try:
        newesconn = Elasticsearch(["172.22.248.206:9229"])
    except 'connection error':
        print ("CONNECTION ERROR")

    INDEX_NAME = 'pruebas'
    TYPE_NAME = 'summary'
    # drop index
    # OJO, sólo si queremos reprocesar toda la información
    if newesconn.indices.exists(INDEX_NAME):
        print("Borrando el indice '%s'..." % (INDEX_NAME))
        res = newesconn.indices.delete(index=INDEX_NAME)
        print("response: '%s'" % (res))

    # Configuracion de particion de indices y replicacion
    request_body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }

    # OJO, sólo si no existe o se ha borrado
    print('')
    print("Creando el indice '%s'..." % (INDEX_NAME))
    res = newesconn.indices.create(index=INDEX_NAME, body=request_body)
    print("response: '%s'" % (res))
    print('')

    # Habrá que buscar los _id asociados al nombre del documento y borrar todos si existen
    # antes de cargar de nuevo los datos en ES

    bulk_data = []

    i = 1
    filename = name

    # Preparamos los datos a cargar en ES
    for data in vec_list:
        indexid = filename + "_" + str(i)
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
                "_id": indexid
            }
        }
        bulk_data.append(doc)
        bulk_data.append(data_dict)
        i = i + 1

    # bulk index the data
    print("bulk indexing...")
    res = newesconn.bulk(index=INDEX_NAME, body=bulk_data, refresh=True, request_timeout=60)
    print("Finalizada la carga de datos")


def res(es__host, indice, name, criterion):
    summaries = summarize(es__host, indice, name, criterion)
    print(summaries)
    loadSummaryIntoElastic(name, summaries)
    print ("Finalizado el proceso de generación de resúmen")

if __name__ == "__main__":
    res(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
