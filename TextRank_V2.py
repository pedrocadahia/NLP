#!/usr/bin/env python
# -*- coding: utf-8 -*-
#######################################################################
#                       AKD PEDRO CADAHIA V2                          #
#######################################################################
# Esta release es la pensada para la puesta en produccion en un sistema scala
# Paralelizable, por lo que ademas se podran encontrar funciones propias de scala.
#######################################################################
#  PARAMETROS
# El primer requisito es la carga de las diferentes paginas en un documento,
# estas paginas se encuentran alojadas en elastisearch de modo que se debera
# realizar una consulta para reunir el documento, por uuid, uri o name.
#######################################################################
# Conexion y Consulta elastisearch

documento = "03_002-008_14.PDF"  # 03_002-008_14.PDF
criterio = "name"
ippropia = "172.22.248.206:9229"


def conn_query(ip, doc, criterion):
    # conexion a elasticsearch
    from elasticsearch import Elasticsearch
    try:
            es = Elasticsearch([ip])
    except:
        print "CONNECTION ERROR "

    # revisar indice para consulta. index="aeacus_kdd". uri:aeacus:fs#9e33cbdbd42b4ec98b1e8d2080d64ed4
    res = es.search(index="aeacus_kdd", doc_type="document", body={"query": {"match": {criterion: doc}}}, sort="page")

    text = ""
    for doc in res['hits']['hits']:
        text = text+doc['_source']['content']

    return text

texto = conn_query(ippropia, documento, criterio)

# Preprocesado basado en elastic search


def remove_accents(input_str, encoding="latin1"):
    try:
        input_str = unicode(input_str.decode(encoding))
    except UnicodeEncodeError as e:
        pass
    import unicodedata
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    return only_ascii


def del_page_num(text):
    import re
    r = re.compile(r'[0-9][0-9]/[0-9][0-9]|[0-9]/[0-9][0-9]', flags=re.I | re.X)
    aux = r.findall(text)
    for item in aux:
        text = text.replace(item, "")
    return text


def del_strange_charact(txt):
    # Los textos de elastic search son una maravilla...
    prev_txt = ''
    while prev_txt != txt:
        prev_txt = txt
        txt = txt.replace(',.', ',')
        txt = txt.replace(', .', ',')
    return prev_txt

texto = del_strange_charact(remove_accents(del_page_num(texto)))

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
# t = nonum(outwords(preProcesoTexto(out_art(txt), 1, 1)))

'''
def sentences1(text):
    # Parte el texto en frases
    import nltk.data
    spanish_tokenizer = nltk.data.load('tokenizers/punkt/spanish.pickle')
    return spanish_tokenizer.tokenize(text)

t1 = sentences1(texto)
'''


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
    WORD = re.compile(r'\w+')
    str1 = preprosentence(str1)
    str2 = preprosentence(str2)
    words1 = WORD.findall(str1)
    words2 = WORD.findall(str2)
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
    # the output is a dict from given nodes and edges
    graph = nx.DiGraph()
    graph.add_nodes_from(nodes)
    graph.add_weighted_edges_from(edges)
    return nx.pagerank(graph)


def summarize(text):
    # from a string gives the most important sentences in asc order
    nodes = sentences(text)
    edges = connect(nodes)
    scores = rank(nodes, edges)
    return sorted(scores, key=scores.get, reverse=True)

# re.sub(r'[^\w]', ' ', s)
# [^\w] will match anything that's not alphanumeric or underscore