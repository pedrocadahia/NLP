#!/usr/bin/env python
# -*- coding: utf-8 -*-

#######################################################################
#                        AKD PEDRO CADAHIA   V1                       #
#######################################################################
# Suponemos que los archivos a tratar son PDF de varias paginas
# Por lo que la simulacion se basa en resumir un pdf en disco local
# El programa realiza grafos a partir de tokenizar en frases un texto
#######################################################################
#  PARAMETROS
# archivo = "C:/Users/pcadahia/Desktop/d1/04_019-025_14.pdf"
archivo = "D://Judicial//input//d1//04_019-025_14.pdf"
# import sys
# sys.stdout.encoding
# chardet.detect(texto)

#######################################################################
# Leer PDF


def pdf2txt(path):
    from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
    from pdfminer.converter import TextConverter
    from pdfminer.layout import LAParams
    from pdfminer.pdfpage import PDFPage
    from cStringIO import StringIO
    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    codec = 'latin1'
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
    fp = file(path, 'rb')
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    password = ""
    maxpages = 0
    caching = True
    pagenos = set()
    numpages = 0
    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching,
                                  check_extractable=True):
        numpages += 1
        interpreter.process_page(page)

    text = retstr.getvalue()

    fp.close()
    device.close()
    retstr.close()
    return text, numpages


texto = pdf2txt(archivo)[0]

#########################################################################
# Comenzamos el proceso


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
    aux = re.findall(r'\b(\d{1,2}/\d{1,2})\b', text)
    pattern = re.compile(r"\b(" + "|".join(aux) + ")\\W", re.I)
    return pattern.sub("", text)


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
    # Elimina ciertas palabras muy repetidas
    # no implementada actualmente
    # en caso de implementar aniadirla a preprosentence
    words = ['ley', 'leyes', 'fj', 'derechos', 'derecho', 'justicia', ' ce ', 'LOTJ']
    for word in words:
        text = text.replace(word, "")  # Erease expresion
    return text


def sentences(text):
    # Parte el texto en frases
    import nltk.data
    spanish_tokenizer = nltk.data.load('tokenizers/punkt/spanish.pickle')
    return spanish_tokenizer.tokenize(remove_accents(text))


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
    # the output is a dict from given nodes and edges
    graph = nx.DiGraph()
    graph.add_nodes_from(nodes)
    graph.add_weighted_edges_from(edges)
    return nx.pagerank(graph)


###################
# Metricas
def get_scroll(text):
    phrases = sentences(text)
    l = []
    c = 0
    for i in phrases:
        c += 1
        l.append((float(c) * 100 / len(phrases), i))
    return l


def get_value(di, keys):
    # keys is a list of keys to check in the dict
    try:
        a = map(di.get, keys)
        a = map(lambda x: 1 if x is None else x, a)
        return a
    except Exception:
        return 'wrong dictionary'


def get_indicators(text):
    # score
    score = get_value(rank(sentences(text), connect(sentences(text))), sentences(text))
    # scroll%
    scroll = get_scroll(text)

    # Show K, y = 2,7938ln(x) + 0,7
    # round(3*math.log1p(x)+0.7)
    return score, scroll


def get_pages(x):
    import math
    return int(round(2.7 * math.log1p(x) + 0.6))

#####################################################################################


def summarize(doc, k=None):
    text = pdf2txt(doc)
    if k is None:
        k = get_pages(text[1])
    # from a string gives the most important sentences in asc order
    nodes = sentences(text[0])
    edges = connect(nodes)
    scores = rank(nodes, edges)
    return sorted(scores, key=scores.get, reverse=True)[:k]

