#!/usr/bin/env python
# -*- coding: utf-8 -*-
#######################################################################
#                   RESUMEN DE TEXTOS PEDRO CADAHIA                   #
#######################################################################
# Suponemos que los archivos a tratar son PDF de varias paginas
# Por lo que la simulacion se basa en resumir un pdf en disco local
# El programa realiza grafos a partir de tokenizar en frases un texto
#######################################################################
#  PARAMETROS
# archivo = "D:/Judicial/data/ejemplo.pdf"
archivo = "D:/Judicial/data/7712964.pdf"
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

    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching,
                                  check_extractable=True):
        interpreter.process_page(page)

    text = retstr.getvalue()

    fp.close()
    device.close()
    retstr.close()
    return text


texto = pdf2txt(archivo)
texto = texto[2230:-1]

#########################################################################
# Comenzamos el proceso


def depPuntArt(texto):
    # Elimina el formato 1.2 que confunde al tokenizar frases
    import re
    auxExp = re.findall('[0-9]\.[0-9]', texto)
    while len(auxExp) > 0:
        for expr in auxExp:
            auxexpr = expr.replace(".", "")
            texto = texto.replace(expr, auxexpr)
            auxExp = re.findall('[0-9]\.[0-9]', texto)
    return texto


def remove_accents(input_str):
    try:
        input_str = unicode(input_str.decode('latin1'))
    except UnicodeEncodeError as e:
        pass
    import unicodedata
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    return only_ascii


def reduce_concat(x, sep=""):
    return reduce(lambda x, y: x + sep + y, x)


def preProcesoTexto(texto, eliminarStopwords=True, eliminarPuntuaciones=False):
    # Funcion que elimina los acentos, stopwords, puntuaciones y convierte a minusculas

    # Quitamos acentos
    texto = remove_accents(texto)

    # Pasamos a minuscula
    texto = texto.lower()

    # Eliminamos simbolos
    symbols = ['[', ']', '•', '^', '*', '/', '=']
    for simbol in symbols:
        texto = texto.replace(simbol, '')

    if eliminarPuntuaciones:
        # except '.'
        puntuations = [',', ':', ';', '(', ')', '?', '¿', '!', '¡', "'", '"']
        for puntuacion in puntuations:
            texto = texto.replace(puntuacion, '')

    # Hacemos algunas conversiones utiles
    textoAnt = ''
    while textoAnt != texto:
        textoAnt = texto
        texto = texto.replace('  ', ' ')       # Quitamos dobles espacios
        #    texto = texto.replace('. ', '.')  # Quitamos espacios despues de puntos
        #    texto = texto.replace(' .', '.')  # Quitamos espacios antes de puntos
        texto = texto.replace('\n\n', '\n')    # Quitamos dobles parrafos
        texto = texto.replace('\n \n', '\n')   # Quitamos dobles parrafos con espacio

    # Eliminamos stopwords
    if eliminarStopwords:
        from nltk.corpus import stopwords
        stop_words = stopwords.words('spanish')
        for i in range(len(stop_words)):
            stop_words[i] = remove_accents(stop_words[i])

        texto_wd = texto.split(" ")
        texto_wd = [word for word in texto_wd if word not in stop_words]
        texto = reduce_concat(texto_wd, sep=" ")

    return texto


def nonum(text):
    # Elimina ciertas expresiones que distorsionan la delimitacion de parrafos
    symbols = ['art.', ' art ', 'num.', 'i.', 'ii.', 'iii.', '.-']
    for symbol in symbols:
        text = text.replace(symbol, " ")  # Erease expresion
    return text


def preprocessingtemporal(text):
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
    # Preprocesa las frases a usar en la metrica para no alterar el texto de cache
    return nonum(preprocessingtemporal(preProcesoTexto(depPuntArt(txt), 1, 1)))


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
    # from a string gives the most k-important sentences
    nodes = sentences(text)
    edges = connect(nodes)
    scores = rank(nodes, edges)
    return sorted(scores, key=scores.get, reverse=True)


def muestrameresumen(texto, k):
    # funcion para mostrar el resumen visualmente por pantalla
    result = summarize(texto, k)
    for i in range(len(result)):
        print result[i]

#################################
# graph


def plotear(txt):
    import networkx as nx
    import matplotlib.pyplot as plt

    graph = nx.DiGraph()
    graph.add_nodes_from(sentences(txt))
    graph.add_weighted_edges_from(connect(sentences(txt)))
    nx.draw(graph)
    return plt.show()


###################
# Metricas

def get_scroll(text):
    frases = sentences(text)
    l = []
    c = 0
    for i in frases:
        c += 1
        l.append((float(c)*100 / len(frases), i))
    return l


def getvalue(dict, keys):
    # keys is a list of keys to check in the dict
    try:
        a = map(dict.get, keys)
        a = map(lambda x: 1 if x is None else x, a)
        return a
    except Exception:
        return 'wrong dictionary'


def get_indicators(text):
    # score
    score = getvalue(rank(sentences(texto), connect(sentences(texto))), sentences(text))
    # scroll%
    scroll = get_scroll(text)

    # Show K, y = 2,7938ln(x) + 0,7
    # round(3*math.log1p(x)+0.7)
    return score, scroll


def getpages(x):
    import math
    return round(2.7*math.log1p(x)+0.6)
#####################################################
# create pdf


#####################################################
# Exportado de pdfs con frases relevantes. Reporting

lista=[]
for i in lista:
    archivo = "D:/Judicial/data/"+str(i)+".pdf"


def create():
    print("creating new  file")
    name=raw_input ("enter the name of file:")
    extension=raw_input ("enter extension of file:")
    try:
        name=name+"."+extension
        file=open(name,'a')

        file.close()
    except:
            print("error occured")
            sys.exit(0)
