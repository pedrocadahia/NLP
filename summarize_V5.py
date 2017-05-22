def conn_query(es__host1, indice, doc, criterion):
    # conexion a elasticsearch
    try:
        es = Elasticsearch([es__host1])
    except 'connection error':
        print("CONNECTION ERROR")
    try:
        # revisar indice para consulta. index="aeacus_kdd". uri:aeacus:fs#9e33cbdbd42b4ec98b1e8d2080d64ed4
        r = es.search(index=indice, doc_type="document",
                      body={"size": 1000, "query": {"match": {criterion: doc}}},
                      sort="page")
        txt = []
        for doc in r['hits']['hits']:
            txt.append(doc['_source']['content'])
        return txt
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
    return del_strange_charact(nonum(outwords(process_text(out_art(txt)))))


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


def get_phrase(nodes, frases):
    # Primero, acotamos las frases segun su aparicion en el texto
    nod = []
    for node in nodes:
        if node not in nod:
            nod.append(node)

    indice_frase = []
    c = 0
    for i in nod:
        c += 1
        if c < 2:
            indice_frase.append((0, len(i), i))
        else:
            indice_frase.append((indice_frase[c - 2][1] + 1, indice_frase[c - 2][1] + len(i), i))
    indice_frase[0]
    # Segundo, buscamos las frases ordenadas por score en que acotacion esta segun el resultado anterior
    frases_acotadas = []
    for j in frases:
        for i in indice_frase:
            if i[2] == j:
                frases_acotadas.append(i[0:2])
    frases_acotadas
    # Tercero, tenemos las paginas acotadas
    # indice de paginas
    texto_paginas = conn_query(ippropia, index, documento, criterio)
    indices = []
    c = 0
    for i in texto_paginas:
        c += 1
        if c < 2:
            indices.append((0, len(i), c))
        else:
            indices.append((indices[c - 2][1] + 1, indices[c - 2][1] + len(i), c))
    indices
    # cuarto, buscamos dentro de los rangos encontrados
    localizacion = []
    no_encontradas = []
    for ind in frases_acotadas:
        encontrada = False
        for ind_pag in indices:
            if ind[0] >= ind_pag[0] and ind[1] <= ind_pag[1] and not encontrada:
                localizacion.append(ind_pag[2])
                encontrada = True
            elif (ind[0] >= ind_pag[0] and ind[0] <= ind_pag[1]) and (ind[1] > ind_pag[1]) and not encontrada:
                localizacion.append((ind_pag[2], ind_pag[2] + 1))
                encontrada = True
        if not encontrada:
            localizacion.append(indices[-1][-1])
    return localizacion


def summarize(es__host1, indice, name, criterion):
    try:
        text = conn_query(es__host1, indice, name, criterion)  # en text se guarda txt1 y txt2
    except TypeError as e:
        print(e)
    txt_junto=''
    for i in text:
        txt_junto+=i
    txt = del_page_num(txt_junto)
    nodes = sentences(txt)
    edges = connect(nodes)
    scores = rank(nodes, edges)
    phrases = sorted(scores, key=scores.get, reverse=True)
    scroll=[]
	for key in phrases:
		if key in nodes:
			try:
				scroll.append(float(nodes.index(key))/len(nodes)*100)
			except:
				pass

    vec_list = (phrases, get_phrase(nodes,phrases), sorted(scores.values(),reverse=True), scroll)
    return vec_list
