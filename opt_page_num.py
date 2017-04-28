#!/usr/bin/env python
# -*- coding: utf-8 -*-
#######################################################################
#                       AKD PEDRO CADAHIA V2                          #
#######################################################################
# Conexion y Consulta elastisearch para ver el numero optimo de paginas
#######################################################################
import sys

if len(sys.argv) != 3:
    raise Exception(" No se han introducido todos los argumentos requeridos o se han introducido de manera incorrecta"
                    " 1. Arg 1: Nombre del documento o identificador. Ej: 03_002-008_14.PDF"
                    " 2. Arg 2: Nombre del criterio de busqueda, Ej: name, uuid o uri...")


def opt_page_num(doc, criterion):
    # conexion a elasticsearch
    from elasticsearch import Elasticsearch
    import math
    try:
        es = Elasticsearch(["172.22.248.206:9229"])
    except 'connection error':
        print ("CONNECTION ERROR")
    try:
        # revisar indice para consulta. index="aeacus_kdd". uri:aeacus:fs#9e33cbdbd42b4ec98b1e8d2080d64ed4
        r = es.search(index="aeacus_kdd", doc_type="document",
                      body={"size": 1000, "query": {"match": {criterion: doc}}},
                      sort="page")
        c = 0
        for _ in r['hits']['hits']:
            c += 1
        return round(2.7 * math.log1p(c) + 0.6)
    except 'Warning':
        print 'la consulta no se ha realizado de manera adecuada'

if __name__ == "__main__":
    opt_page_num(sys.argv[1], sys.argv[2])
