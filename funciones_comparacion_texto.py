# -*- coding: utf-8 -*-

# Funciones busqueda texto
# Llamame:
#		execfile(PATH + 'syntax/scripts/funciones_comparacion_texto.py')

# from math import log
import numpy as np

def comparaFrases(llistaFrases, fileMod, totesContraTotes = True):
	
	if es_hdfs:
		print "S'ha de programar!!"
	
	compta = 0
	nRep = factorial(len(llistaFrases)) / (factorial(2) * factorial(len(llistaFrases) - 2))
	resultado = pd.DataFrame(data = dict(item1 = np.repeat(None, nRep), item2 = np.repeat(None, nRep), valor_comp = np.repeat(None, nRep)))
	model = gensim.models.Word2Vec.load(fileMod)
	if not totesContraTotes:
		avgVal = np.mean([len(frase.split(" ")) for frase in llistaFrases[1:]])
	for i in range(len(llistaFrases)):
		for j in range(len(llistaFrases)):
			if i > j:	 # Nomes comparem si son diferents i no les hem comparat ja
				resultado["item1"][compta] = i
				resultado["item2"][compta] = j
				frase1 = llistaFrases[i].split(" ")
				frase2 = llistaFrases[j].split(" ")
				vals_comuns = []
				for paraula1 in frase1:
					for paraula2 in frase2:
						if len(paraula1) > 2 and len(paraula2) > 2:
							try:
								aux_m = model.similarity(paraula1, paraula2)
								vals_comuns.append(aux_m)
							except:
								iguals = 0
								total = 0
								for str1 in paraula1:
									for str2 in paraula2:
										total += 1
										if str1 == str2:
											iguals += 1
								vals_comuns.append(float(iguals)/total)
				if totesContraTotes:
					resultado["valor_comp"][compta] = np.mean(vals_comuns)
				else:
					resultado["valor_comp"][compta] = np.mean(vals_comuns)*(1 - (abs(avgVal - min(len(frase1), len(frase2))))/len(frase1 + frase2))
					
				compta += 1

	return resultado

def unirGrupos(item1, item2, vanJunts):
	from copy import deepcopy
	import itertools

	# Creem llista de grups i amb qui es relacionen
	dictRels = dict()
	for i in range(len(vanJunts)):
		if item1[i] in dictRels.keys() and vanJunts[i]:
			dictRels[item1[i]].append(item2[i])
		elif vanJunts[i]:
			dictRels[item1[i]] = []
			dictRels[item1[i]].append(item2[i])
		elif item1[i] not in dictRels.keys():
			dictRels[item1[i]] = []
		
		if item2[i] in dictRels.keys() and vanJunts[i]:
			dictRels[item2[i]].append(item1[i])
		elif vanJunts[i]:
			dictRels[item2[i]] = []
			dictRels[item2[i]].append(item1[i])
		elif item2[i] not in dictRels.keys():
			dictRels[item2[i]] = []
			
	for ikey in dictRels.keys():
		 dictRels[ikey].append(ikey)
		 dictRels[ikey] = list(set(dictRels[ikey]))
	
	# Sacamos los grupos a partir de las relaciones
	dictAnt = deepcopy(dictRels)
	dictGroups = dict()
	while dictAnt != dictGroups:
		dictAnt = deepcopy(dictGroups)
		dictGroups = dict()
		for ikey in dictRels.keys():
			grupAct = deepcopy(dictRels[ikey])
			for jkey in dictRels.keys():
				if ikey != jkey and ikey in dictRels[jkey]:
					grupAct.extend(dictRels[jkey])
					grupAct = list(set(grupAct))
			
			dictGroups[ikey] = grupAct
			
		dictRels = deepcopy(dictGroups)
	
	# Identificamos grupos unicos
	aux_l = []
	for ikey in dictGroups.keys():
		aux_l.append(dictGroups[ikey])
	aux_l.sort()
	result = list(aux_l for aux_l,_ in itertools.groupby(aux_l))
	
	return result
