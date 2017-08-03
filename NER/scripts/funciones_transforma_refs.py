
DictSubsMeses = dict(
	esp = dict(
		enero = "01",
		febrero = "02",
		marzo = "03",
		abril = "04",
		mayo = "05",
		junio = "06",
		julio = "07",
		agosto = "08",
		setiembre = "09",
		septiembre = "09",
		octubre = "10",
		noviembre = "11",
		diciembre = "12"
	),
	bra = dict(
		janeiro = "01",
		fevereiro = "02",
		marco = "03",
		abril = "04",
		maio = "05",
		junho = "06",
		julho = "07",
		agosto = "08",
		setembro = "09",
		outubro = "10",
		novembro = "11",
		dezembro = "12"
	)
)


# transformaNumsString("dos mil setecientos trentaydos")
def transformaNumsString(numstr):

	DictPaisDictUnidades = dict(
		esp = dict(
			uno = 1,
			dos = 2,
			tres = 3,
			cuatro = 4,
			cinco = 5, 
			seis = 6,
			siete = 7,
			ocho = 8,
			nueve = 9
		),
		bra = dict(
			um = 1,
			dois = 2,
			tres = 3,
			quatro = 4,
			cinco = 5, 
			seis = 6,
			sete = 7,
			oito = 8,
			nove = 9
		)
	)
	
	DictPaisDictDecenas = dict(
		esp = dict(
			diez = 10,
			once = 11,
			doce = 12,
			trece = 13,
			catorce = 14,
			quince = 15,
			dieciseis = 16,
			diecisiete = 17,
			dieciocho = 18,
			diecinueve = 19,
			veinti = 20,
			veinty = 20,
			veintiun = 21,
			veintyun = 21,
			veinte = 20,
			treinta = 30,
			trenta = 30,
			cuarenta = 40,
			cincuenta = 50,
			sesenta = 60,
			setenta = 70,
			ochenta = 80,
			noventa = 90
		),
		bra = dict(
			dez = 10,
			onze = 11,
			doze = 12,
			treze = 13,
			catorze = 14,
			quinze = 15,
			dezesseis = 16,
			dezessete = 17,
			dezoito = 18,
			dezenove = 19,
			vinte = 20,
			trinta = 30,
			quarenta = 40,
			cinquenta = 50,
			sessenta = 60,
			setenta = 70,
			oitenta = 80,
			noventa = 90
		)
	)
	
	
	DictPaisDictCentenas = dict(
		esp = dict(
			cien = 100,
			ciento = 100,
			quinientos = 500,
			setecientos = 700,
			novecientos = 900
		),
		bra = dict(
			cem = 100,
			duzentos = 200,
			trezentos = 300,
			quatrocentos = 400,
			quinhentos = 500,
			seiscentos = 600,
			setecentos = 700,
			oitocentos = 800,
			novecentos = 900
		)
	)
	
	result = "0000"
	
	# Miles
	if "mil" in numstr:
		result = "1000"
		aux_num = numstr.split("mil")
		aux_num = aux_num[0].strip()
		if aux_num != "":
			result = str(DictPaisDictUnidades[pais][aux_num]) + result[1:]
	
	# Centenas
	centenas = False
	for ikey in DictPaisDictCentenas[pais].keys():
		if ikey in numstr:
			centenas = True

	if centenas:
		aux_num = numstr.split("mil")
		aux_num = aux_num[len(aux_num) - 1].strip()
		if pais == "bra":
			keyutiliza = ""
			for ikey in DictPaisDictCentenas[pais].keys():
				if len(re.findall("\\b" + ikey + "\\b", aux_num)) > 0:
					keyutiliza = ikey
			
			if keyutiliza != "":
				result = result[0] + str(DictPaisDictCentenas[pais][keyutiliza])
			
		elif "cientos" in numstr:
			aux_num = aux_num.split("cientos")
			aux_num = aux_num[0].strip()
			
			if aux_num in DictPaisDictUnidades[pais].keys():
				result = result[0] + str(DictPaisDictUnidades[pais][aux_num]) + result[2:]
			elif "setecientos" in numstr or "novecientos" in numstr:
				if "setecientos" in numstr:
					result = result[0] + str(DictPaisDictCentenas[pais]["setecientos"])
				elif "novecientos" in numstr:
					result = result[0] + str(DictPaisDictCentenas[pais]["novecientos"])
			else:
				pass
				#print "No sabemos de que numero hablas!!"
			
		elif "quinientos" in numstr:
			result = result[0] + str(DictPaisDictCentenas[pais]["quinientos"])
		elif "ciento" or "cien" in numstr:
			result = result[0] + str(DictPaisDictCentenas[pais]["ciento"])
		else:
			pass
			#print "No sabemos de que numero hablas!!"
		
	# Decenas
	encontrado = False
	for ikey in DictPaisDictDecenas[pais].keys():
		if len(re.findall("\\b" + ikey + "\\b", numstr)) > 0 and not encontrado:
			result = result[:2] + str(DictPaisDictDecenas[pais][ikey])
			encontrado = True
			keyDecenas = ikey
		elif ikey in numstr and encontrado:
			pass
			#print "No puede ser que exista mas de una key de decenas en el mismo numero!!"
	
	if not encontrado:
		keyDecenas = " "
	
	# Unidades
	if result[3] == "0":
		auxnum = numstr.split(keyDecenas)
		auxnum = auxnum[len(auxnum) - 1].strip()
		auxnum = auxnum.split(" ")
		auxnum = auxnum[len(auxnum) - 1]
		if pais == "esp":
			auxnum = auxnum.split("y")
			auxnum = auxnum[len(auxnum) - 1]
			
		auxnum = auxnum.split("-")
		auxnum = auxnum[len(auxnum) - 1]
		if auxnum in DictPaisDictUnidades[pais].keys():
			result = result[:3] + str(DictPaisDictUnidades[pais][auxnum])
		else:
			pass
			#print "No sabemos detectar las unidades!!"
	
	while result[0] == "0":
		result = result[1:]
		
	return result


def transformaGrandesNumsString(grannumstr):

	if pais == "esp":
		grannumstr = grannumstr.replace("un ", "uno ")
		grannumstr = grannumstr.replace("miles", "mil")
		grannumstr = grannumstr.replace("millones", "millon")
		auxstr = grannumstr.split("millon")
		if "mil" in auxstr[len(auxstr) - 1]:
			grannumstr = grannumstr.replace("millon", "mil")
		else:
			grannumstr = grannumstr.replace("millon", "mil mil")
			
		auxnum = grannumstr.split("mil")
		
	elif pais == "bra":
		grannumstr = grannumstr.replace("milhares", "mil")
		grannumstr = grannumstr.replace("milhoes", "milhao")
		auxstr = grannumstr.split("milhao")
		if "mil" in auxstr[len(auxstr) - 1]:
			grannumstr = grannumstr.replace("milhao", "mil")
		else:
			grannumstr = grannumstr.replace("milhao", "mil mil")
			
		auxnum = grannumstr.split("mil")
	
		
	result = []
	for i in range(len(auxnum)):
		item = auxnum[i]
		if i == 0 and item.replace(" ", "") == "":
			result.append("001")
		elif item.replace(" ", "") == "":
			result.append("000")
		else:
			auxres = transformaNumsString(item.strip())
			while len(auxres) < 3:
				auxres = "0" + auxres
			result.append(auxres)
	
	result = reduce_concat(result, sep = "")

	while result[0] == "0":
		result = result[1:]
	
	return result
	
	
	
	
