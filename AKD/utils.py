# Funcion de Instalacion y carga de paquetes
def install_packages(package):
    # Es un install_packages y require en python desde consola
    import importlib
    try:
        importlib.import_module(package)
    except ImportError:
        import pip
        pip.main(['install', package])
    finally:
        globals()[package] = importlib.import_module(package)


def plotwcloud(texto,rscale=1):
    # a partir de un texto crea un wordloud basado en frecuencias
    import image
    from wordcloud import WordCloud, STOPWORDS
    import matplotlib.pyplot as plt

    wordcloud = WordCloud(relative_scaling=rscale).generate(texto)
    plt.imshow(wordcloud)
    plt.axis("off")
    plt.show()


def freqword(texto,k=50, cumul=False):
    # Vemos la frecuencia de las palabras
    from nltk import FreqDist
    texto= unicode(texto, "utf-8")
    fdist = FreqDist(texto.split(" "))
    fdist.plot(k, cumulative=cumul)


def loadtxt(filename):
    infile = open(filename,'rb')
    words  = infile.readlines(); infile.close()
    return words


def stemmizer(Words,Language):
    from nltk import word_tokenize
    from nltk.stem import SnowballStemmer
    stemmer = SnowballStemmer(Language)
    if len(Words) == 1:
        stemmed = stemmer.stem(Words)
    else:
        stemmed = [stemmer.stem(i) for i in word_tokenize(Words)]
    return stemmed


def Similarity(measure, str1, str2):
    if measure =='cosine':

        def dcosine(str1, str2):
            # Cosine Distance
            import re
            import math
            from collections import Counter
            WORD = re.compile(r'\w+')
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
        return dcosine(str1,str2)

    elif measure == 'ldist':

        def ldist(str1, str2):
            # levenshtein distance
            d = dict()
            for i in range(len(str1) + 1):
                d[i] = dict()
                d[i][0] = i
            for i in range(len(str2) + 1):
                d[0][i] = i
            for i in range(1, len(str1) + 1):
                for j in range(1, len(str2) + 1):
                    d[i][j] = min(d[i][j - 1] + 1, d[i - 1][j] + 1, d[i - 1][j - 1] + (not str1[i - 1] == str2[j - 1]))
            return d[len(str1)][len(str2)]

        return ldist(str1, str2)

    elif measure == 'dice':

        def dice_coefficient(str1, str2):
            # Coeficiente de Sorensen-Dice
            if not len(str1) or not len(str2): return 0.0
            # quick case for true duplicates
            if str1 == str2: return 1.0
            # if a != b, and a or b are single chars, then they can't possibly match
            if len(str1) == 1 or len(str2) == 1: return 0.0

            # use python list comprehension, preferred over list.append()
            a_bigram_list = [str1[i:i + 2] for i in range(len(str1) - 1)]
            b_bigram_list = [str2[i:i + 2] for i in range(len(str2) - 1)]

            a_bigram_list.sort()
            b_bigram_list.sort()

            # assignments to save function calls
            lena = len(a_bigram_list)
            lenb = len(b_bigram_list)
            # initialize match counters
            matches = i = j = 0
            while (i < lena and j < lenb):
                if a_bigram_list[i] == b_bigram_list[j]:
                    matches += 2
                    i += 1
                    j += 1
                elif a_bigram_list[i] < b_bigram_list[j]:
                    i += 1
                else:
                    j += 1

            score = float(matches) / float(lena + lenb)
            return score

        return dice_coefficient(str1, str2)
    elif measure == 'jaccard':
        def jaccard(str1, str2):
            # jaccard similarity
            str1 = set(str1.split())
            str2 = set(str2.split())
            return float(len(str1 & str2)) / len(str1 | str2)

        return jaccard(str1, str2)
    else:
        return 'Error de argumento: cosine, ldist, dice,jaccard'

####################################################
#              Detector de Lenguaje                #
####################################################
# 55 Idiomas soportados, ISO 639-1 codes
# Base de datos

Siglas =      ('ab',    'aa',  'af',   'ak',   'sq',	'am',	'ar',	'an',
              'hy',	'as',	'av',	'ae',	'ay',	'az',	'bm',	'ba',
              'eu',	'be',	'bn',	'bh',	'bi',	'bs',	'br',	'bg',
              'my',	'ca',	'ch',	'ce',	'ny',	'zh',	'cv',	'kw',
              'co',	'cr',	'hr',	'cs',	'da',	'dv',	'nl',	'dz',
              'en',	'eo',	'et',	'ee',	'fo',	'fj',	'fi',	'fr',
              'ff',	'gl',	'ka',	'de',	'el',	'gn',	'gu',	'ht',
              'ha',	'he',	'hz',	'hi',	'ho',	'hu',	'ia',	'id',
              'ie',	'ga',	'ig',	'ik',	'io',	'is',	'it',	'iu',
              'ja',	'jv',	'kl',	'kn',	'kr',	'ks',	'kk',	'km',
              'ki',	'rw',	'ky',	'kv',	'kg',	'ko',	'ku',	'kj',
              'la',	'lb',	'lg',	'li',	'ln',	'lo',	'lt',	'lu',
              'lv',	'gv',	'mk',	'mg',	'ms',	'ml',	'mt',	'mi',
              'mr',	'mh',	'mn',	'na',	'nv',	'nd',	'ne',	'ng',
              'nb',	'nn',	'no',	'ii',	'nr',	'oc',	'oj',	'cu',
              'om',	'or',	'os',	'pa',	'pi',	'fa',	'pl',	'ps',
              'pt',	'qu',	'rm',	'rn',	'ro',	'ru',	'sa',	'sc',
              'sd',	'se',	'sm',	'sg',	'sr',	'gd',	'sn',	'si',
              'sk',	'sl',	'so',	'st',	'es',	'su',	'sw',	'ss',
              'sv',	'ta',	'te',	'tg',	'th',	'ti',	'bo',	'tk',
              'tl',	'tn',	'to',	'tr',	'ts',	'tt',	'tw',	'ty',
              'ug',	'uk',	'ur',	'uz',	've',	'vi',	'vo',	'wa',
              'cy',	'wo',	'fy',	'xh',	'yi',	'yo',	'za',	'zu')
Definicion = ('Abkhaz', 'Afar', 'Afrikaans', 'Akan', 'Albanian',
             'Amharic',	'Arabic', 'Aragonese', 'Armenian',
             'Assamese',    'Avaric', 'Avestan', 'Aymara',
             'Azerbaijani', 'Bambara',  'Bashkir',  'Basque',
             'Belarusian',  'Bengali', 'Bihari', 'Bislama', 'Bosnian',
             'Breton', 'Bulgarian', 'Burmese',	'Catalan',  'Chamorro',
             'Chechen',	'Chichewa', 'Chinese','Chuvash', 'Cornish', 'Corsican',
             'Cree',b'Croatian', 'Czech', 'Danish', 'Maldivian', 'Dutch',
             'Dzongkha', 'English', 'Esperanto', 'Estonian', 'Ewe', 'Faroese',
             'Fijian',	'Finnish',  'French','Fula','Galician','Georgian',
             'German',	'Greek',	'Guarani',	'Gujarati',
             'Haitian',	'Hausa',	'Hebrew',	'Herero',
             'Hindi',	'Hiri Motu',	'Hungarian',	'Interlingua',
             'Indonesian',	'Interlingue',	'Irish',    'Igbo',
             'Inupiaq',	'Ido',	'Icelandic',	'Italian',  'Inuktitut',
             'Japanese',	'Javanese',	'Kalaallisut','Kannada',
             'Kanuri',	'Kashmiri',	'Kazakh',   'Khmer',
             'Kikuyu',	'Kinyarwanda',	'Kyrgyz',   'Komi', 'Kongo',
             'Korean',	'Kurdish',	'Kwanyama', 'Latin',
             'Luxembourgish',	'Ganda',	'Limburgish','Lingala',
             'Lao',	'Lithuanian',	'Luba',	'Latvian',  'Manx',
             'Macedonian',  'Malagasy', 'Malay','Malayalam',
             'Maltese',	'Maori',	'Marathi',	'Marshallese',
             'Mongolian',   'Nauruan',  'Navajo',
             'Northern Ndebele', 'Nepali',	'Ndonga',
             'Norwegian Bokmal', 'Norwegian Nynorsk',	'Norwegian',
             'Nuosu',	'Southern Ndebele', 'Occitan',	'Ojibwe',
             'Slavonic',    'Oromo',    'Oriya',	'Ossetian',
             'Punjabi',	'Pali',	'Persian','Polish',	'Pashto',
             'Portuguese',  'Quechua',  'Romansh',	'Kirundi',
             'Romanian',    'Russian',  'Sanskrit',	'Sardinian',
             'Sindhi',	'Northern Sami','Samoan',	'Sango',
             'Serbian',	'Gaelic',   'Shona',	'Sinhalese',
             'Slovak',	'Slovene',  'Somali',	'Southern Sotho',
             'Spanish',	'Sundanese','Swahili',	'Swati',
             'Swedish',	'Tamil',    'Telugu',	'Tajik',
             'Thai',	'Tigrinya', 'Tibetan Standard',	'Turkmen',
             'Tagalog',	'Tswana',   'Tonga',	'Turkish',
             'Tsonga',	'Tatar',    'Twi',	'Tahitian',	'Uyghur',
             'Ukrainian',   'Urdu',	'Uzbek',	'Venda',
             'Vietnamese',  'Volapuk',	'Walloon',	'Welsh',
             'Wolof',	'Western Frisian',  'Xhosa',    'Yiddish',
             'Yoruba',	'Zhuang',	'Zulu')
Diccionario = dict(zip(Siglas, Definicion))
del Siglas, Definicion


def lenguaje(texto):
    from langdetect import detect
    import unicodedata
    try:
        texto = unicode(texto.decode('latin1'))
    except UnicodeEncodeError as e:
        pass
    nfkd_form = unicodedata.normalize('NFKD', texto)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    # Reconocimiento
    Siglas = detect(only_ascii) # Cuidado! hay que tener el diccionario cargado!
    return Diccionario[Siglas]
