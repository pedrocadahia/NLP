#!/usr/bin/env python
# -*- coding: utf-8 -*-
###################################################################################
#                           KDD PEDRO CADAHIA V1                                  #
###################################################################################
# Target. simulate a user interaction data base from the future SenSir KDD platform .
###################################################################################
# Correlated Variables


def rnormal(N):
    from numpy.random import normal as rnorm
    x = rnorm(size=N)
    return x


def correlatedValue(x, loc=0, r=.5):
    # genera variables correladas
    import numpy as np
    from numpy.random import normal as rnorm
    SD = np.sqrt(1 - (r ** 2))
    e = rnorm(size=len(x), loc=loc, scale=SD)
    y = r * x + e
    return np.array(list(map(int, y)))


# Chi Distribution


def get_chisquare(num_obs, grados_libertad=3, non_centr=0.0000001):
    import numpy as np
    dat = np.random.noncentral_chisquare(grados_libertad, non_centr, num_obs)
    return dat


def get_intchisquare(num_obs, grados_libertad=11, non_centr=1, scale=5):
    import numpy as np
    dat = np.random.noncentral_chisquare(grados_libertad, non_centr, num_obs)
    return np.dot(list(map(int, dat)), scale).flatten()


# Normal Distribution
def randn_skew_fast(N, alpha=0.0, loc=0.0, scale=1.0):
    import numpy as np
    sigma = alpha / np.sqrt(1.0 + alpha ** 2)
    u0 = np.random.randn(N)
    v = np.random.randn(N)
    u1 = (sigma * u0 + np.sqrt(1.0 - sigma ** 2) * v) * scale
    u1[u0 < 0] *= -1
    u1 = u1 + loc
    return u1


# Categorical Values


def cat_var(value_str_list, prob_list, n_obs=5000):
    import numpy as np
    # value_str_list es una lista con los nombres de las variables de los tipos de juicio
    # prob_list es la probabilidad de aparición de ese elemento para un expediente o el porcentaje de distribucion
    if len(value_str_list) == len(prob_list):
        res = np.random.choice(value_str_list, n_obs, p=prob_list)
    else:
        print('la lista de strings y sus probabilidades no tienen la misma longuitud!')
    return res


###############################################
# Reading xls files
def read_xsl(loc, ind=0):
    import xlrd
    workbook = xlrd.open_workbook(loc)
    sheet = workbook.sheet_by_index(ind)
    data = [[], []]
    for col in range(sheet.ncols):
        for row in range(sheet.nrows):
            if col == 0:
                data[0].append(sheet.cell_value(row, col))
            else:
                data[1].append(sheet.cell_value(row, col))
    return data


def outunicode(lista):
    try:
        # si se puede a flotante hazlo
        res = list(map(float, lista))
    except:
        # sino es un string
        res = list(map(str, lista))
    return res


def map_many(iterable, function, *other):
    import numpy as np
    if other:
        return map_many(map(function, iterable), *other)
    return np.array(map(function, iterable))


# Variable Generator
def df_corrplot(data_frame, type='pearson'):
    import matplotlib.pyplot as plt
    import seaborn as sns
    f, ax = plt.subplots(figsize=(10, 8))
    corr = data_frame.corr(method=type)
    sns.heatmap(corr, mask=np.zeros_like(corr, dtype=np.bool), cmap=sns.diverging_palette(220, 10, as_cmap=True),
                square=True, ax=ax)
    plt.show()
    return corr


def find_ncomp(lista_array, umbral=70):
    import numpy as np
    if type(lista_array) is list:
        for i in lista_array:
            if i >= umbral:
                selected = i
                n_comp = lista_array.tolist().index(i) + 1
                return tuple([selected, n_comp])
                break
            else:
                continue
    if type(lista_array) is np.ndarray:
        for i in lista_array:
            if i >= umbral:
                selected = i
                n_comp = lista_array.tolist().index(i) + 1
                return tuple([selected, n_comp])
                break
            else:
                continue


###############################################
# Variable Lab

import pandas as pd
import numpy as np

v1 = map_many(rnormal(5000), abs, int)
v2 = map_many(correlatedValue(v1, loc=4), abs)
v3 = map_many(correlatedValue(v1, loc=10, r=0.9), abs)
v4 = map_many(correlatedValue(get_chisquare(5000), r=0.9) * v1 + randn_skew_fast(5000),abs,int)
v5 = correlatedValue(get_intchisquare(5000), r=0.9)
v6 = map_many(correlatedValue(randn_skew_fast(5000, loc=7), r=0.9), abs)
v7 = 0.3 * v1 + 0.2 * v2 + 0.7 * v3 + 0.1 * v4

var_tit = ['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7']

'''
titles = outunicode(read_xsl('D:\Judicial\syntax\scripts\Variables.xlsx')[0])
var = outunicode(read_xsl('D:\Judicial\syntax\scripts\Variables.xlsx')[1])
categ = outunicode(read_xsl('D:\Judicial\syntax\scripts\Variables.xlsx', ind=1)[0])  # usadas para generar el random
probs = outunicode(read_xsl('D:\Judicial\syntax\scripts\Variables.xlsx', ind=1)[1])
data_frame = pd.DataFrame(matrix, columns=titles)
data_frame = pd.DataFrame.join(data_frame, pd.DataFrame(cat_var(categ, probs, n_obs=5000), columns=['Doc']))
# Saving dataset
data_frame.to_csv('simulacion.csv', index=False)
'''
df = pd.DataFrame(np.array([v1, v2, v3, v4, v5, v6, v7]).transpose(), columns=[var_tit])

df_corrplot(df)

'''
import matplotlib.pyplot as plt
import seaborn as sns

sns.distplot(v1)
sns.distplot(v2)
sns.distplot(v3)
sns.distplot(v4)
sns.distplot(v5)
sns.distplot(v6)
sns.distplot(v7)
plt.show()

sns.set()
sns.pairplot(df) # sns.pairplot(df, hue="Doc")
plt.show()

'''
