#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Carga de dependencias
import sys
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler
import psycopg2 as p
from pprint import pprint

# controlamos que se introduzcan 6 argumentos ( aunque la llamada de RDD consuma 5, la 1 es el nombre del script)
if len(sys.argv) == 7:
    print "Ejecutando Proceso RDD para: " + sys.argv[1] + " " + sys.argv[2]

else:
    raise Exception(
        "Fallo de llamada de script con los argumentos: " + sys.argv[1] + " " + sys.argv[2]
        + " con el password e IP, por favor revise otra vez los parametros")

if sys.argv[6].upper() == "T":
    print('Ejecutando modo spark..')
    PATH = "hdfs:///user/ijdocs/"
    import subprocess
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SQLContext, HiveContext
    from pyspark.sql.types import *
    from pyspark.sql import DataFrame as SparkDataframe

    # Crear la configuracion de spark
    conf = (SparkConf()
            .setAppName("ejemplo")
            .set("spark.executor.memory", "1g")
            .set("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/usr/bin/python")
            .set("spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON", "/usr/bin/python"))

    # Crear el SparkContext con la configuración anterior
    sc = SparkContext(conf=conf)

    # Conectores SQL para trabajar en nuestro script
    sqlContext = SQLContext(sc)
    hiveContext = HiveContext(sc)


# Functions and Classes
class DBConn:
    def __init__(self, dbname, dbuser, dbpass, host, port):
        con_str = str(
            'dbname=' + '\'' + dbname + '\'' + ' user=' + '\'' + dbuser + '\'' + ' host=' + '\'' + host + '\''
            + ' password=' + '\'' + dbpass + '\'' + ' port=' + '\'' + port + '\'')
        pprint("Conectando a: [%s]..." % con_str)
        try:
            self.connection = p.connect(con_str)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            pprint("Conectado")

        except (Exception, p.DatabaseError) as error:
            pprint(error)

    def insert_record(self, table, rows, values):
        # Inserta Registros en la tabla indicada.
        query_insert = "INSERT INTO " + table + " ( " + concat(rows, ",") + " ) " + " VALUES (" + concat(values) + ")"
        pprint(query_insert)
        self.cursor.execute(query_insert)

    def query_all(self, table):
        # Consulta de toda la tabla
        query = "SELECT * FROM " + table
        pprint(query)
        return pd.read_sql_query(query, self.connection)

    def update_record(self, table, cambio, condicion):
        # en construccion, necesaria
        query_update = "UPDATE " + table + " SET " + cambio + ' where ' + condicion
        pprint(query_update)
        self.cursor.execute(query_update)

    def execute_query(self, query):
        return pd.read_sql_query(query, self.connection)

    def free_execute(self):
        return self.cursor.execute

    def close_connection(self):
        self.cursor.close()
        self.connection.close()


class Group:
    def __init__(self, df):
        self.df = pd.DataFrame(zip(*get_score(df).groupby('ids').mean().score.to_dict().items())).T

    def mean_level_1(self):
        self.df['jurisdiction'], self.df['classtype'] = zip(*self.df[0].apply(lambda x: x.split('/_/', 1)))
        self.df.iloc[:, 1] = standardize_index(self.df.iloc[:, 1])
        self.df.rename(columns={1: 'score'}, inplace=True)
        return self.df[filtra([0], self.df)][['jurisdiction', 'classtype', 'score']]

    def mean_level_2(self):
        self.df['jurisdiction'], self.df['casematter'], self.df['processtype'], self.df['classtype'] = zip(
            *self.df[0].apply(lambda x: x.split('/_/', 3)))
        self.df.iloc[:, 1] = standardize_index(self.df.iloc[:, 1])
        self.df.rename(columns={1: 'score'}, inplace=True)
        return self.df[['jurisdiction', 'casematter', 'processtype', 'classtype', 'score']]

    def mean_level_owner1(self):
        self.df['owner'], self.df['jurisdiction'], self.df['classtype'] = zip(
            *self.df[0].apply(lambda x: x.split('/_/', 2)))
        self.df.iloc[:, 1] = standardize_index(self.df.iloc[:, 1])
        self.df.rename(columns={1: 'score'}, inplace=True)
        return self.df[['owner', 'jurisdiction', 'classtype', 'score']]

    def mean_level_owner2(self):
        self.df['owner'], self.df['jurisdiction'], self.df['casematter'], self.df['processtype'], self.df[
            'classtype'] = zip(*self.df[0].apply(lambda x: x.split('/_/', 4)))
        self.df.iloc[:, 1] = standardize_index(self.df.iloc[:, 1])
        self.df.rename(columns={1: 'score'}, inplace=True)
        return self.df[['owner', 'jurisdiction', 'casematter', 'processtype', 'classtype', 'score']]


def unificar_df(df):
    df.columns = pd.io.parsers.ParserBase({'names': df.columns})._maybe_dedup_names(df.columns)
    return df


def filtra(out_ele, obj):
    return filter(lambda o: o not in out_ele, list(obj))


def filter_bd(x):
    # elimina registros de apertura y cierre de sesion ya que no hay datos relevantes
    return x.drop(x[(x.type == 0) & (x.subtype == 1) | (x.type == 0) & (x.subtype == 2)].index)


def find_ncomp(lista_array, umbral=70):
    import numpy as np
    if type(lista_array) is list:
        for i in lista_array:
            if i >= umbral:
                selected = i
                n_comp = lista_array.tolist().index(i) + 1
                return tuple([selected, n_comp])
            else:
                continue
    if type(lista_array) is np.ndarray:
        for i in lista_array:
            if i >= umbral:
                selected = i
                n_comp = lista_array.tolist().index(i) + 1
                return tuple([selected, n_comp])
            else:
                continue


def get_eff(x):
    import warnings
    import numpy as np
    warnings.filterwarnings("error")
    try:
        return effort(x)
    except RuntimeWarning:
        return 0


def summation(xx, yy):
    return sum(list((map(lambda x, y: x * y, xx, yy))))


def get_score(data_frame, standarize=True):
    """Scoring of Dataframe from numeric obs
    Input: Dataframe with at least 2 numeric cols
    Output: The input with one more columns (the score)
    """
    if standarize:
        df_scaled = map_std(data_frame.select_dtypes(include=['float', 'int']))
    else:
        df_scaled = data_frame.select_dtypes(include=['float', 'int'])
    # Fitting PCA
    pca = fit_pca(df_scaled)

    # Autoselect components
    corte = find_ncomp(acum(pca.explained_variance_ratio_), umbral=70)

    """
    NSI = W1 (Factor 1 score) + W2 (Factor 2 score) + ... + Wn (Factor n score)
    Using the proportion of these percentages as weights on the factor score coefficients,
    a Non- standardized Index (NSI) was developed
    """
    # Computing Weights:  (53.05/55.69) (F1s) + (36.68/55.69) (F2s)
    W = np.divide(pca.explained_variance_ratio_[:corte[1]] * 100, corte[0])
    # Computing Projections
    X = np.array(map(df_scaled.dot, pca.components_)[0:corte[1]])
    # Computing Index (resultado NSI)
    NSI = summation(X, W)

    # Computing SI
    """This index measures the socioeconomic status of one DA relative to the other on a linear
    scale. The value of the index can be positive or negative, making it difficult to interpret.
    Therefore, a Standardized Index (SI) was developed, the value of which can range from 0
    to 100, using the formula"""
    SI = standardize_index(NSI)
    data_frame = data_frame.assign(score=SI)

    return data_frame


# LAMBDA
def map_std(df1):
    return pd.DataFrame(MinMaxScaler().fit(df1).transform(df1))


def acum(x):
    return np.cumsum(np.round(x, decimals=4) * 100)


def fit_pca(x):
    return PCA(n_components=x.shape[1]).fit(x)


def standardize_index(x):
    return (x - min(x)) / (max(x) - min(x)) * 100


def get_time(x):
    return x[x.argmax()] - x[x.argmin()]


def deconcat(x, elem):
    return '/_/'.join(x.split('/_/')[0:elem])


def count_uniques(x):
    return x.unique().shape[0]


def get_uniques(x):
    return float(x.unique())


concat = lambda x, sep: reduce(lambda s, y: str(s) + sep + str(y), x)

hash_list = lambda s: abs(hash(concat(s, ','))) % (10 ** 8)


def effort(x):
    return np.nanmean(np.divide(x.groupby('paper').agg({'pages': count_uniques}).pages.as_matrix(),
                                x.groupby('paper').agg({'sheets': 'max'}).sheets.as_matrix()))


def do_rdd(dbname, dbuser, dbpass, host, port):
    db_conn = DBConn(dbname, dbuser, dbpass, host, port)

    # Megajoin de tablas
    DB = db_conn.execute_query(
        'SELECT dm_session.*, dm_accesses.*, dm_paper_access.* FROM dm_accesses LEFT OUTER JOIN dm_session ON dm_session."id" = dm_accesses.sessionid LEFT OUTER JOIN dm_paper_access ON dm_paper_access."id" = dm_accesses.paper GROUP BY dm_session.id, dm_accesses.id, dm_paper_access.id ORDER BY accessdate')
    db_conn.cursor.execute('DELETE FROM kdd_paperclassenum')

    # Preprocessing
    DB = unificar_df(DB)
    duplicados = [s for s in list(DB) if "." in s]
    duplicados.extend(["id"])

    # creacion de label combinado para simplificar syntaxis en operaciones
    DB['label'] = DB.type.astype(str) + '/_/' + DB.subtype.astype(str)

    # eliminamos campos con los que no trabajamos
    DB = DB[filtra(duplicados, DB)]
    DB = DB[DB.status != 500][['owner', 'sessionid', 'nig',
                               'jurisdiction', 'processtype', 'casematter', 'subprocesstype', 'classtype',
                               'accessdate', 'type', 'subtype', 'label', 'paper', 'page', 'pages', 'sheets']]

    DB = filter_bd(DB).drop_duplicates().dropna(subset=['nig'])[
        ['owner', 'sessionid', 'jurisdiction', 'processtype', 'casematter', 'subprocesstype', 'classtype',
         'accessdate', 'type', 'subtype', 'label', 'paper', 'page', 'pages', 'sheets']]
    #######################################################################################################
    # Calculo Modelo Generico (2 niveles de agregacion)
    #######################################################################################################
    # Calculos a nivel: jurisdiction + classtype ( 1 nivel)
    # Prepreocesing
    colNames = ['ids', 'time_elapsed', 'uniques', 'effort']
    req = "INSERT INTO kdd_paperclassenum values(%s,%s,%s,%s,%s,%s,%s,%s)"

    if set(DB.jurisdiction) != {None}:
        DB1 = DB[DB.jurisdiction.notnull() & DB.classtype.notnull()]
        DB1 = DB1[filter(lambda x: x not in ['casematter', 'processtype', 'subprocesstype'], DB1.columns)]
        DB1['db1_id'] = DB1.jurisdiction.astype(str) + '/_/' + DB1.classtype.astype(str) + '/_/' + DB1.sessionid.astype(
            str)
        DB1 = DB1[filter(lambda x: x not in ['sessionid', 'classtype', 'jurisdiction', 'type', 'subtype'], DB1.columns)]

        dfs = []
        for ids in set(DB1.db1_id):
            # deconcat son 2 para escoger solo los niveles jurisdiction y classtype unidos por '/_/'
            dfs.append([deconcat(ids, 2),
                        np.float32(get_time(DB1[(DB1['db1_id'] == ids)]['accessdate'].values).astype('timedelta64[m]')),
                        count_uniques(DB1[(DB1['db1_id'] == ids)].label),
                        get_eff(DB1[(DB1['db1_id'] == ids)])])

        DB1_result = pd.DataFrame(dfs, columns=colNames)
        DB1_result = DB1_result[DB1_result.time_elapsed != 0]

        for juri in set(DB.jurisdiction):
            try:
                df = Group(DB1_result[DB1_result.ids.str.startswith(juri + '/_/')]).mean_level_1()
                for index, row in df.iterrows():
                    res = [hash_list(row[df.columns].tolist())] + row[df.columns].tolist()
                    values = [res[0], res[2], None, res[1], None, None, None, int(res[3])]
                    db_conn.cursor.execute(req, tuple(values))
            except ValueError:
                pass

                # Calculos a nivel: jurisdiction + casematter + processtype + classtype ( 2 nivel)
                # Prepreocesing
    if (set(DB.jurisdiction) != {None}) & (set(DB.casematter) != {None}) & (set(DB.processtype) != {None}):
        DB2 = DB[DB.casematter.notnull() & DB.processtype.notnull() & DB.classtype.notnull()]
        DB2 = DB2[filter(lambda x: x not in ['subprocesstype'], DB2.columns)]
        DB2['db2_id'] = DB2.jurisdiction.astype(str) + '/_/' + DB2.casematter.astype(
            str) + '/_/' + DB2.processtype.astype(str) + '/_/' + DB2.classtype.astype(
            str) + '/_/' + DB2.sessionid.astype(str)
        DB2 = DB2[filter(
            lambda x: x not in ['sessionid', 'classtype', 'jurisdiction', 'casematter', 'processtype', 'type',
                                'subtype'], DB2.columns)]

        dfs = []
        for ids in set(DB2.db2_id):
            dfs.append([deconcat(ids, 4),
                        np.float32(get_time(DB2[(DB2['db2_id'] == ids)]['accessdate'].values).astype('timedelta64[m]')),
                        count_uniques(DB2[(DB2['db2_id'] == ids)].label),
                        get_eff(DB2[(DB2['db2_id'] == ids)])])

        DB2_result = pd.DataFrame(dfs, columns=colNames)
        DB2_result = DB2_result[DB2_result.time_elapsed != 0]

        for juri in set(DB.jurisdiction):
            try:
                df = Group(DB2_result[DB2_result.ids.str.startswith(juri + '/_/')]).mean_level_2()
                for index, row in df.iterrows():
                    res = [hash_list(row[df.columns].tolist())] + row[df.columns].tolist()
                    values = [res[0], res[4], None, res[1], res[2], res[3], None, int(res[5])]
                    db_conn.cursor.execute(req, tuple(values))
            except ValueError:
                pass
    ###################################################################################################
    # Modelo Personalizado
    ###################################################################################################
    # Calculos a nivel 1 por Owner: Owner + jurisdiction + classtype ( 1 nivel)
    if (set(DB.jurisdiction) != {None}) & (set(DB.owner) != {None}):
        #     Prepreocesing
        DB_O_1 = DB[DB.owner.notnull() & DB.jurisdiction.notnull() & DB.classtype.notnull()]
        DB_O_1 = DB_O_1[filter(lambda x: x not in ['casematter', 'processtype', 'subprocesstype'], DB_O_1.columns)]
        DB_O_1['db1_id'] = DB_O_1.owner.astype(str) + '/_/' + DB_O_1.jurisdiction.astype(
            str) + '/_/' + DB_O_1.classtype.astype(str) + '/_/' + DB_O_1.sessionid.astype(str)
        DB_O_1 = DB_O_1[
            filter(lambda x: x not in ['sessionid', 'classtype', 'jurisdiction', 'type', 'subtype'], DB_O_1.columns)]

        dfs = []
        for ids in set(DB_O_1.db1_id):
            # deconcat son 2 para escoger solo los niveles jurisdiction y classtype unidos por '/_/'
            dfs.append([deconcat(ids, 3),
                        np.float32(
                            get_time(DB_O_1[(DB_O_1['db1_id'] == ids)]['accessdate'].values).astype('timedelta64[m]')),
                        count_uniques(DB_O_1[(DB_O_1['db1_id'] == ids)].label),
                        get_eff(DB_O_1[(DB_O_1['db1_id'] == ids)])])

        DB_O_1_result = pd.DataFrame(dfs, columns=colNames)
        DB_O_1_result = DB_O_1_result[DB_O_1_result.time_elapsed != 0]

        for owner in set(DB.owner):
            try:
                df = Group(DB_O_1_result[DB_O_1_result.ids.str.startswith(owner + '/_/')]).mean_level_owner1()
                for index, row in df.iterrows():
                    res = [hash_list(row.tolist())] + row.tolist()
                    values = [res[0], res[3], res[1], res[2], None, None, None, int(res[4])]
                    # id code owner jurisdiction classmatter processtype stage ranking
                    db_conn.cursor.execute(req, tuple(values))
            except:
                pass

    if (set(DB.jurisdiction) != {None}) & (set(DB.owner) != {None}) & (set(DB.casematter) != {None}) & (
                set(DB.processtype) != {None}):
        # Prepreocesing

        DB_O_2 = DB[DB.owner.notnull() & DB.jurisdiction.notnull() & DB.classtype.notnull()]
        DB_O_2 = DB_O_2[filter(lambda x: x not in ['subprocesstype'], DB_O_2.columns)]
        DB_O_2['db1_id'] = DB_O_2.owner.astype(str) + '/_/' + DB_O_2.jurisdiction.astype(
            str) + '/_/' + DB_O_2.casematter.astype(str) + '/_/' + DB_O_2.processtype.astype(
            str) + '/_/' + DB_O_2.classtype.astype(str) + '/_/' + DB_O_2.sessionid.astype(str)
        DB_O_2 = DB_O_2[filter(
            lambda x: x not in ['owner', 'sessionid', 'classtype', 'jurisdiction', 'casematter', 'processtype', 'type',
                                'subtype'], DB_O_2.columns)]

        dfs = []
        for ids in set(DB_O_2.db1_id):
            # deconcat son 2 para escoger solo los niveles jurisdiction y classtype unidos por '/_/'
            dfs.append([deconcat(ids, 5),
                        np.float32(
                            get_time(DB_O_2[(DB_O_2['db1_id'] == ids)]['accessdate'].values).astype('timedelta64[m]')),
                        count_uniques(DB_O_2[(DB_O_2['db1_id'] == ids)].label),
                        get_eff(DB_O_2[(DB_O_2['db1_id'] == ids)])])

        DB_O_2_result = pd.DataFrame(dfs, columns=colNames)
        DB_O_2_result = DB_O_2_result[DB_O_2_result.time_elapsed != 0]

        for owner in set(DB.owner):
            try:
                df = Group(DB_O_2_result[DB_O_2_result.ids.str.startswith(owner + '/_/')]).mean_level_owner2()
                for index, row in df.iterrows():
                    res = [hash_list(row[df.columns].tolist())] + row[df.columns].tolist()
                    values = [res[0], res[5], res[1], res[2], res[3], res[3], None, int(res[6])]
                    # id code owner jurisdiction classmatter processtype stage ranking
                    db_conn.cursor.execute(req, tuple(values))
            except ValueError:
                pass


if __name__ == "__main__":
    script = sys.argv[0]
    dbname = sys.argv[1]
    dbuser = sys.argv[2]
    dbpass = sys.argv[3]
    host = sys.argv[4]
    port = sys.argv[5]
    print('Procesando script ' + script + ' ...')
    do_rdd(dbname, dbuser, dbpass, host, port)
