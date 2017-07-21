import matplotlib.pyplot as plt
import numpy as np
from sklearn.decomposition import PCA
import pandas as pd
import scipy.stats as ss

titles = ['TIEMPO_LECTURA_REAL',
          'NO_NOTAS',
          'NO_MARCADORES',
          'NO_HIGHLIGHTS',
          'PORCENTAJE_MARCADORES',
          'NO_ENVIOS',
          'NO_Descargas']
data_frame = pd.read_csv('simulacion.csv')
data_frame = pd.DataFrame(data_frame.as_matrix(), columns=titles)


#################################################
# PCA

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


# Scaling data
df_scaled = data_frame[titles]
df_scaled = pd.DataFrame(ss.zscore(df_scaled), columns=titles)

# PCA
[m, n] = df_scaled.shape
pca = PCA(n_components=n)
pca = pca.fit(df_scaled)

# covariance matrix
mean_vec = np.mean(df_scaled, axis=0)
cov_mat = (df_scaled - mean_vec).T.dot((df_scaled - mean_vec)) / (df_scaled.shape[0] - 1)

# eigenvals y eigenvec
eig_vals, eig_vecs = np.linalg.eig(cov_mat)

# Explicacion de la varianza
var = pca.explained_variance_ratio_

# acumulative error list
var1 = np.cumsum(np.round(pca.explained_variance_ratio_, decimals=4) * 100)
corte = find_ncomp(var1, umbral=70)

# loadings
components_loadings = np.dot(eig_vecs, np.sqrt(eig_vals))[0:corte[1]]
percents = (np.round(pca.explained_variance_ratio_, decimals=4) * 100)[:corte[1]]

# acumulative error plot
plt.plot(range(0, len(var1)), var1)
plt.scatter(range(0, len(var1)), var1)
plt.axvline(corte[1])
plt.grid()
plt.show()

# The typical goal of a PCA is to reduce the dimensionality of the original feature space
# by projecting it onto a smaller subspace, where the eigenvectors will form the axes.
# However, the eigenvectors only define the directions of the new axis, since they have all the same
# unit length 1, which can confirmed by the following two lines of code:

for ev in eig_vecs:
    np.testing.assert_array_almost_equal(1.0, np.linalg.norm(ev))
print('Todo ok!')

# Make a list of (eigenvalue, eigenvector) tuples
eig_pairs = [(np.abs(eig_vals[i]), eig_vecs[:, i]) for i in range(len(eig_vals))]
# Sort the (eigenvalue, eigenvector) tuples from high to low
eig_pairs.sort()
eig_pairs.reverse()

# Visually confirm that the list is correctly sorted by decreasing eigenvalues
print('Eigenvalues in descending order:')
for i in eig_pairs:
    print(i[0])


# projection onto the new feature space
w = np.hstack((eig_pairs[0][1].reshape(n, 1),
               eig_pairs[1][1].reshape(n, 1)))

Y = df_scaled.dot(w)  # pca.fit_transform(df_scaled)

# component scores
# Factor Score = np.sum(standarized_score * components_loadings)
standarized_score = np.divide(percents, np.sum(percents))

#########################
#  Creando composite    #
#########################

# Components
comp = pca.components_[:corte[1]].T

# for the composite
data_frame.as_matrix()  # vemos los datos

# weighted matrix data dimension reduction
W = data_frame.as_matrix().dot(comp)

# Weighted matrix with standarized_score relevance
regre_coeff= W.dot(np.diag(standarized_score))
composite = [sum(i) for i in regre_coeff]