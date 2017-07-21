import pandas as pd
import numpy as np
# Lasso BIC/AIC based
import time
import matplotlib.pyplot as plt

from sklearn.linear_model import LassoLarsIC

data_frame = pd.read_csv('simulacion.csv')

# Split Dataset
y = data_frame.v7  # Y = data_frame.FUN_SCORE
X = data_frame[data_frame.columns[0:-1]]

# normalize data as done by Lars to allow for comparison, the data has the same distance from the origin
X /= np.sqrt(np.sum(X ** 2, axis=0))

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.10, random_state=42)

model_bic = LassoLarsIC(criterion='bic')
t1 = time.time()
model_bic.fit(X_train, y_train)
t_bic = time.time() - t1
alpha_bic_ = model_bic.alpha_

model_aic = LassoLarsIC(criterion='aic')
model_aic.fit(X_train, y_train)
alpha_aic_ = model_aic.alpha_


def plot_ic_criterion(model, name, color):
    alpha_ = model.alpha_
    alphas_ = model.alphas_
    criterion_ = model.criterion_
    plt.plot(-np.log10(alphas_), criterion_, '--', color=color,
             linewidth=3, label='%s criterion' % name)
    plt.axvline(-np.log10(alpha_), color=color, linewidth=3,
                label='alpha: %s estimate' % name)
    plt.xlabel('-log(alpha)')
    plt.ylabel('criterion')


plt.figure()
plot_ic_criterion(model_aic, 'AIC', 'b')
plot_ic_criterion(model_bic, 'BIC', 'r')
plt.legend()
plt.title('Information-criterion for model selection (training time %.3fs)'
          % t_bic)
plt.show()

# Predictions, new incoming documents/user_iterations
import sklearn.metrics as metrics


def mape(y_pred, y_true):
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100


y_pred_aic = model_aic.predict(X_test)
print('{:0.4f}'.format(metrics.mean_squared_error(y_test, y_pred_aic)))
print('{:0.4f}'.format(metrics.mean_absolute_error(y_test, y_pred_aic)))
print('{:0.4f}'.format(mape(y_test, y_pred_aic)))

y_pred_bic = model_bic.predict(X_test)
print('{:0.4f}'.format(metrics.mean_squared_error(y_test, y_pred_bic)))
print('{:0.4f}'.format(metrics.mean_absolute_error(y_test, y_pred_bic)))
print('{:0.4f}'.format(mape(y_test, y_pred_bic)))

from sklearn import linear_model

lassocv = linear_model.LassoCV()
lassocv.fit(X_train, y_train)
alpha_aic_ = lassocv.alpha_

y_pred_lcv = lassocv.predict(X_test)
print('{:0.4f}'.format(metrics.mean_squared_error(y_test, y_pred_lcv)))
print('{:0.4f}'.format(metrics.mean_absolute_error(y_test, y_pred_lcv)))
print('{:0.4f}'.format(mape(y_test, y_pred_lcv)))

#######################
#   LASSO Regression  #
#######################
'''
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn import linear_model
from sklearn.preprocessing import scale

# Split Dataset
Y = data_frame.v7  # Y = data_frame.FUN_SCORE
X = data_frame[data_frame.columns[0:-1]]

# Cross Validation to select the best model

X_scaled = scale(X)
X_mat = X_scaled  # X_mat = X.as_matrix()
Y_mat = Y.as_matrix()

lassocv = linear_model.LassoCV()
lassocv.fit(X, Y)
lassocv_score = lassocv.score(X, Y)
lassocv_alpha = lassocv.alpha_
print('alpha: ', lassocv.alpha_, 'CV', lassocv.coef_)

plt.plot(lassocv.alphas_, lassocv.mse_path_[:, 1], '-ko')
plt.xlabel(r'$\alpha$')
plt.ylabel('Score')
sns.despine(offset=15)
plt.show()

# print results
mydict1 = dict(zip(X.columns, lassocv.coef_))
dict((k, v) for k, v in mydict1.items() if v != 0)

mydict2 = dict(zip(X.columns, lasso.coef_))
dict((k, v) for k, v in mydict2.items() if v != 0)
'''
