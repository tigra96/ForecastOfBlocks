import itertools
import pandas as pd

from scipy.optimize 		 import nnls
from sklearn.model_selection import GridSearchCV
from sklearn 				 import datasets, linear_model
from catboost 				 import Pool, CatBoostRegressor


def NNLS_model(X, y):
	"""
	Non-negative least squares solver
	"""
	
	columns = ['average_grp',  
			   'average_grp_week', 
			   'average_grp_month', 
			   'average_grp_line_prg', 
			   'isprime', 
			   'blocknumber', 
			   'Series_diff', 
			   'Movies_diff']

	x, rnorm = nnls(X[columns], y)

	return (columns, x)
	
def predict(coef_, X): return (coef_ * X).sum(axis=1)
	
def CatBoost_model(X, y):
	
	param_grid = [
				{'iterations': [5, 10], 
				 'depth': [int(i) for i in np.linspace(5, 10, 6)]},
		]
	Regressor = CatBoostRegressor()
	
	grid = GridSearchCV(Regressor, param_grid)
	grid.fit(X, y)
	
	best_params_ = grid.best_params_
	
	Regressor = CatBoostRegressor(iterations=best_params_['iterations'],
								  depth=best_params_['depth'],
								  loss_function='RMSE')
    
	Regressor.fit(X, y)
	return Regressor