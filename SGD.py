import numpy as np
import pandas as pd

def stochastic_gradient_descent(X, y, w_init, eta=0.1, max_iter=1e3,
								min_weight_dist=1e-8, seed=42, verbose=False):
								
    # Инициализируем расстояние между векторами весов на соседних
    # итерациях большим числом. 
	weight_dist = np.inf

	# Инициализируем вектор весов
	w = w_init

	# Сюда будем записывать ошибки на каждой итерации
	errors = []

	# Счетчик итераций
	iter_num = 0

	# Основной цикл
	for i in range(int(max_iter)):
		iter_num += 1
		
        # порождаем псевдослучайный 
        # индекс объекта обучающей выборки
		random_ind = np.random.randint(X.shape[0])

		w_new = stochastic_gradient_step(X, y, w, random_ind, 10 / iter_num)
		weight_dist = np.linalg.norm(w - w_new)
		w = w_new
		errors.append(mserror(y, linear_prediction(X, w)))
		
		
		if weight_dist <= min_weight_dist:
			break
			
	return w, errors
	
def stochastic_gradient_step(X, y, w, train_ind, eta=0.1):
	return w + 2 * eta/X.shape[0] * X[train_ind] * (y[train_ind] - linear_prediction(X[train_ind], w) + \
                                                    (0.9 * w.sum() if y[train_ind] > linear_prediction(X[train_ind], w) else 0 ))
	 
def linear_prediction(X, w):
	return np.dot(X, w)
	
def mserror(y, y_pred):
	y_pred=y_pred.reshape([len(y),1])
	return np.mean((y-y_pred)**2)