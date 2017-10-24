import logging
import warnings
import datetime
import traceback
import numpy   as np
import pandas  as pd

from sqlalchemy 	import create_engine
from datetime 		import date, timedelta
from sqlalchemy.orm import scoped_session, sessionmaker

import samples, models, SGD, Execute
warnings.filterwarnings("ignore")

ENGINE_zeus      = create_engine('mssql+pymssql://palomars\Zeus/Vimb?charset=cp1251')
ENGINE_utf8_zeus = create_engine('mssql+pymssql://palomars\Zeus/Vimb')
ENGINE 			 = create_engine('mssql+pymssql://palomars/Forecast_data')

Session = scoped_session(sessionmaker(bind=ENGINE))
s = Session()

logging.basicConfig(filename='log_file.log', level=logging.INFO,
					format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')
logger = logging.getLogger('MainLogger')
logger.info( u'Program started' )

days  = {
	['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][i-1]: i for i in range(1, 8)
}

Channels = ['2х2',
 '5 канал',
 'Домашний',
 'Звезда',
 'Канал Дисней',
 'Карусель',
 'Матч ТВ',
 'Муз ТВ',
 'НТВ',
 'Первый',
 'Пятница',
 'РЕН ТВ',
 'Россия 1',
 'Россия 24',
 'СТС',
 'СТС ЛАВ',
 'ТВ Центр',
 'ТВ3',
 'ТНТ',
 'ТНТ4',
 'Че',
 'Ю']

# Выполним sql запросы
Execute.executeScriptsFromFile(logger)

# -3 дня, поскольку факт рейтингов приходит только через 3 дня
start_predict_day = datetime.datetime.today() - datetime.timedelta(days=3)
sample = samples.train_sample(logger, start_predict_day)  # Получим выборки

# Разделим выборку  
train = sample[sample.blockdate <  int(start_predict_day.strftime("%Y%m%d"))]  # на тренировочную
test  = sample[sample.blockdate >= int(start_predict_day.strftime("%Y%m%d"))]  # и тестовую

# по всем каналам, кроме тех, 
for ch in [i for i in Channels if i not in ['Канал Дисней', 'Муз ТВ']]: # которых еще нет в новом вимбе

	# Разные модели дня выходных и будней
	for day_type in [[0,1,2,5,6], [3, 4]]:
		logger.info( u'Model is building for ' + 
					  ch + 
					  ' and dates: ' +
					  ('weekend' if day_type == [3, 4] else 'weekdays') )

		# Возьмем выборку для канала
		train_ch = train[(train.cnlname==ch) & (train.weekday.isin(day_type))]	
		test_ch = test[(test.cnlname==ch) & (test.weekday.isin(day_type))]
		
		if train_ch.shape[0] and test_ch.shape[0]:
			try:
			
				"""	
				Избавимся от аномалий
				Предположим, что значения GRP распределены нормально.
				Тогда можно воспользоваться правилом 3-х сигм, которое гласит:
				практически все значения нормально распределённой случайной величины лежат в интервале
					(x - 3*sigma; x + 3*sigma)
				"""
				mean = train_ch.factratebase.mean() # Надем среднее
				std = train_ch.factratebase.std()   # Стандартное отклонение

				train_ch = train_ch[train_ch.factratebase < mean + 3*std]

				# NNLS_model
				columns, coef_ = models.NNLS_model(train_ch,
												   train_ch.factratebase.values)

				# CatBoost
				regr = models.CatBoost_model(train_ch[columns].values, 
											 train_ch.factratebase.values)

				# SGD
				stoch_grad_desc_weights, stoch_errors_by_iter = SGD.stochastic_gradient_descent(train_ch[columns].values, 
																								train_ch.factratebase.values, 
																								coef_)

				logger.info( str(list(zip(columns, 
										  coef_, 
										  stoch_grad_desc_weights))) )

				# Посчитаем прогноз по 3 моделям
				test_ch['Pred_CatBoost'] = regr.  predict(test_ch[columns].values)
				test_ch['Pred_SGD'] 	 = models.predict(stoch_grad_desc_weights, test_ch[columns])
				test_ch['Pred_NNLS'] 	 = models.predict(coef_, test_ch[columns])
				test_ch['Today'] 		 = (start_predict_day + datetime.timedelta(days=3)).strftime("%Y-%m-%d")

				# И занесем в базу
				test_ch.to_sql('Forecast_Networks', 
							   ENGINE_utf8_zeus, 
							   if_exists="append", 
							   index=False)
							   
				logger.info( u'Forecast is loaded' )
			except Exception:
				logger.exception("exception")
		else:
			logger.error( u'Sample is enty !' )
			
logger.info( u'Program finished\n\n\n' )



