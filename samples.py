import numpy
import pandas as pd

from memory_profiler import profile
from datetime 		 import date, timedelta
from timeit 		 import default_timer as timer
from sqlalchemy 	 import create_engine
from sqlalchemy.orm  import scoped_session, sessionmaker

ENGINE = create_engine("mssql+pymssql://:STONE\palomars@localhost/Forecast_data?charset=cp1251")
ENGINE_zeus = create_engine('mssql+pymssql://palomars\Zeus/Vimb?charset=cp1251')

days  = {
	['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][i-1]: i for i in range(1, 8)
}

fp=open('memory_profiler.log', 'w+')
@profile(stream=fp)
def train_sample(logger, start_predict_day):
	"""
	train_sample получает на вход
	стартовый день прогноза,
	возвращает дата фрейм с
	тренировочной и тестовой
	выборками
	"""

	start_ = timer()
	network_res = pd.DataFrame() # Тестовая выборка
	n_furures = 14;				 # За прошедший период тянем по 2 недели

	for delta in range(98, -14, -14):
		start = timer()

		cur_day = start_predict_day - timedelta(days = delta + 1)

		if (cur_day >= start_predict_day - timedelta(days = 1)):  # Если за будующий
			n_furures = 365;										# то до конца года

		# Считываем всю сетку до cur_day
		network = pd.read_sql('''
										SELECT distinct blockdate % 7 as weekday,
														cnlname,
														blocktime,
														blockdate,
														factratebase
										FROM [Vimb].[dbo].[Union_of_the_networks_1]
										WHERE
											  blockdate>=20170110 AND
											  blockdate<={0} '''.format(cur_day.strftime("%Y%m%d")),
								ENGINE_zeus)


		# Обработка их
		network['hour'] = network.blocktime.apply(lambda x: round(x // 1800))
		network['hour'] = [ i if i <= 48 else i - 48 for i in network['hour'].values ]

		# Получаем сетку за неделю до cur_day и за месяц
		network_week  = network[network.blockdate>=int((cur_day - timedelta(days = 7)).strftime("%Y%m%d"))]
		network_month = network[network.blockdate>=int((cur_day - timedelta(days = 31)).strftime("%Y%m%d"))]

		# Получаем средние GRP
		Average_GRP 	  = network.	  groupby(['weekday', 'cnlname', 'hour'])['factratebase'].mean().reset_index()
		Average_GRP_week  = network_week. groupby(['weekday', 'cnlname', 'hour'])['factratebase'].mean().reset_index()
		Average_GRP_month = network_month.groupby(['weekday', 'cnlname', 'hour'])['factratebase'].mean().reset_index()

		Average_GRP.	  columns = ['weekday', 'cnlname', 'hour', 'average_grp']
		Average_GRP_week. columns = ['weekday', 'cnlname', 'hour', 'average_grp_week']
		Average_GRP_month.columns = ['weekday', 'cnlname', 'hour', 'average_grp_month']
		
		del network

		# Берем по proid, поскольку они совпадают в старом и новом вимбе
		Avarage_progid = pd.read_sql('''
										SELECT  SUM(factratebase) / COUNT(factratebase) as average_grp_line_prg,
												proid,
												blocktime / 1800 as hour,
												blockdate % 7 as weekday
										FROM [Vimb].[dbo].[Union_of_the_networks_1]
										WHERE
												blockdate<={0}
										GROUP BY
												 proid,
												 blocktime / 1800,
												 blockdate % 7 '''.format(cur_day.strftime("%Y%m%d")), ENGINE_zeus)
												 
		Avarage_pro2 = pd.read_sql('''
										SELECT  SUM(factratebase) / COUNT(factratebase) as average_pro2,
												pro2,
												blocktime / 1800 as hour,
												cnlname
										FROM [Vimb].[dbo].[Union_of_the_networks_1]
										WHERE blockdate<={0} AND
											  pro2 <> 0
										GROUP BY
												 pro2,
												 cnlname,
												 blocktime / 1800 '''.format(cur_day.strftime("%Y%m%d")), ENGINE_zeus)

		# Получаем сетку с cur_day + 2 недели
		network_res_in = pd.read_sql('''
										SELECT [blockid]
											  ,[blockdate]
											  ,blockdate % 7 as weekday
											  ,[blocknumber]
											  ,[blocktime]
											  ,[cnlname]
											  ,[factratebase]
											  ,[forecastratebase]
											  ,[isprime]
											  ,[prgname]
											  ,[pro2]
											  ,[proid]
											  ,[progid]
											  ,[is_series]
											  ,[is_movie]
											  ,[tnsblockfactdur]
											  ,[tnsblockfactid]
											  ,[tnsblockfacttime]
											  ,[border]
											  ,[VIMB_type]
										FROM [Vimb].[dbo].[Union_of_the_networks_1]
										WHERE
											   [Union_of_the_networks_1].blockdate> {0} AND
											   [Union_of_the_networks_1].blockdate<={1} '''.format(cur_day.strftime("%Y%m%d"),
																								(cur_day + timedelta(days=n_furures)).strftime("%Y%m%d")), ENGINE_zeus)
		#network_res_in = network_res_in.replace({"weekday": days})
		network_res_in['hour'] = network_res_in.blocktime.apply(lambda x: round(x // 1800))


		# Объеденим выборку за текущие 2 недели со средними за прошлый период
		network_res_in = network_res_in.merge(Average_GRP, 		 how='left', on=['weekday', 'cnlname', 'hour'])
		network_res_in = network_res_in.merge(Average_GRP_week,  how='left', on=['weekday', 'cnlname', 'hour'])
		network_res_in = network_res_in.merge(Average_GRP_month, how='left', on=['weekday', 'cnlname', 'hour'])
		network_res_in = network_res_in.merge(Avarage_progid, 	 how='left', on=['proid', 'hour', 'weekday'])
		network_res_in = network_res_in.merge(Avarage_pro2,   	 how='left', on=['pro2', 'hour', 'cnlname'])



		if not network_res.shape[0]:
			network_res = network_res_in
		else:
			network_res = network_res.append(network_res_in)

		end = timer()
		logger.info( u'Data is processed to ' + 
					  cur_day.strftime("%Y%m%d") + 
					  ' for time: ' + 
					  str(round(end - start, 2)) )

	# Заполним все пропуски
	network_res.average_grp.	 		fillna(network_res.average_grp.mean(), inplace=True)
	network_res.average_grp_week.		fillna(network_res.average_grp, inplace=True)
	network_res.average_grp_month.		fillna(network_res.average_grp, inplace=True)
	network_res.average_grp_line_prg.	fillna(network_res.average_grp, inplace=True)
	network_res.average_pro2.			fillna(network_res.average_grp_line_prg, inplace=True)

	# Создадим столбцы с разницей между ленейкой программы 
	network_res['Movies_diff'] = network_res.average_pro2 - network_res.average_grp_line_prg  

	network_res['Series_diff'] = network_res['Movies_diff'] * network_res['is_series']  # и конкретным сериалом
	network_res['Movies_diff'] = network_res['Movies_diff'] * network_res['is_movie']   # и конкретным фильмом

	end_ = timer()
	logger.info( u'Total sampling time is: ' + 
				  str(round(end_ - start_, 2)) )

	return network_res
