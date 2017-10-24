				

				DELETE FROM [Vimb].[dbo].[Union_of_the_networks_1]
				INSERT INTO [Vimb].[dbo].[Union_of_the_networks_1]
				SELECT blockid,
					   blockdate,
					   datename(dw, CONVERT (datetime,convert(char(8),[VIMB_NetworksFact].blockdate))) as weekday,
					   IIF(blocknumber<=0, 0, log(blocknumber) ) as blocknumber,
					   blocktime,
					   [VIMB_NetworksFact].cnlname,
					   factratebase,
					   forecastratebase,
					   isprime,
					   prgname,
					   pro2,
					   proid,
					   progid,
					   iif(prgname like '%сериал%', 1, 0) as is_series,
					   iif(prgname like '%х/ф%', 1, 0) as is_movie,
					   tnsblockfactdur,
					   tnsblockfactid,
					   tnsblockfacttime,
					   max_blockdate as border,
					   'OV' as VIMB_type
				  FROM [Vimb].[dbo].[VIMB_NetworksFact]
				  LEFT JOIN (SELECT cnlname, max(blockdate) as max_blockdate
						  FROM [Vimb].[dbo].[VIMB_NetworksFact]
						  WHERE blkadverttypeptr=2 AND tnsblockfactid<>0
						  Group BY cnlname
						) as OV
						ON OV.cnlname=[Vimb].[dbo].[VIMB_NetworksFact].cnlname
				  WHERE BlkAdvertTypePTR=2 AND blockdate<=max_blockdate