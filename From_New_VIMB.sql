				

				INSERT INTO [Vimb].[dbo].[Union_of_the_networks_1]
				SELECT blockid,
					   blockdate,
					   datename(dw, CONVERT (datetime,convert(char(8),[NewVimb].[dbo].[VIMB_Networks].blockdate))) as weekday,
					   IIF(blocknumber<=0, 0, log(blocknumber) ) as blocknumber,
					   blocktime,
					   [VIMB_Networks].cnlname,
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
					   min_blockdate as border,
					   'NV' as VIMB_type
				  FROM [NewVimb].[dbo].[VIMB_Networks]
				  JOIN (SELECT cnlname, min(blockdate) as min_blockdate
						  FROM [NewVimb].[dbo].[VIMB_Networks]
						  WHERE blkadverttypeptr=32 AND tnsblockfactid<>0
						  Group BY cnlname
						  ) AS NV
						  ON NV.cnlname=[NewVimb].[dbo].[VIMB_Networks].cnlname
				  WHERE blockdate>=min_blockdate AND 
						BlkAdvertTypePTR=32 AND 
						(update_date=(SELECT MAX(update_date) FROM [NewVimb].[dbo].[VIMB_Networks]) OR ISFact=1)