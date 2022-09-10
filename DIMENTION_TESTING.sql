--1 DIM_CHARGE_CATEG_SQL
CREATE TABLE DIM_CHARGE_CATEG_SQL_IN1542(
Charge_categ_Key	int	   identity(1,1) primary key         ,       
CHARGE_CATEG_ID	int	              NOT NULL  ,         
TENANT_ORG_ID	int               NOT NULL  ,       
CHARGE_CATEG	varchar(50)	      NOT NULL  ,       
CHARGE_CATEG_DESC	varchar(50)	  NOT NULL  ,       
TAX_IND	int	                      NOT NULL,       
VERSION INT
        ) ;	
		
SELECT * FROM DIM_CHARGE_CATEG_SQL_IN1542
 INSERT INTO DIM_CHARGE_CATEG_SQL_IN1542(
 CHARGE_CATEG_ID,TENANT_ORG_ID,CHARGE_CATEG,CHARGE_CATEG_DESC,TAX_IND,VERSION)

 SELECT 
 DISTINCT
 ISNULL(CONVERT(INT,LTRIM(RTRIM(CHARGE_CATEG_ID))),101) AS CHARGE_CATEG_ID ,
 ISNULL(CONVERT(INT,LTRIM(RTRIM(TENANT_ORG_ID))),101)
AS TENANT_ORG_ID,
CASE
when LEN(LTRIM(RTRIM(CHARGE_CATEG)))> 5 then LTRIM(RTRIM(CHARGE_CATEG))
else upper(ltrim(rtrim(charge_categ))) END AS CHARGE_CATEG,
 ltrim(rtrim(charge_categ_desc))  AS CHARGE_CATEG_DESC,
ISNULL(CONVERT(INT,TAX_IND),101) ,1 AS VERSION
FROM [BCMPWMT].[CHARGE_CATEG_LKP]


SELECT * FROM DIM_CHARGE_CATEG_SQL_IN1542

--TESTING----
----row count-------------
select count(*) from DIM_CHARGE_CATEG_SQL_IN1542 ---42

select 
	count(*)
from 
	(SELECT 
		ISNULL(CONVERT(INT,LTRIM(RTRIM(CHARGE_CATEG_ID))),101) AS CHARGE_CATEG_ID ,
		ISNULL(CONVERT(INT,LTRIM(RTRIM(TENANT_ORG_ID))),101) AS TENANT_ORG_ID,
		CASE
			when LEN(LTRIM(RTRIM(CHARGE_CATEG)))> 5 then LTRIM(RTRIM(CHARGE_CATEG))
			else upper(ltrim(rtrim(charge_categ)))
			END AS CHARGE_CATEG,
		ltrim(rtrim(charge_categ_desc))  AS CHARGE_CATEG_DESC,
		ISNULL(CONVERT(INT,TAX_IND),101) as tax,
		1 AS VERSION
	FROM [BCMPWMT].[CHARGE_CATEG_LKP])kkk


	----------------------------------------------











--42
---row count group by ---------------------42
SELECT * FROM 
[BCMPWMT].[CHARGE_CATEG_LKP]
select TENANT_ORG_ID	,count(*) from DIM_CHARGE_CATEG_SQL_in1542 group by TENANT_ORG_ID
select TENANT_ORG_ID	,count(*) 
from 
	(SELECT 
		ISNULL(CONVERT(INT,LTRIM(RTRIM(CHARGE_CATEG_ID))),101) AS CHARGE_CATEG_ID ,
		ISNULL(CONVERT(INT,LTRIM(RTRIM(TENANT_ORG_ID))),101) AS TENANT_ORG_ID,
		CASE
			when LEN(LTRIM(RTRIM(CHARGE_CATEG)))> 5 then LTRIM(RTRIM(CHARGE_CATEG))
			else upper(ltrim(rtrim(charge_categ)))
			END AS CHARGE_CATEG,
		ltrim(rtrim(charge_categ_desc))  AS CHARGE_CATEG_DESC,
		ISNULL(CONVERT(INT,TAX_IND),101) as tax,
		1 AS VERSION
	FROM [BCMPWMT].[CHARGE_CATEG_LKP])kkk




group by TENANT_ORG_ID


---------------------------------------

---------------random record check---- 
select CHARGE_CATEG_ID,charge_categ_desc from DIM_CHARGE_CATEG_SQL_in1542 where charge_categ='vas'
select CHARGE_CATEG_ID,charge_categ_desc   from 

	(SELECT 
		ISNULL(CONVERT(INT,LTRIM(RTRIM(CHARGE_CATEG_ID))),101) AS CHARGE_CATEG_ID ,
		ISNULL(CONVERT(INT,LTRIM(RTRIM(TENANT_ORG_ID))),101) AS TENANT_ORG_ID,
		CASE
			when LEN(LTRIM(RTRIM(CHARGE_CATEG)))> 5 then LTRIM(RTRIM(CHARGE_CATEG))
			else upper(ltrim(rtrim(charge_categ)))
			END AS CHARGE_CATEG,
		ltrim(rtrim(charge_categ_desc))  AS CHARGE_CATEG_DESC,
		ISNULL(CONVERT(INT,TAX_IND),101) as tax,
		1 AS VERSION
	FROM [BCMPWMT].[CHARGE_CATEG_LKP])kkk

	select * from [BCMPWMT].[CHARGE_CATEG_LKP]



where charge_categ='vas'

-------------duplicate check---------------- 0
select CHARGE_CATEG_ID,count(*) 

from DIM_CHARGE_CATEG_SQL_in1542
group by CHARGE_CATEG_ID
having count(*)>1
-------------column level check----------------
select count(*)
from
DIM_CHARGE_CATEG_SQL_IN1542  t left join 

	(SELECT 
		ISNULL(CONVERT(INT,LTRIM(RTRIM(CHARGE_CATEG_ID))),101) AS CHARGE_CATEG_ID ,
		ISNULL(CONVERT(INT,LTRIM(RTRIM(TENANT_ORG_ID))),101) AS TENANT_ORG_ID,
		CASE
			when LEN(LTRIM(RTRIM(CHARGE_CATEG)))> 5 then LTRIM(RTRIM(CHARGE_CATEG))
			else upper(ltrim(rtrim(charge_categ)))
			END AS CHARGE_CATEG,
		ltrim(rtrim(charge_categ_desc))  AS CHARGE_CATEG_DESC,
		ISNULL(CONVERT(INT,TAX_IND),101) as tax,
		1 AS VERSION
	FROM [BCMPWMT].[CHARGE_CATEG_LKP])s
on t.CHARGE_CATEG_ID=s.charge_categ_id
where s.charge_categ_id is null and(

s.CHARGE_CATEG_ID  <>  t.CHARGE_CATEG_ID or
s.TENANT_ORG_ID  <>  t.TENANT_ORG_ID or
s.CHARGE_CATEG  <>  t.CHARGE_CATEG or
s.CHARGE_CATEG_DESC  <>  t.CHARGE_CATEG_DESC 
)
