import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, datediff, lag, coalesce, lit, isnull, when, to_utc_timestamp,substring
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, LongType


def craete_dataset(df, issave=False):
    findspark.init()
    
    spark = SparkSession.builder.appName('cdr datast craete').getOrCreate()
    
    
  
    
    
    ####
    df = df.select('Call_ID', 'OP_ANI','Called_Number', 'Start_Time', 'End_Time', 
                   'Duration', 'OP_Price', 'TP_Price', 'Cost_IN', 'Cost_OUT', 'OP_Name')

    df = df.withColumn("Start_Time_utc", to_timestamp('Start_Time')).\
                    withColumn("Start_Time",to_utc_timestamp("Start_Time_utc", 'Asia/Tehran'))

    df = df.withColumn("End_Time_utc", to_timestamp('End_Time')).\
             withColumn("End_Time",to_utc_timestamp("End_Time_utc", 'Asia/Tehran'))
        
    df = df.withColumn('Duration', df["Duration"].cast(DoubleType()))

    df = df.withColumn('OP_ANI', df['OP_ANI'].cast(LongType()))

    df = df.withColumn('Called_Number', df["Called_Number"].cast(LongType()))

    df = df.withColumn('Start_date', to_date('Start_Time'))

    df = df.withColumn('End_date', to_date('End_Time'))

    df = df.withColumn('B_Continent', substring('Called_Number', 0,1))

    df = df.withColumn('A_Continent', substring('OP_ANI', 0,1))

    df = df.fillna(0)
    
    #####
    df.createOrReplaceTempView("cdr")

    df_BNUMS_per_ANUM_zero = spark.sql("""SELECT   distinct Called_Number,      
              DENSE_RANK() OVER (PARTITION BY   Called_Number order by OP_ANI) +
              DENSE_RANK() OVER (PARTITION BY  Called_Number order by OP_ANI  DESC) - 1 AS BNUMS_per_ANUM_zero,
              
              COUNT(Called_Number) OVER (PARTITION BY   Called_Number order by Called_Number) AS 
              BNUMS_per_ANUM_All_count_zero
              
              FROM cdr
              where Duration = 0 """).na.fill(0)


    df_ANUMS_per_BNUM_zero = spark.sql("""SELECT   distinct OP_ANI,      
              DENSE_RANK() OVER (PARTITION BY   OP_ANI order by Called_Number) +
              DENSE_RANK() OVER (PARTITION BY  OP_ANI order by Called_Number  DESC) - 1 AS ANUMS_per_BNUM_zero,
              
              COUNT(OP_ANI) OVER (PARTITION BY   OP_ANI order by OP_ANI) AS 
              ANUMS_per_BNUM_All_count_zero              
              FROM cdr
              where Duration = 0 """).na.fill(0)  
              
    
    df_BNUMS_per_ANUM = spark.sql("""SELECT   distinct Called_Number,
              DENSE_RANK() OVER (PARTITION BY   Called_Number order by OP_ANI) +
              DENSE_RANK() OVER (PARTITION BY  Called_Number order by OP_ANI  DESC) - 1 AS BNUMS_per_ANUM,
              COUNT(Called_Number) OVER (PARTITION BY   Called_Number order by Called_Number) BNUMS_per_ANUM_All_count,
              SUM(Duration) OVER (PARTITION BY   Called_Number order by Called_Number) Duration_B_A_SUM,
              AVG(Duration) OVER (PARTITION BY   Called_Number order by Called_Number) Duration_B_A_AVG,
              STD(Duration) OVER (PARTITION BY   Called_Number order by Called_Number) Duration_B_A_STD  
              FROM cdr
              where Duration > 0""").na.fill(0)
             
    
    df_BNUMS_per_ANUM.createOrReplaceTempView("df_BNUMS_per_ANUM")
    df_ANUMS_per_BNUM_zero.createOrReplaceTempView("df_ANUMS_per_BNUM_zero")
    df_BNUMS_per_ANUM_zero.createOrReplaceTempView("df_BNUMS_per_ANUM_zero")

    df_diff = spark.sql("""SELECT cdr.*,      
              CAST(cdr.Start_Time AS bigint) - CAST(lag(cdr.Start_Time, 1) OVER (
              PARTITION BY cdr.OP_ANI ORDER BY cdr.OP_ANI, cdr.Start_Time) AS bigint) as Start_Time_diff,
              
              CAST(cdr.Start_Time AS bigint) - CAST(lag(cdr.Start_Time, 1) OVER (
              PARTITION BY cdr.OP_ANI, cdr.Start_date ORDER BY cdr.OP_ANI, cdr.Start_Time) AS bigint) as Start_Time_diff_day,
              
              SUM(cdr.Duration) OVER (PARTITION BY cdr.OP_ANI )  as Duration_sum,
              
              SUM(cdr.Duration) OVER (PARTITION BY cdr.OP_ANI, cdr.Start_date )  as Duration_day_sum,
           
              count(cdr.OP_ANI) OVER (PARTITION BY cdr.OP_ANI, cdr.Start_date )  as Calls_per_day_count, 
           
              DENSE_RANK() OVER (PARTITION BY cdr.OP_ANI order by cdr.Called_Number) +
              DENSE_RANK() OVER (PARTITION BY cdr.OP_ANI order by cdr.Called_Number DESC) -
              1 as ANUMS_per_BNUM,
              
              bpa.BNUMS_per_ANUM,
              bpa.BNUMS_per_ANUM_All_count,
              bpa.Duration_B_A_SUM,
              bpa.Duration_B_A_AVG,
              bpa.Duration_B_A_STD,
              
              apbz.ANUMS_per_BNUM_zero,
              apbz.ANUMS_per_BNUM_All_count_zero,
              
              bpaz.BNUMS_per_ANUM_zero,
              bpaz.BNUMS_per_ANUM_All_count_zero
              
              /*,
              
              CASE 
                  WHEN hour(cdr.Start_Time) between 1 and 7 THEN '1-7'
                  WHEN hour(cdr.Start_Time) between 8 and 9 THEN '8-9'
                  WHEN hour(cdr.Start_Time) between 10 and 13 THEN '10-13'
                  WHEN hour(cdr.Start_Time) between 14 and 19 THEN '14-19'
                  WHEN hour(cdr.Start_Time) between 20 and 21 THEN '20-21'
                  WHEN hour(cdr.Start_Time) IN (22, 23, 0) THEN '22-0'
              END AS per_hour
                */
                
     FROM cdr
         LEFT JOIN df_BNUMS_per_ANUM bpa on cdr.OP_ANI = bpa.Called_Number 
         LEFT JOIN df_ANUMS_per_BNUM_zero apbz on cdr.OP_ANI = apbz.OP_ANI          
         LEFT JOIN df_BNUMS_per_ANUM_zero bpaz on cdr.OP_ANI = bpaz.Called_Number
     WHERE cdr.Duration > 0""").na.fill(0)    
        
    
    df_diff.createOrReplaceTempView('df_diff')

    spark_dff = spark.sql("""
                SELECT OP_ANI,
                
                COUNT(DISTINCT Start_date) as Days_Spoken_Count,
                
                count(OP_ANI) AS call_count,  
                
                max(ANUMS_per_BNUM) AS ANUMS_per_BNUM_count ,
               
               
                max(BNUMS_per_ANUM) AS BNUMS_per_ANUM_count ,
                abs(max(ANUMS_per_BNUM) - max(BNUMS_per_ANUM)) as Nrotations,
                max(BNUMS_per_ANUM_All_count) AS  BNUMS_per_ANUM_All_count,
               
                
                
                sum(Duration) AS Duration_sum, 
                avg(Duration) AS Duration_avg,
                std(Duration) AS Duration_std,
                
                max(Duration)  AS Duration_max,
                min(Duration)  AS Duration_min,
                
                avg(Duration_day_sum)  AS Duration_day_sum_avg,
                std(Duration_day_sum)  AS Duration_day_sum_std,
                
                max(Duration_day_sum)  AS Duration_day_sum_max,
                min(Duration_day_sum)  AS Duration_day_sum_min,
                
                max(Duration_B_A_SUM) AS Duration_B_A_SUM,
                max(Duration_B_A_AVG) AS Duration_B_A_AVG,
                max(Duration_B_A_STD) AS Duration_B_A_STD,
              
                sum(Start_Time_diff) AS Start_Time_diff_sum, 
                avg(Start_Time_diff) AS Start_Time_diff_avg,
                std(Start_Time_diff) AS Start_Time_diff_std,
                
                sum(Start_Time_diff_day) AS Start_Time_diff_day_sum, 
                avg(Start_Time_diff_day) AS Start_Time_diff_day_avg,
                std(Start_Time_diff_day) AS Start_Time_diff_day_std,
                
                sum(OP_Price) as OP_Price_sum, 
                avg(OP_Price) as OP_Price_avg,
                std(OP_Price) as OP_Price_std,
                
                sum(TP_Price) as TP_Price_sum, 
                avg(TP_Price) as TP_Price_avg,
                std(TP_Price) as TP_Price_std,
                
                sum(Cost_IN) as Cost_IN_sum, 
                avg(Cost_IN) as Cost_IN_avg,
                std(Cost_IN) as Cost_IN_std,
                
                sum(Cost_OUT) as Cost_OUT_sum, 
                avg(Cost_OUT) as Cost_OUT_avg,
                std(Cost_OUT) as Cost_OUT_std,
                
                avg(Calls_per_day_count) as Calls_per_day_count_avg,
          
                AVG((bigint(to_timestamp(Start_Time))) - (bigint(to_timestamp(Start_date))))  as start_hour_avg,
                STD((bigint(to_timestamp(Start_Time))) - (bigint(to_timestamp(Start_date))))  as start_hour_std,
                
                AVG((bigint(to_timestamp(End_Time))) - (bigint(to_timestamp(End_date))))  as end_hour_avg,
                STD((bigint(to_timestamp(End_Time))) - (bigint(to_timestamp(End_date))))  as end_hour_std,
                      
                
                avg(CASE 
                  WHEN hour(Start_Time) between 1 and 7 THEN 1
                  ELSE 0
                END)  AS Occupation_rate_1_7_sum,
                
                avg(CASE 
                  WHEN hour(Start_Time) between 1 and 7 THEN Duration
                  ELSE 0
                END)  AS Calls_per_hour_1_7_sum,
                
                
                avg(CASE 
                  WHEN hour(Start_Time) between 8 and 9 THEN 1
                  ELSE 0
                END)  AS Occupation_rate_8_9_sum,
                
                avg(CASE 
                  WHEN hour(Start_Time) between 8 and 9 THEN Duration
                  ELSE 0
                END)  AS Calls_per_hour_8_9_sum,
                
                
                avg(CASE 
                  WHEN hour(Start_Time) between 10 and 13 THEN 1
                  ELSE 0
                END)  AS Occupation_rate_10_13_sum,
                
                avg(CASE 
                  WHEN hour(Start_Time) between 10 and 13 THEN Duration
                  ELSE 0
                END)  AS Calls_per_hour_10_13_sum,
                
                
                
                avg(CASE 
                  WHEN hour(Start_Time) between 14 and 19 THEN 1
                  ELSE 0
                END)  AS Occupation_rate_14_19_sum,
                
                avg(CASE 
                  WHEN hour(Start_Time) between 14 and 19 THEN Duration
                  ELSE 0
                END)  AS Calls_per_hour_14_19_sum,
                
                
               avg(CASE 
                  WHEN hour(Start_Time) between 20 and 21 THEN 1
                  ELSE 0
                END)  AS Occupation_rate_20_21_sum,
                
                avg(CASE 
                  WHEN hour(Start_Time) between 20 and 21 THEN Duration
                  ELSE 0
                END)  AS Calls_per_hour_20_21_sum,
                
                
                avg(CASE 
                  WHEN hour(Start_Time)IN (22, 23, 0) THEN 1
                  ELSE 0
                END)  AS Occupation_rate_22_0_sum,
                
                avg(CASE 
                  WHEN hour(Start_Time) IN (22, 23, 0) THEN Duration
                  ELSE 0
                END)  AS Calls_per_hour_22_0_sum,
                
                max(ANUMS_per_BNUM_zero) AS ANUMS_per_BNUM_zero_,
                max(ANUMS_per_BNUM_All_count_zero) AS ANUMS_per_BNUM_All_count_zero_,

                max(BNUMS_per_ANUM_zero) AS BNUMS_per_ANUM_zero_,
                max(BNUMS_per_ANUM_All_count_zero) AS BNUMS_per_ANUM_All_count_zero_,
              
                SUM(IF(B_Continent = 9, 1, 0)) AS B_Continent_9_count,
                SUM(IF(B_Continent = 8, 1, 0)) AS B_Continent_8_count,
                SUM(IF(B_Continent = 7, 1, 0)) AS B_Continent_7_count,
                SUM(IF(B_Continent = 6, 1, 0)) AS B_Continent_6_count,
                SUM(IF(B_Continent = 5, 1, 0)) AS B_Continent_5_count,
                SUM(IF(B_Continent = 4, 1, 0)) AS B_Continent_4_count,
                SUM(IF(B_Continent = 3, 1, 0)) AS B_Continent_3_count,
                SUM(IF(B_Continent = 2, 1, 0)) AS B_Continent_2_count,
                SUM(IF(B_Continent = 1, 1, 0)) AS B_Continent_1_count,
                SUM(IF(
                    (B_Continent <> 1) AND
                    (B_Continent <> 2) AND
                    (B_Continent <> 3) AND
                    (B_Continent <> 4) AND
                    (B_Continent <> 5) AND
                    (B_Continent <> 6) AND
                    (B_Continent <> 7) AND
                    (B_Continent <> 8) AND
                    (B_Continent <> 9), 1, 0)) AS B_Continent_None_count
                
                FRom df_diff
                WHERE Duration > 0 AND OP_Name = 'TIC'
                
                group by OP_ANI""")
                
    if issave:
        spark_dff.toPandas().to_csv(f'DS_{file}')
    
    return spark_dff