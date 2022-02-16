import findspark
from pyspark.sql import SparkSession
from  create_dataset import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from sklearn.metrics import davies_bouldin_score

from pyspark.ml.clustering import KMeans
import numpy as np 
from datetime import datetime
import pandas as pd

import itertools

import os
from tkinter import filedialog as fd
    
###################
def k_mans_on_k(data, initMode='k-means||', k=2, maxIter=5000, tol = 0.001, initSteps=150):

    evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized', \
                                    metricName='silhouette', distanceMeasure='squaredEuclidean')
    
    KMeans_algo=KMeans(initMode=initMode, maxIter=maxIter, tol = tol, featuresCol='standardized', k=k, initSteps=initSteps)

    KMeans_fit=KMeans_algo.fit(data)
    
    output=KMeans_fit.transform(data)  

    #costs
    trainingCost = KMeans_fit.summary.trainingCost
    

    #silhouette
    score=evaluator.evaluate(output)   

    #davies_bouldin_score
    X = output.select('standardized').toPandas()['standardized'].\
                        apply(lambda x : np.array(x.toArray())).to_numpy()
    labels = output.select('prediction').toPandas()['prediction'].to_numpy()

    db_score = davies_bouldin_score(list(X), labels)

    print(f"""when k is {k} Silhouette Score is {score} davies_bouldins is {db_score} 
                    \n {'-'*50} """)
    
    centers = KMeans_fit.clusterCenters()
    
    return output, trainingCost, score, db_score, centers

#######
def read_dataset(spark, file):
    print('\n', 'Read Data', '\n') 
    
    df = spark.read.option("dateFormat", "yyyy-MM-dd HH:mm:ss").csv(file, header=True, inferSchema=True, sep=';')
    
    new_column_name_list= list(map(lambda x: x.replace(" ", "_"), df.columns))
    df = df.toDF(*new_column_name_list)
    
    #
    print('\n', 'Craete Data Set', '\n')
    spark_dff = craete_dataset(df)
        
    spark_dff = spark_dff.filter(spark_dff.OP_ANI>0).drop('_c0', 'OP_Name')    
    
    #VectorAssembler
    spark_dff = spark_dff.fillna(0)
    assemble = VectorAssembler(inputCols=spark_dff.columns[1:], outputCol='features')
    
    assembled_data=assemble.transform(spark_dff)
    assembled_data = assembled_data.fillna(0)
    
    #StandardScaler
    scale=StandardScaler(inputCol='features',outputCol='standardized')
    data_scale=scale.fit(assembled_data)
    data_scale_output = data_scale.transform(assembled_data)
    
    return df, spark_dff, data_scale_output

#########
def clustering(spark, file, df, data, k, no_phone, iteration):
    initMode='k-means||'
    maxIter=5000
    tol = 0.001
    initSteps=150 
    
    if no_phone:
        data = data.filter(~data.OP_ANI.isin(no_phone))
    
    output, trainingCost, score, db_score, centers =\
                k_mans_on_k(data, initMode=initMode, k=k, maxIter=maxIter, tol = tol, initSteps=initSteps)
    
    # 
    print('\n', 'counting cluster labels', '\n')
    cluster_labels_counts = output.groupBy(f'prediction').count().orderBy('count').toPandas()
    print('\n', cluster_labels_counts, '\n')
    
    save_csv(spark, file, k, df, output, cluster_labels_counts, iteration='All')
    
    return output, cluster_labels_counts
    
###########
def save_csv(spark, file, k, df, output, cluster_labels_counts, iteration, isDelete=False):    
    if not isDelete:
        print('\n', 'Query Result', '\n')
        prediction_list = input('Is use lable with 1,000 count? (Enter for YES or n for NO)')
        
        if prediction_list != 'n':
            prediction_list = cluster_labels_counts[cluster_labels_counts['count']<1000].prediction.tolist()
            print('Selected Label: ', prediction_list)
            
            output = output.filter(output['prediction'].isin(prediction_list))
    else:
        print('\n', 'Delete Phone Result', '\n')
        
    df.createOrReplaceTempView("cdr")
    output.createOrReplaceTempView("output")
    
    query = spark.sql("""
                    SELECT OP_Name, TP_Name, Setup_Time, cdr.OP_ANI,  Called_Number_IN, Duration, prediction  FROM cdr
                    INNER JOIN output on (cdr.OP_ANI = output.OP_ANI)
                    """)
                    
    print('\n', 'Save Result', '\n')  
    date = datetime.now().strftime("%Y_%m_%d_%I_%M_%S_%p")
    name = file.split('/')[-1].split('.')[0]
    query.toPandas().to_excel(f"_result/{name}_{k}_{iteration}_partitaion_{date}.xlsx", index=False)
    #query.toPandas().to_csv(f"_result/{name}_{k}_{iteration}_partitaion_{date}.csv", index=False)

#######    
def create_cluster(file, k):

    spark = SparkSession.builder.appName('cdr Analyses').getOrCreate()
    
    #read      
    df, spark_dff, data_scale_output = read_dataset(spark, file)
    
    #
    print('\n', 'Cluster 1', '\n') 
    no_phone = [0]
    output, cluster_labels_counts = clustering(spark, file, df, data_scale_output, k, no_phone, 'ALL')
    
    
    for i in itertools.count():
        print('\n', f'Cluster {i+2}', '\n')   
        
        prediction_list = cluster_labels_counts[cluster_labels_counts['count']<=10].prediction.tolist()
        
        if len(prediction_list)>0:
            no_phone_df = output.filter(output['prediction'].isin(prediction_list))
            no_phone = no_phone_df.toPandas()['OP_ANI'].tolist()
            print('deleted phone in 2 iteration : ', no_phone)
            
            save_csv(spark, file, k, df, no_phone_df, cluster_labels_counts, f'deleted_phone_iter_{i+2}', True)            
            
            output = output.drop('prediction')
            output, cluster_labels_counts = clustering(spark, file, df, output, k, no_phone, f'with_Deleted_phone_{i+2}')
        else:
            break
    print('*'*50)
    a = input('END') 
    
    return 
    
##########    
if __name__ == "__main__":
    os.makedirs('_result', exist_ok=True)
    initialdir =  os.getcwd()  
    
    filetypes = (
        ('csv files', '*.csv'),
    )

    file = fd.askopenfilename(
        title='Open a file',
        initialdir=initialdir,
        filetypes=filetypes)
    #file = 'cdr_all_ops.csv'
    print(file)
    if file=='':
        print('Pleas sealect a file')
    else:
        isint = False
        while not isint:        
            try:
                k = int(input('inter cluster count: '))
                isint = True
            except :
                isint = False
                
        create_cluster(file, k)