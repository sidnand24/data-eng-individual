from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt


sc = SparkSession.builder.appName('nba-pred').getOrCreate()

st.title('''NBA salary prediction''')
st.subheader('Machine Learning model comparison with Apache Spark and MLlib')

# Read in data and split
df = sc.read.load("processed_data.parquet")
splits = df.randomSplit([0.7, 0.3], seed=124)
train_df = splits[0]
test_df = splits[1]

# Sidebar to choose model to run
st.sidebar.title('Models')
st.sidebar.subheader('Select your model')
mllib_model = st.sidebar.selectbox("   MLlib Models", \
        ('Linear Regression', 'Gradient-Boosted Tree', 'Decision Tree Regressor', \
            'Random Forest Regressor'))
st.sidebar.text('70 - 30 split')

def regression_model(mllib_model, train_df, test_df):
    if mllib_model == 'Linear Regression':
        lr = LinearRegression(featuresCol = 'features', labelCol='label')   
        lr_model = lr.fit(train_df)
        fullPredictions = lr_model.transform(test_df).cache()
        lr_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label",metricName="r2")
        r2 = lr_evaluator.evaluate(fullPredictions)
        lr_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label",metricName="rmse")
        rmse = lr_evaluator.evaluate(fullPredictions)
        pred = [int(row['prediction']) for row in fullPredictions.select('prediction').collect()]
        actual = [int(row['label']) for row in fullPredictions.select('label').collect()]
        return r2,rmse,pred,actual

    elif mllib_model == 'Decision Tree Regressor':
        dt = DecisionTreeRegressor(featuresCol = 'features', labelCol='label')
        dt_model = dt.fit(train_df)
        dtPrediction = dt_model.transform(test_df).cache()
        dt_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label",metricName="r2")
        r2 = dt_evaluator.evaluate(dtPrediction)
        dt_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label",metricName="rmse")
        rmse = dt_evaluator.evaluate(dtPrediction)
        pred = [int(row['prediction']) for row in dtPrediction.select('prediction').collect()]
        actual = [int(row['label']) for row in dtPrediction.select('label').collect()]
        return r2,rmse,pred,actual

    elif mllib_model == 'Gradient-Boosted Tree':
        gb = GBTRegressor(featuresCol = 'features', labelCol='label')
        gb_model = gb.fit(train_df)
        gbPredictions = gb_model.transform(test_df).cache()
        gb_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label",metricName="r2")
        r2 = gb_evaluator.evaluate(gbPredictions)
        gb_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label",metricName="rmse")
        rmse = gb_evaluator.evaluate(gbPredictions)
        pred = [int(row['prediction']) for row in gbPredictions.select('prediction').collect()]
        actual = [int(row['label']) for row in gbPredictions.select('label').collect()]
        return r2,rmse,pred,actual   

    else:
        rf = RandomForestRegressor(featuresCol = 'features', labelCol='label')
        rf_model = rf.fit(train_df)
        rfPredictions = rf_model.transform(test_df).cache()
        rf_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label",metricName="r2")
        r2 = rf_evaluator.evaluate(rfPredictions)
        rf_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label",metricName="rmse")
        rmse = rf_evaluator.evaluate(rfPredictions)
        pred = [int(row['prediction']) for row in rfPredictions.select('prediction').collect()]
        actual = [int(row['label']) for row in rfPredictions.select('label').collect()]
        return r2,rmse,pred,actual


st.markdown('The target variable of focus is player salary and vectorized data is given as input to the model.')
st.write("#")

st.markdown("""The input variables selected were a mixture of general player information (e.g. age and height), a proxy measure of popularity (total tweet
count over the previous week), and performance statistics of the season thus far. As numerous factors influence the salary offered to players by their 
respective clubs, the model is unlikely to truly accurately determine the correct value. Nevertheless the best performing model can offer insight into potential earning
and a rough estimate to be utilised by the board of the team and players themselves.""")

st.write("#")
st.text('The results and scores of the model on the testing data are displayed below.')
r2,rmse,actual,pred = regression_model(mllib_model, train_df, test_df)

st.write(mllib_model," model")

st.write(r2," R2 score")
st.write(rmse," RMSE of the model")

st.markdown('From the results of this project the random forest regressor is the best performing model.')