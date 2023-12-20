# Databricks notebook source
#importing the dataset:
data = spark.read.csv('/FileStore/tables/Hotel_Booking.csv',
                        header=True,
                        inferSchema=True)
display(data)

# COMMAND ----------

#checking the schema:
data.printSchema()

# COMMAND ----------

#reanaming the colums:
col_renamed = {i:i.replace(' ','_') for i in data.columns}
data = data.withColumnsRenamed(col_renamed)
display(data)

# COMMAND ----------

#analyzing the data:
dbutils.data.summarize(data)

# COMMAND ----------

#dropping the columns that contains more than 90% values as 0 --> (repeated,P-C, P-not-C, car_parking_space):
data = data.drop('repeated','P-C', 'P-not-C', 'car_parking_space')
display(data)

# COMMAND ----------

#droping the row where number of children is 10:
data = data.filter('number_of_children != 10')
display(data)

# COMMAND ----------

#splitting the date,month and year of reservation:
from pyspark.sql.functions import col,split,translate
df = data.withColumn('date_of_reservation',translate('date_of_reservation','-','/'))
df = df.withColumn('Month_of_reservation',split(col('date_of_reservation'),'/')[0])
df = df.withColumn('Day_of_reservation',split(col('date_of_reservation'),'/')[1])
df = df.withColumn('Year_of_reservation',split(col('date_of_reservation'),'/')[2])
df = df.drop('date_of_reservation')
display(df)

# COMMAND ----------

#changing the datatype:
df = df.withColumn('Month_of_reservation',col('Month_of_reservation').cast('int'))
df = df.withColumn('Day_of_reservation',col('Day_of_reservation').cast('int'))
df = df.withColumn('Year_of_reservation',col('Year_of_reservation').cast('int'))
display(df)

# COMMAND ----------

#saving the cleaned dataset: 
df.write.mode('overwrite').csv('/FileStore/tables/Hotel_Booking_cleaned.csv')

# COMMAND ----------


