# Spark Java implementation
The purpose of this repository is to develop a model example with Java and Spark API. 
The case example uses a kaggle repository https://www.kaggle.com/c/GiveMeSomeCredit. All data for the
models are stored on this site (change variable names without "-" e.g.
"NumberOfTime60-89DaysPastDueNotWorse"-> "NumberOfTime6089DaysPastDueNotWorse") --> also stored in 01_DATA.
Credit worthiness is predicted. This can be used as a template to setup spark and see how the API can be used with Java. 
The case sample has been developed in a way that most users of Java can access this example relatively easy and use 
it as a reference. The case of using RDD vs DataSet is also investigated, given different versions of the Spark 
implementation. Although RDD is likely to be phased out, the usecase is also worthwhile.  

Jacoco is used for presentation tool of junit tests - in /target/site/jacoco-ut.

Dependencies: 
Spark 2.1.0 http://spark.apache.org/downloads.html
Maven https://maven.apache.org/download.cgi
jar dependencies : 
see pom.xml
