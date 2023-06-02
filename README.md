# **Big Data**
## Home Sales Analysis with Spark

## Intro to Big Data


Big data refers to a large amount
of structured, unstructured, or
semistructured data that is received at a
fast rate, that is received with the utmost
accuracy, and that needs to be
processed at a high capacity. Big data is a collection of data that is so large in size and complexity that none of traditional data management tools can store or process it efficiently.

## Intro to Apache Spark


Apache Spark is a data-processing engine for large data sets. It is designed to deliver the speed, scalability, and programmability required for Big Data, machine learning, and artificial intelligence applications.
Earlier tools for handling massive amounts of data were slow. The AMPLab at UC Berkeley came up with the idea of storing data in Resilien Distributed Datasets(RDDs) and using memory instead of disk space. That improved the speed by 100 times. AMPLab  later donated Spark to the Apache Software Foundation - which now maintains it. That is why it is now called Apache Spark.

## Report

### Procedure
Using SparkSQL, we read the home_sales_revised.csv data in the starter code into a Spark DataFrame.

  * Created a temporary table called `home_sales`.
  
Answer the following questions using SparkSQL:

  * What is the average price for a four-bedroom house sold for each year? Round off your answer to two decimal places.

```SQL
spark.sql("""
          SELECT date_built, ROUND(AVG(price), 2) as avg_price
          FROM houseSale
          WHERE bedrooms = 4
          GROUP BY date_built
          ORDER BY date_built DESC
           """).show()

+----------+---------+
|date_built|avg_price|
+----------+---------+
|      2017|296576.69|
|      2016|296050.24|
|      2015|307908.86|
|      2014|299073.89|
|      2013|299999.39|
|      2012|298233.42|
|      2011| 302141.9|
|      2010|296800.75|
+----------+---------+
```

   * What is the average price of a home for each year it was built that has three bedrooms and three bathrooms? Round off your answer to two decimal places.

  ```SQL
  spark.sql("""
           SELECT date_built, ROUND(AVG(price), 2) as avg_price
           FROM houseSale
           WHERE bedrooms = 3 AND bathrooms = 3
           GROUP BY date_built
           ORDER BY 1 DESC
           """).show()

  +----------+---------+
|date_built|avg_price|
+----------+---------+
|      2017|292676.79|
|      2016|290555.07|
|      2015| 288770.3|
|      2014|290852.27|
|      2013|295962.27|
|      2012|293683.19|
|      2011|291117.47|
|      2010|292859.62|
+----------+---------+ 
```

* What is the average price of a home for each year that has three bedrooms, three bathrooms, two floors, and is greater than or equal to 2,000 square feet? Round off your answer to two decimal places.  
  
```SQL
  spark.sql("""
           SELECT date_built, ROUND(AVG(price), 2) as avg_price
           FROM houseSale
           WHERE bedrooms = 3 AND bathrooms = 3 AND floors = 2 AND sqft_living >= 2000
           GROUP BY date_built
           ORDER BY 1 DESC
           """).show()

+----------+---------+
|date_built|avg_price|
+----------+---------+
|      2017|280317.58|
|      2016| 293965.1|
|      2015|297609.97|
|      2014|298264.72|
|      2013|303676.79|
|      2012|307539.97|
|      2011|276553.81|
|      2010|285010.22|
+----------+---------+
```
  * What is the "view" rating for homes costing more than or equal to $350,000? Determine the run time for this query, and round off your answer to two decimal places.

 ```SQL
 start_time = time.time()

spark.sql("SELECT view, avg(price) FROM houseSale where price > 350000 group by view").show()

print("--- %s seconds ---" % (time.time() - start_time))

+----+------------------+
|view|        avg(price)|
+----+------------------+
|  31|399856.95135135134|
|  85|1056336.7435897435|
|  65| 736679.9333333333|
|  53|          755214.8|
|  78|1080649.3666666667|
|  34| 401419.7451923077|
|  81|1053472.7878787878|
|  28|402124.62176165805|
|  76|1058802.7777777778|
|  26|401506.96774193546|
|  27| 399537.6586826347|
|  44|400598.04761904763|
|  12| 401501.3243243243|
|  91| 1137372.731707317|
|  22| 402022.6847826087|
|  93|1026006.0606060605|
|  47| 398447.4976744186|
|   1| 401044.2513368984|
|  52| 733780.2608695652|
|  13|         398917.98|
+----+------------------+
only showing top 20 rows

--- 0.7600610256195068 seconds ---
```
