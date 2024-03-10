// Databricks notebook source
// dbfs:/FileStore/world_happiness_report_2021.csv
// dbfs:/FileStore/world_happiness_report.csv

// COMMAND ----------

val df1 = spark.read.option("delimiter", ",").option("header", true).option("inferSchema", true).csv("dbfs:/FileStore/world_happiness_report_2021.csv") 
display(df1)

// COMMAND ----------

val df2 = spark.read.option("delimiter", ",").option("header", true).option("inferSchema", true).csv("dbfs:/FileStore/world_happiness_report.csv")
display(df2)

// COMMAND ----------

//Pregunta 1
import org.apache.spark.sql.functions.{col, desc, min, max}

df1.select(
  col("Country name").as("World Hapiest Country name"),
  col("Ladder score").as("Max Ladder score")
).orderBy(desc("Ladder score")).limit(1).show()

// COMMAND ----------

//Pregunta 2
df1.createOrReplaceTempView("df1_temp")

// COMMAND ----------

// MAGIC %sql
// MAGIC select rn.`Regional indicator`, rn.`Country name`, rn.`Ladder score` 
// MAGIC from ( select `Regional indicator`, `Country name`, `Ladder score`, ROW_NUMBER() OVER(PARTITION BY `Regional indicator` ORDER BY `Ladder score` desc) as rank
// MAGIC             from df1_temp) as rn
// MAGIC where rn.rank = 1
// MAGIC

// COMMAND ----------

//Pregunta 2 con scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
 
display(df1.select("Regional indicator", "Country name", "Ladder score").withColumn("rank", row_number().over(Window.partitionBy($"Regional indicator").orderBy($"Ladder score".desc))).filter(col("rank") === "1"))

// COMMAND ----------

//Pregunta 3
df2.createOrReplaceTempView("df2_temp")

// COMMAND ----------

// MAGIC %sql
// MAGIC select rn.`Country name`, count(rn.`Country name`) as `Num of Top Position`
// MAGIC from ( select year, `Country name`, `Life Ladder`, ROW_NUMBER() OVER(PARTITION BY year ORDER BY `Life Ladder` desc) as rank
// MAGIC             from df2_temp
// MAGIC               union all
// MAGIC             select 2021 as year, `Country name`, `Ladder score` as `Life Ladder`, ROW_NUMBER() OVER(ORDER BY `Ladder score` desc) AS rank
// MAGIC             from df1_temp
// MAGIC             ) as rn
// MAGIC where rn.rank = 1
// MAGIC group by rn.`Country name`

// COMMAND ----------

//Pregunta 3 con scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import org.apache.spark.sql.functions.typedLit
 
display(df2.select("year", "Country name", "Life Ladder").union(df1.withColumn("year", typedLit(2021)).withColumnRenamed("Ladder score","Life Ladder").select("year", "Country name","Life Ladder")).withColumn("rank", row_number().over(Window.partitionBy($"year").orderBy($"Life Ladder".desc))).orderBy("rank", "year").filter(col("rank") === "1").groupBy("Country name").count.withColumnRenamed("count","Num of Top Position"))

// COMMAND ----------

//Pregunta 4
import org.apache.spark.sql.functions.{max}

display(df2.select("year", "Country name", "Life Ladder", "Log GDP per capita").withColumn("rank", row_number().over(Window.partitionBy($"year").orderBy($"Life Ladder".desc))).orderBy($"Log GDP per capita".desc).filter(col("year") === "2020").limit(1))

// COMMAND ----------

//Pregunta 5
import org.apache.spark.sql.functions.{avg, lag, round}

val df3 = df2.groupBy("year").avg("Log GDP per capita").filter(col("year") === "2020").union(df1.withColumn("year", typedLit(2021)).groupBy("year").avg("Logged GDP per capita")).withColumnRenamed("avg(Log GDP per capita)","Global avg GPD").withColumn("diff", col("Global avg GPD") - lag("Global avg GPD", 1, null).over(Window.partitionBy().orderBy("year"))).withColumn("diff percent", round((col("diff") / lag("Global avg GPD", 1, null).over(Window.partitionBy().orderBy("year")))  * 100, 2))
display(df3)



// COMMAND ----------

//Pregunta 6 con scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import org.apache.spark.sql.functions.typedLit
 
display(df2.select("year", "Country name", "Healthy life expectancy at birth").filter(col("year") ===  "2019").orderBy($"Healthy life expectancy at birth".desc).limit(1))

