package com.knsoft

import java.time
import java.io.File


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataFrameCsvDemo{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("csv-df").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val employeeDF = spark.read.option("header","true").option("inferschema", "true").csv("D:/users/tinku/bigdata/data/task1/employee.csv")

    val simpleSchema = StructType(Array(
      StructField("deptid", IntegerType, true),
      StructField("deptname", StringType, true)
    ))

    val departmentDF = spark.read.schema(simpleSchema).option("delimiter","\t").csv("D:/users/tinku/bigdata/data/task1/departments.tsv")

    val resultDF = employeeDF.join(departmentDF, employeeDF("dept")===departmentDF("deptid"))
    resultDF.createOrReplaceTempView("result")
    var finalDF = spark.sql("""select id as EMPLOYEE_ID, name as EMPLOYEE_NAME, deptname as EMPLOYEE_DEPT from result""")

    finalDF.write.option("header", true).option("delimiter", "|").csv("D:/users/tinku/bigdata/data/task1/employee_currentTimestamp")
    val currentTimestamp = time.LocalDateTime.now().toString.replace("T", "_").replace("-", "").replace(":", "").replace(".", "")

    println(currentTimestamp)
    new File("D:/users/tinku/bigdata/data/task1/employee_currentTimestamp/part-00000-cd28da42-5762-483b-84b9-aa75a68ba61c-c000.csv").renameTo(new File("D:/users/tinku/bigdata/data/task1/employee_currentTimestamp/employee_"+currentTimestamp+".csv"))

    spark.close()
  }
}