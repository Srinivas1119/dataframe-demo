package com.knsoft

import com.knsoft.utils.FileUtils

import java.time
import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

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

    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    val reportFolder = s"D:/users/tinku/bigdata/data/task1/reports/${timestamp}/"
    finalDF.write.option("header", true).option("delimiter", "|").csv(reportFolder)
    FileUtils.renameReport(reportFolder)
    spark.close()
  }
}