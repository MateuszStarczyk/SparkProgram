package org.spark;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class SparkProgram {


    public static void main(String[] args) {
        final Logger logger = LogManager.getRootLogger();
        System.out.println("DUPA");

        SparkConf conf = new SparkConf().setMaster("local").setAppName("MySpark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());
        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true") // Use first line of all files as header
                .option("inferSchema", "true") // Automatically infer data types
                .load("hdfs://quickstart.cloudera:8020/user/cloudera/data/accidents-full.csv");
//        df.select("Accident_Index", "Longitude", "Latitude", "Accident_Severity", "Number_of_Vehicles",
//                "Number_of_Casualties", "Date", "Day_of_Week", "Time", "Local_Authority_(District)", "Local_Authority_(Highway)",
//                "1st_Road_Class", "1st_Road_Number", "Road_Type", "Speed_limit", "Light_Conditions", "Weather_Conditions",
//                "Road_Surface_Conditions").show();
        df.select("1st_Road_Number", "Road_Type", "1st_Road_Class").distinct().write().saveAsTable(" car_accidents.road_temp");
    }
}
