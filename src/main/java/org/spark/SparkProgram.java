package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import static org.apache.spark.sql.functions.*;

public class SparkProgram {


    public static void main(String[] args) {
        System.out.println("DUPA");

        SparkConf conf = new SparkConf().setMaster("local").setAppName("MySpark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());
        DataFrame dfVehicles = getVehicles(sqlContext);
        DataFrame dfAccidents = getAccidents(sqlContext);
        DataFrame dfCasualties = getCasualties(sqlContext);

//        dfAccidents.select(col("1st_Road_Number").as("Road_Number"), col("Road_Type"), col("1st_Road_Class").as("Road_Class"))
//                .distinct()
//                .write().mode(SaveMode.Overwrite)
//                .saveAsTable("car_accidents.road_temp");
//        //"Light_Conditions", "Weather_Conditions", "Road_Surface_Conditions"
        DataFrame dfConditions = dfAccidents.select("Light_Conditions", "Weather_Conditions", "Road_Surface_Conditions").distinct()
                .withColumn("Conditions_ID", monotonically_increasing_id());//.write().mode(SaveMode.Overwrite).saveAsTable("car_accidents.conditions_temp1");
//        dfAccidents.select(col("Longitude"), col("Latitude"), col("Local_Authority_(District)").as("District"), col("Local_Authority_(Highway)").as("Highway"), col("Speed_limit"))
//                .distinct()
//                .withColumn("Place_ID", monotonically_increasing_id())
//                .write().mode(SaveMode.Overwrite)
//                .saveAsTable("car_accidents.places_temp1");
//        dfAccidents.select("Date", "Day_of_Week").withColumn("Year",  substring(col("Date"), 6, 9)).withColumn("Month", substring(col("Date"), 3, 4))
//                .distinct()
//                .write().mode(SaveMode.Overwrite)
//                .saveAsTable("car_accidents.dates_temp1");

//        dfVehicles.select(col("Accident_Index"), col("Vehicle_Reference"),
//                col("Age_of_Driver"),
//                when(col("Age_of_Driver").between(0, 29), "0-29")
//                        .when(col("Age_of_Driver").between(30, 45), "30-45")
//                        .when(col("Age_of_Driver").$less(0), "-1")
//                        .otherwise("46+").as("Driver_Age_Band"),
//                col("Age_of_Vehicle"),
//                when(col("Age_of_Vehicle").between(0, 5), "0-5")
//                        .when(col("Age_of_Vehicle").between(6, 9), "6-9")
//                        .when(col("Age_of_Vehicle").$less(0), "-1")
//                        .otherwise("10+").as("Vehicle_Age_Band"),
//                col("Vehicle_Type"),
//                col("Journey_Purpose_of_Driver"),
//                when(col("Vehicle_Type").equalTo("8"), "Taxi driver")
//                        .when(col("Vehicle_Type").equalTo("9"), "Passenger car driver")
//                        .when(col("Vehicle_Type").in(2, 3, 4, 5, 97), "Motorcyclist")
//                        .when(col("Vehicle_Type").in(19, 20, 21, 98).and(col("Journey_Purpose_of_Driver").equalTo(1)), "Professional truck driver")
//                        .otherwise("Others").as("Driver_Type"))
//                .withColumn("Driver_ID", monotonically_increasing_id())
//                .write().mode(SaveMode.Overwrite)
//                .saveAsTable("car_accidents.bands");

        DataFrame avgCasualty = dfCasualties.select(col("Age_of_Casualty"), col("Casualty_Severity"), col("Accident_Index"))
                .groupBy("Accident_Index")
                .agg(avg("Age_of_Casualty").as("Average_Casualty_Age"), avg("Casualty_Severity").as("Average_Casualty_Severity"));
        DataFrame accidentsWithCasualties = avgCasualty
                .join(
                        dfAccidents.select(col("Accident_Index"),
                                col("Number_of_Vehicles"),
                                col("1st_Road_Number").as("Road_Number"),
                                col("Light_Conditions"),
                                col("Weather_Conditions"),
                                col("Road_Surface_Conditions")),
                        avgCasualty.col("Accident_Index").equalTo(dfAccidents.col("Accident_Index")))
                .drop(dfAccidents.col("Accident_Index"));

        DataFrame accidentsWithConditions = accidentsWithCasualties
                .join(dfConditions.select(col("Conditions_ID")),
                        dfConditions.col("Light_Conditions").equalTo(accidentsWithCasualties.col("Light_Conditions"))
                                .and(dfConditions.col("Weather_Conditions").equalTo(accidentsWithCasualties.col("Weather_Conditions")))
                                .and(dfConditions.col("Road_Surface_Conditions").equalTo(accidentsWithCasualties.col("Road_Surface_Conditions"))));
        accidentsWithConditions.write().mode(SaveMode.Overwrite).saveAsTable("car_accidents.accidents_temp");
    }

    public static DataFrame getVehicles(HiveContext sqlContext) {
        return sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true") // Use first line of all files as header
                .option("inferSchema", "true") // Automatically infer data types
                .load("hdfs://quickstart.cloudera:8020/user/cloudera/data/vehicles-full.csv");
    }

    public static DataFrame getAccidents(HiveContext sqlContext) {
        return sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true") // Use first line of all files as header
                .option("inferSchema", "true") // Automatically infer data types
                .load("hdfs://quickstart.cloudera:8020/user/cloudera/data/accidents-full.csv");
    }

    public static DataFrame getCasualties(HiveContext sqlContext) {
        return sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true") // Use first line of all files as header
                .option("inferSchema", "true") // Automatically infer data types
                .load("hdfs://quickstart.cloudera:8020/user/cloudera/data/casualties-full.csv");
    }


}
