package org.spark;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import static org.apache.spark.sql.functions.*;

public class DataProcessing {

    private final String vehiclesCsvPath;
    private final String accidentsCsvPath;
    private final String casualtiesCsvPath;

    private final HiveContext sqlContext;
    private DataFrame dfSourceVehicles;
    private DataFrame dfSourceAccidents;
    private DataFrame dfSourceCasualties;

    public DataProcessing(HiveContext sqlContext,
                          String vehiclesCsvPath,
                          String accidentsCsvPath,
                          String casualtiesCsvPath) {
        this.sqlContext = sqlContext;
        this.vehiclesCsvPath = vehiclesCsvPath;
        this.accidentsCsvPath = accidentsCsvPath;
        this.casualtiesCsvPath = casualtiesCsvPath;
    }

    public void readData() {
        dfSourceVehicles = readDataFrameFromCsv(vehiclesCsvPath);
        dfSourceAccidents = readDataFrameFromCsv(accidentsCsvPath);
        dfSourceCasualties = readDataFrameFromCsv(casualtiesCsvPath);
    }

    public DataFrame processRoads() {
        return dfSourceAccidents
                .select(col("1st_Road_Number").as("Road_Number"), col("Road_Type"), col("1st_Road_Class").as("Road_Class"))
                .distinct();
    }

    public DataFrame processConditions() {
        return dfSourceAccidents.select(
                when(dfSourceAccidents.col("Light_Conditions").equalTo(-1), 0).otherwise(lit(dfSourceAccidents.col("Light_Conditions"))).as("Light_Conditions"),
                when(dfSourceAccidents.col("Weather_Conditions").equalTo(-1), 0).otherwise(lit(dfSourceAccidents.col("Weather_Conditions"))).as("Weather_Conditions"),
                when(dfSourceAccidents.col("Road_Surface_Conditions").equalTo(-1), 0).otherwise(lit(dfSourceAccidents.col("Road_Surface_Conditions"))).as("Road_Surface_Conditions"))
                .withColumn("Conditions_ID",
                        col("Light_Conditions").multiply(100)
                                .plus(col("Weather_Conditions").multiply(10)
                                        .plus(col("Road_Surface_Conditions"))));
    }

    public DataFrame processPlaces() {
        return dfSourceAccidents.select(col("Accident_Index").as("Accident_ID"), col("Longitude"),
                col("Latitude"), col("Local_Authority_(District)").as("District"),
                col("Local_Authority_(Highway)").as("Highway"), col("Speed_limit"));
    }

    public DataFrame processDates() {
        return dfSourceAccidents.select("Date", "Day_of_Week").withColumn("Year", substring(col("Date"), 7, 4)).withColumn("Month", substring(col("Date"), 4, 2))
                .distinct();
    }

    public DataFrame processDrivers() {
        return dfSourceVehicles.select(col("Accident_Index"), col("Vehicle_Reference"),
                col("Age_of_Driver"),
                when(col("Age_of_Driver").between(0, 29), "0-29")
                        .when(col("Age_of_Driver").between(30, 45), "30-45")
                        .when(col("Age_of_Driver").$less(0), "Unknown")
                        .otherwise("46+").as("Driver_Age_Band"),
                col("Age_of_Vehicle"),
                when(col("Age_of_Vehicle").between(0, 5), "0-5")
                        .when(col("Age_of_Vehicle").between(6, 9), "6-9")
                        .when(col("Age_of_Vehicle").$less(0), "Unknown")
                        .otherwise("10+").as("Vehicle_Age_Band"),
                col("Vehicle_Type"),
                col("Journey_Purpose_of_Driver"),
                when(col("Vehicle_Type").equalTo("8"), "Taxi driver")
                        .when(col("Vehicle_Type").equalTo("9"), "Passenger car driver")
                        .when(col("Vehicle_Type").in(2, 3, 4, 5, 97), "Motorcyclist")
                        .when(col("Vehicle_Type").in(19, 20, 21, 98).and(col("Journey_Purpose_of_Driver").equalTo(1)), "Professional truck driver")
                        .otherwise("Others").as("Driver_Type"))
                .withColumn("Driver_ID", monotonically_increasing_id());
    }

    public DataFrame processAccidents(DataFrame dfDrivers) {
        DataFrame avgCasualty = dfSourceCasualties.select(col("Age_of_Casualty"), col("Casualty_Severity"), col("Accident_Index"))
                .groupBy("Accident_Index")
                .agg(avg("Age_of_Casualty").as("Average_Casualty_Age"), avg("Casualty_Severity").as("Average_Casualty_Severity"));
        DataFrame accidentsWithCasualties = avgCasualty
                .join(
                        dfSourceAccidents,
                        avgCasualty.col("Accident_Index").equalTo(dfSourceAccidents.col("Accident_Index")))
                .drop(dfSourceAccidents.col("Accident_Index"));

        DataFrame accidentsWithConditions = accidentsWithCasualties
                .withColumn("Conditions_ID",
                        when(dfSourceAccidents.col("Light_Conditions").equalTo(-1), 0)
                                .otherwise(lit(dfSourceAccidents.col("Light_Conditions"))).as("Light_Conditions").multiply(100)
                                .plus(when(dfSourceAccidents.col("Weather_Conditions").equalTo(-1), 0)
                                        .otherwise(lit(dfSourceAccidents.col("Weather_Conditions"))).as("Weather_Conditions").multiply(10)
                                        .plus(when(dfSourceAccidents.col("Road_Surface_Conditions").equalTo(-1), 0)
                                                .otherwise(lit(dfSourceAccidents.col("Road_Surface_Conditions"))).as("Road_Surface_Conditions"))));

        DataFrame accidentsWithDrivers = dfDrivers
                .join(accidentsWithConditions, dfDrivers.col("Accident_Index").equalTo(accidentsWithConditions.col("Accident_Index")))
                .drop(dfDrivers.col("Accident_Index"));
        return accidentsWithDrivers.
                select(col("Accident_Index").as("Accident_ID"), col("Driver_ID"), col("Number_of_Vehicles"),
                        col("Number_of_Casualties"), col("Accident_Severity").as("Severity"), col("Average_Casualty_Age"),
                        col("Average_Casualty_Severity"), col("1st_Road_Number").as("Road_Number"), col("Conditions_Id"),
                        col("Date"));
    }

    public DataFrame cleanUpDrivers(DataFrame dfDrivers) {
        return dfDrivers.select("Driver_ID", "Driver_Type", "Driver_Age_Band", "Vehicle_Age_Band");
    }

    public void saveDataFrame(DataFrame dataFrame, String tableName) {
        dataFrame.write().mode(SaveMode.Overwrite)
                .saveAsTable(tableName);
    }

    private DataFrame readDataFrameFromCsv(String path) {
        return sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true") // Use first line of all files as header
                .option("inferSchema", "true") // Automatically infer data types
                .load(path);
    }
}
