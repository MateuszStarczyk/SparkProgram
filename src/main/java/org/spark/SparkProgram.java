package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class SparkProgram {

    private static final String VEHICLES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/vehicles-full.csv";
    private static final String ACCIDENTS_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/accidents-full.csv";
    private static final String CASUALTIES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/casualties-full.csv";

    public static void main(String[] args) {

        DataProcessing dataProcessing;

        DataFrame roads;
        DataFrame conditions;
        DataFrame places;
        DataFrame dates;
        DataFrame drivers;
        DataFrame accidents;

        SparkConf conf = new SparkConf().setMaster("local").setAppName("MySpark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());

        dataProcessing = new DataProcessing(sqlContext,
                VEHICLES_CSV_PATH, ACCIDENTS_CSV_PATH, CASUALTIES_CSV_PATH);


        dataProcessing.readData();
        roads = dataProcessing.processRoads();
        conditions = dataProcessing.processConditions();
        places = dataProcessing.processPlaces();
        dates = dataProcessing.processDates();
        DataFrame tempDfDrivers = dataProcessing.processDrivers();
        accidents = dataProcessing.processAccidents(tempDfDrivers);
        drivers = dataProcessing.cleanUpDrivers(tempDfDrivers);

        dataProcessing.saveDataFrame(roads, "car_accidents.roads");
        dataProcessing.saveDataFrame(conditions, "car_accidents.conditions");
        dataProcessing.saveDataFrame(places, "car_accidents.places");
        dataProcessing.saveDataFrame(drivers, "car_accidents.drivers");
        dataProcessing.saveDataFrame(dates, "car_accidents.dates");
        dataProcessing.saveDataFrame(accidents, "car_accidents.accidents");

        //FUNCTIONAL TESTING
        Testing testing = new Testing(sqlContext);

        testing.process();
        boolean test1Result = testing.test1();
        boolean test2Result = testing.test2();
        boolean test3Result = testing.test3();
        boolean test4Result = testing.test4();
        boolean test5Result = testing.test5();

        //PERFORMANCE TESTING
        PerformanceTesting performanceTesting = new PerformanceTesting(sqlContext);

        long performanceTest1SmallResult = performanceTesting.test1Small();
        long performanceTest1MediumResult = performanceTesting.test1Medium();
        long performanceTest1LargeResult = performanceTesting.test1Large();

        long performanceTest2OneTypeResult = performanceTesting.test2OneType();
        long performanceTest2AllTypesResult = performanceTesting.test2AllTypes();

        long performanceTest3LowResult = performanceTesting.test3Low();
        long performanceTest3HighResult = performanceTesting.test3High();

        System.out.println("TEST 1: " + test1Result);
        System.out.println("TEST 2: " + test2Result);
        System.out.println("TEST 3: " + test3Result);
        System.out.println("TEST 4: " + test4Result);
        System.out.println("TEST 5: " + test5Result);

        System.out.println("TEST 1 - MAŁY ZBIÓR (1K) = " + performanceTest1SmallResult + " [ms]");
        System.out.println("TEST 1 - ŚREDNI ZBIÓR (10K) = " + performanceTest1MediumResult + " [ms]");
        System.out.println("TEST 1 - DUŻY ZBIÓR (100K) = " + performanceTest1LargeResult + " [ms]");

        System.out.println("TEST 2 - JEDEN TYP KIEROWCY = " + performanceTest2OneTypeResult + " [ms]");
        System.out.println("TEST 2 - WSZYSTKIE TYPY KIEROWCÓW = " + performanceTest2AllTypesResult + " [ms]");

        System.out.println("TEST 3 - MAŁA LICZBA OFIAR I POJAZDÓW = " + performanceTest3LowResult + " [ms]");
        System.out.println("TEST 3 - DUŻA LICZBA OFIAR I POJAZDÓW = " + performanceTest3HighResult + " [ms]");
    }

}
