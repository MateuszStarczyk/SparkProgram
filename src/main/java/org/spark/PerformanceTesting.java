package org.spark;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class PerformanceTesting {

    private static final String LP_TEST1_SMALL_VEHICLES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-vehicles.csv";
    private static final String LP_TEST1_SMALL_ACCIDENTS_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-accidents.csv";
    private static final String LP_TEST1_SMALL_CASUALTIES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-casualties.csv";

    private static final String LP_TEST1_MEDIUM_VEHICLES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-vehicles.csv";
    private static final String LP_TEST1_MEDIUM_ACCIDENTS_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-accidents.csv";
    private static final String LP_TEST1_MEDIUM_CASUALTIES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-casualties.csv";

    private static final String LP_TEST1_LARGE_VEHICLES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-vehicles.csv";
    private static final String LP_TEST1_LARGE_ACCIDENTS_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-accidents.csv";
    private static final String LP_TEST1_LARGE_CASUALTIES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-casualties.csv";

    private static final String LP_TEST2_ONETYPE_VEHICLES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-vehicles.csv";
    private static final String LP_TEST2_ONETYPE_ACCIDENTS_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-accidents.csv";
    private static final String LP_TEST2_ONETYPE_CASUALTIES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-casualties.csv";

    private static final String LP_TEST2_ALLTYPES_VEHICLES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-vehicles.csv";
    private static final String LP_TEST2_ALLTYPES_ACCIDENTS_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-accidents.csv";
    private static final String LP_TEST2_ALLTYPES_CASUALTIES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-casualties.csv";

    private static final String LP_TEST3_LOW_VEHICLES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-vehicles.csv";
    private static final String LP_TEST3_LOW_ACCIDENTS_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-accidents.csv";
    private static final String LP_TEST3_LOW_CASUALTIES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-casualties.csv";

    private static final String LP_TEST3_HIGH_VEHICLES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-vehicles.csv";
    private static final String LP_TEST3_HIGH_ACCIDENTS_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-accidents.csv";
    private static final String LP_TEST3_HIGH_CASUALTIES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-casualties.csv";

    private final HiveContext sqlContext;

    PerformanceTesting(HiveContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    public long test1Small() {
        return process("LP1_SMALL", LP_TEST1_SMALL_VEHICLES_CSV_PATH, LP_TEST1_SMALL_ACCIDENTS_CSV_PATH, LP_TEST1_SMALL_CASUALTIES_CSV_PATH);
    }

    public long test1Medium() {
        return process("LP1_MEDIUM", LP_TEST1_MEDIUM_VEHICLES_CSV_PATH, LP_TEST1_MEDIUM_ACCIDENTS_CSV_PATH, LP_TEST1_MEDIUM_CASUALTIES_CSV_PATH);
    }

    public long test1Large() {
        return process("LP1_LARGE", LP_TEST1_LARGE_VEHICLES_CSV_PATH, LP_TEST1_LARGE_ACCIDENTS_CSV_PATH, LP_TEST1_LARGE_CASUALTIES_CSV_PATH);
    }

    public long test2OneType() {
        return process("LP2_ONETYPE", LP_TEST2_ONETYPE_VEHICLES_CSV_PATH, LP_TEST2_ONETYPE_ACCIDENTS_CSV_PATH, LP_TEST2_ONETYPE_CASUALTIES_CSV_PATH);
    }

    public long test2AllTypes() {
        return process("LP2_ALLTYPES", LP_TEST2_ALLTYPES_VEHICLES_CSV_PATH, LP_TEST2_ALLTYPES_ACCIDENTS_CSV_PATH, LP_TEST2_ALLTYPES_CASUALTIES_CSV_PATH);
    }

    public long test3Low() {
        return process("LP3_LOW", LP_TEST3_LOW_VEHICLES_CSV_PATH, LP_TEST3_LOW_ACCIDENTS_CSV_PATH, LP_TEST3_LOW_CASUALTIES_CSV_PATH);
    }

    public long test3High() {
        return process("LP3_HIGH", LP_TEST3_HIGH_VEHICLES_CSV_PATH, LP_TEST3_HIGH_ACCIDENTS_CSV_PATH, LP_TEST3_HIGH_CASUALTIES_CSV_PATH);
    }

    private long process(String tableSuffix, String vehiclesCsvPath, String accidentsCsvPath, String casualtiesCsvPath) {
        long start = System.currentTimeMillis();

        DataProcessing dataProcessing = new DataProcessing(sqlContext, vehiclesCsvPath, accidentsCsvPath, casualtiesCsvPath);
        dataProcessing.readData();

        DataFrame roads = dataProcessing.processRoads();
        DataFrame conditions = dataProcessing.processConditions();
        DataFrame places = dataProcessing.processPlaces();
        DataFrame dates = dataProcessing.processDates();
        DataFrame tempDfDrivers = dataProcessing.processDrivers();
        DataFrame accidents = dataProcessing.processAccidents(tempDfDrivers);
        DataFrame drivers = dataProcessing.cleanUpDrivers(tempDfDrivers);

        dataProcessing.saveDataFrame(roads, "car_accidents.roads_" + tableSuffix);
        dataProcessing.saveDataFrame(conditions, "car_accidents.conditions_" + tableSuffix);
        dataProcessing.saveDataFrame(places, "car_accidents.places_" + tableSuffix);
        dataProcessing.saveDataFrame(drivers, "car_accidents.drivers_" + tableSuffix);
        dataProcessing.saveDataFrame(dates, "car_accidents.dates_" + tableSuffix);
        dataProcessing.saveDataFrame(accidents, "car_accidents.accidents_" + tableSuffix);

        return System.currentTimeMillis() - start;
    }

}
