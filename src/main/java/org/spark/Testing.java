package org.spark;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class Testing {

    private static final String TEST_VEHICLES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-vehicles.csv";
    private static final String TEST_ACCIDENTS_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-accidents.csv";
    private static final String TEST_CASUALTIES_CSV_PATH = "hdfs://quickstart.cloudera:8020/user/cloudera/data/test-data/test-data-casualties.csv";

    private final DataProcessing dataProcessing;

    private DataFrame dates;
    private DataFrame drivers;
    private DataFrame accidents;

    public Testing(HiveContext sqlContext) {
        this.dataProcessing = new DataProcessing(sqlContext,
                TEST_VEHICLES_CSV_PATH, TEST_ACCIDENTS_CSV_PATH, TEST_CASUALTIES_CSV_PATH);
    }

    public void process() {
        dataProcessing.readData();
        dates = dataProcessing.processDates();
        DataFrame tempDfDrivers = dataProcessing.processDrivers();
        accidents = dataProcessing.processAccidents(tempDfDrivers);
        drivers = dataProcessing.cleanUpDrivers(tempDfDrivers);
        dataProcessing.saveDataFrame(drivers, "car_accidents.drivers_test");
        dataProcessing.saveDataFrame(dates, "car_accidents.dates_test");
        dataProcessing.saveDataFrame(accidents, "car_accidents.accidents_test");
    }

    public boolean test1() {
        return
                drivers.collectAsList().size() == 20 &&
                        drivers.collectAsList().get(0).getString(2).equals("46+") &&
                        drivers.collectAsList().get(0).getString(3).equals("10+") &&
                        drivers.collectAsList().get(1).getString(2).equals("Unknown") &&
                        drivers.collectAsList().get(1).getString(3).equals("10+") &&
                        drivers.collectAsList().get(13).getString(2).equals("30-45") &&
                        drivers.collectAsList().get(13).getString(3).equals("Unknown");
    }

    public boolean test2() {
        return
                drivers.collectAsList().size() == 20 &&
                        drivers.collectAsList().get(0).getString(1).equals("Passenger car driver") &&
                        drivers.collectAsList().get(5).getString(1).equals("Others") &&
                        drivers.collectAsList().get(13).getString(1).equals("Taxi driver");
    }

    public boolean test3() {
        return
                dates.collectAsList().size() == 3 &&
                        dates.collectAsList().get(0).getString(2).equals("2019") &&
                        dates.collectAsList().get(0).getString(3).equals("01") &&
                        dates.collectAsList().get(1).getString(2).equals("2019") &&
                        dates.collectAsList().get(1).getString(3).equals("02") &&
                        dates.collectAsList().get(2).getString(2).equals("2019") &&
                        dates.collectAsList().get(2).getString(3).equals("01");
    }

    public boolean test4() {
        return accidents.sort(accidents.col("Accident_ID")).collectAsList().size() == 20 &&
                String.valueOf(accidents.collectAsList().get(7).getDouble(5)).equals("31.5");
    }

    public boolean test5() {
        return accidents.sort(accidents.col("Accident_ID")).collectAsList().size() == 20 &&
                String.valueOf(accidents.collectAsList().get(12).getDouble(6)).equals("2.3333333333333335");
    }

}
