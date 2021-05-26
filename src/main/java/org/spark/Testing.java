package org.spark;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class Testing {

    private static final String TEST_VEHICLES_CSV_PATH = "";
    private static final String TEST_ACCIDENTS_CSV_PATH = "";
    private static final String TEST_CASUALTIES_CSV_PATH = "";

    private final DataProcessing dataProcessing;

    private DataFrame roads;
    private DataFrame conditions;
    private DataFrame places;
    private DataFrame dates;
    private DataFrame drivers;
    private DataFrame accidents;

    public Testing(HiveContext sqlContext) {
        this.dataProcessing = new DataProcessing(sqlContext,
                TEST_VEHICLES_CSV_PATH, TEST_ACCIDENTS_CSV_PATH, TEST_CASUALTIES_CSV_PATH);
    }

    public void process() {
        dataProcessing.readData();
        roads = dataProcessing.processRoads();
        conditions = dataProcessing.processConditions();
        places = dataProcessing.processPlaces();
        dates = dataProcessing.processDates();
        DataFrame tempDfDrivers = dataProcessing.processDrivers();
        accidents = dataProcessing.processAccidents(tempDfDrivers);
        drivers = dataProcessing.cleanUpDrivers(tempDfDrivers);
    }

    public boolean test1() {
        return
                drivers.collectAsList().get(0).getString(5).equals("aaa") &&
                        drivers.collectAsList().get(0).getString(5).equals("aaa") &&
                        drivers.collectAsList().get(0).getString(5).equals("aaa") &&
                        drivers.collectAsList().get(0).getString(5).equals("aaa") &&
                        drivers.collectAsList().get(0).getString(5).equals("aaa");
    }

    public boolean test2() {
        return true;
    }

    public boolean test3() {
        return true;
    }

    public boolean test4() {
        return true;
    }

    public boolean test5() {
        return true;
    }

}
