package com.innoq.joineryexample;

import joinery.DataFrame;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Observable;

public class Application {

    public static void main(String[] argv) {
        String fileName = null;
        if (argv.length > 0) {
            fileName = argv[0];
        }
        System.out.println("starting with file - " + fileName);

        try {
            DataFrame<Object> platforms = readPlatformData();
            System.out.println("platforms: "+ platforms.types());
            DataFrame<Object> stations = readStationData();
            System.out.println("stations: "+ stations.types());
            processPlatforms(platforms, stations);

        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedReader buf = new BufferedReader(new InputStreamReader(System.in));
        try {
            System.out.println("Enter to exit");
            buf.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processPlatforms(DataFrame<Object> platforms, DataFrame<Object> stations) {

//        Integer[] pCols = {0, 2};
        DataFrame<Object> p = platforms.retain(0, 4).groupBy(0).sum();
        System.out.println(p);
        DataFrame<Object> s = stations.reindex(2, false);
        System.out.println(s);
        System.out.println("trying");

        DataFrame<Object> dataFrame = p.join(s);
        System.out.print(dataFrame);
    }

    private static DataFrame<Object> readStationData() throws IOException {
        String url = "http://download-data.deutschebahn.com/static/datasets/stationsdaten/DBSuS-Uebersicht_Bahnhoefe-Stand2016-07.csv";
        DataFrame<Object> stations = DataFrame.readCsv(url, ";");
        return stations;
    }

    private static DataFrame<Object> readPlatformData() throws IOException {
        String url = "http://download-data.deutschebahn.com/static/datasets/bahnsteig/DBSuS-Bahnsteigdaten-Stand2016-03.csv";
        DataFrame<Object> platforms = DataFrame.readCsv(url, ";");
        return platforms;
    }

    private static void tryToReadLocal(String fileName) throws IOException {
        Long s1 = new Date().getTime();
        DataFrame<Object> df = DataFrame.readCsv(fileName)
                .retain("COUNTYNAME", "STATE")
                .groupBy("COUNTYNAME")
                .count();

        Long s2 = new Date().getTime();
        LocalTime t = LocalTime.MIDNIGHT.plus(Duration.ofMillis(s2 - s1));
        String dur = DateTimeFormatter.ofPattern("m:ss.SSS").format(t);
        System.out.println("reading took " + dur + " sec");
//        df.plot();

        df = null;

    }

}
