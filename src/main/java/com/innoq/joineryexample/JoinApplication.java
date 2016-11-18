package com.innoq.joineryexample;

import joinery.DataFrame;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class JoinApplication {

    public static void main(String[] argv) {

        try {
            DataFrame<Object> platforms = readPlatformData();
            DataFrame<Object> stations = readStationData();
            findLongestPlatformStations(platforms, stations);

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

    private static DataFrame<Object> findLongestPlatformStations(DataFrame<Object> platforms, DataFrame<Object> stations) {

        DataFrame<Object> p = platforms.retain(0, 4) // keep 'Bahnhofsnummer' and 'nettobaulänge (m)'
                .groupBy(0)     // group (and index) by 'Bahnhofsnummer'
                .max()  // max platform length of all platforms per station
                .sortBy(-1) // sort ascending by index
                .head(5); // take first 5
        System.out.println("p" + p);
        DataFrame<Object> s = stations
                .retain(2, 3)   // keep 'Bf. Nr.' and 'Station'
                .reindex(0);    // create index, need for join
        System.out.println("s" + s);

        DataFrame<Object> dataFrame = p.join(s);
        System.out.print(dataFrame);

        return dataFrame;
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

}