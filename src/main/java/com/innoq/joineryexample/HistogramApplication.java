package com.innoq.joineryexample;

import joinery.DataFrame;

import java.io.IOException;
import java.util.List;

public class HistogramApplication {

    private static int NUM_BUCKETS = 20; // number of segments for histogram

    public static void main(String[] argv) {

        try {
            DataFrame<Object> platforms = readPlatformData();
            System.out.println("platforms: " + platforms.types());
            createHistogram(platforms);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createHistogram(DataFrame<Object> platforms) {

        DataFrame<Object> p = platforms
                .retain(4) // keep only length of platforms
                .sortBy(0);
        final int low = ((Double) p.get(0, 0)).intValue();          // min value
        final int high = ((Double) p.tail(1).get(0, 0)).intValue(); // max value

        final int step = (high - low) / NUM_BUCKETS;

        // group platforms into buckets according to length
        DataFrame<Object> histData = p.groupBy(new DataFrame.KeyFunction<Object>() {
            public Object apply(List<Object> row) {
                double l = Double.class.cast(row.get(0));
                return numberToBucket(low, step, l);
            }
        })
                .count();
        histData.rename("Nettobaulänge (m)", "Anzahl");
        histData.plot(DataFrame.PlotType.BAR);

        System.out.println("histData: \n" + histData);

    }

    private static int numberToBucket(int low, int step, double value) {
        int intValue = new Double(value).intValue();
        int offset = (intValue - low) % step;
        return intValue - offset;
    }

    private static DataFrame<Object> readPlatformData() throws IOException {
        String url = "http://download-data.deutschebahn.com/static/datasets/bahnsteig/DBSuS-Bahnsteigdaten-Stand2016-03.csv";
        DataFrame<Object> platforms = DataFrame.readCsv(url, ";");
        return platforms;
    }

}
