package com.innoq.joineryexample;

import joinery.DataFrame;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class Application {

    public static void main(String[] argv) {
        String fileName = null;
        if (argv.length > 0) {
            fileName = argv[0];
        }
        System.out.println("starting with file - " + fileName);

        if (fileName != null) {
            try {
                tryToRead(fileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        BufferedReader buf = new BufferedReader(new InputStreamReader(System.in));
        try {
            System.out.println("Enter to exit");
            buf.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void tryToRead(String fileName) throws IOException {
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
