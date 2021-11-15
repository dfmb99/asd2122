
import java.io.File;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;

import static java.time.temporal.ChronoUnit.*;

public class Metrics {

    public static final int prepareTime = 120;
    public static final int runTimeEnd = 240;
    public static final int prepareTolerance = 500; //Adjust to minimum until no errors appear
    public static final int runTolerance = 500; //Adjust to minimum until no errors appear

    public static LocalTime startTime;

    public static final String test = "test";
    public static final String result1 = "results_chord_base/results";
    public static final String result2 = "results_chord_rate_200/results";
    public static final String result3 = "results_chord_rate_200_size_10k/results";
    public static final String result4 = "results_chord_size_10k/results";


    public static float TotalMessagesReceived = 0;
    public static long BytesReceived = 0;

    public static float TotalMessagesTransmitted = 0;
    public static long BytesTransmitted = 0;

    public static float StoreLatency = 0;
    public static float RetrieveLatency = 0;

    public static Map<String, LocalTime> stored;
    public static Map<String, LocalTime> retrieve;

    public static float numberOfRetrieveRequest = 0;
    public static float numberOfStoreRequest = 0;

    public static boolean started = false;

    public static void main(String[] args) {
        final File folder = new File(test);
        listFilesForFolder(folder);

        System.out.println("TotalMessagesReceived: " + TotalMessagesReceived);
        System.out.println("BytesReceived: " + BytesReceived);
        System.out.println("TotalMessagesTransmitted: " + TotalMessagesTransmitted);
        System.out.println("BytesTransmitted: " + BytesTransmitted);

        System.out.println("RecallRate: " + (numberOfRetrieveRequest - retrieve.keySet().size()) / numberOfRetrieveRequest);
        System.out.println("StoreLatency: " + StoreLatency/numberOfStoreRequest);
        System.out.println("RetrieveLatency: " + RetrieveLatency/numberOfRetrieveRequest);

        System.out.println("numberOfRetrieveRequest: " + numberOfRetrieveRequest);
        System.out.println("numberOfStoreRequest: " + numberOfStoreRequest);
    }

    public static void listFilesForFolder(final File folder) {
        for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
            stored = new HashMap<>();
            retrieve = new HashMap<>();
            started = false;
            startTime = null;
            parseFile(fileEntry);
        }
    }

    public static void parseFileByTime(File file) {
        try {
            Scanner myReader = new Scanner(file);
            while (myReader.hasNextLine()) {
                String line = myReader.nextLine();
                LocalTime currentTime;

                try {
                    String[] timestamp = line.split("]")[0].replace("[","").split(",");
                    currentTime = LocalTime.parse(timestamp[0]).plus(Long.parseLong(timestamp[1]), MILLIS);
                } catch (Exception e) {
                    if(line.contains("[") && line.contains("]")) {
                        System.out.println("Timestamp parse error: " + line);
                        System.exit(-1);
                    }
                    continue;
                }

                if(startTime == null) {
                    startTime = currentTime;
                }
                else if(currentTime.isAfter(startTime.plus(prepareTime, SECONDS).minus(prepareTolerance, MILLIS))) {
                    parseLine(line);
                }
                else if(currentTime.isAfter(startTime.plus(runTimeEnd, SECONDS).plus(runTolerance, MILLIS))) {
                    myReader.close();
                    return;
                }
            }
            myReader.close();
        } catch (Exception e) {
            System.out.println(file.getName());
            e.printStackTrace();
        }
    }

    public static void parseFile(File file) {
        try {
            Scanner myReader = new Scanner(file);
            while (myReader.hasNextLine()) {
                String line = myReader.nextLine();
                if(stopCondition(line)) {
                    myReader.close();
                    return;
                }
                if(!started) startCondition(line);
                else parseLine(line);
            }
            myReader.close();
        } catch (Exception e) {
            System.out.println(file.getName());
            e.printStackTrace();
        }
    }

    public static void startCondition(String line) {
        started = line.contains("Starting");
    }

    public static boolean stopCondition(String line) {
        return line.contains("Stopping");
    }

    public static void parseLine(String line) {
        if(line.contains("Message received with size")) {
            TotalMessagesReceived++;
            BytesReceived += Long.parseLong(line.split("Message received with size ")[1]);
        }
        else if(line.contains("Message sent with size")) {
            TotalMessagesTransmitted++;
            BytesTransmitted += Long.parseLong(line.split("Message sent with size ")[1]);
        }
        else if(line.contains("Sent request StoreRequest")) {
            numberOfStoreRequest++;
            String[] h = line.split(" Sent request StoreRequest");
            String[] timestamp = h[0].replace("[","").replace("]","").split(",");
            String requestId = h[1].split(",")[0].split("=")[1];
            LocalTime time = LocalTime.parse(timestamp[0]).plus(Long.parseLong(timestamp[1]), MILLIS);
            stored.put(requestId, time);
        }
        else if(line.contains("Sent request RetrieveRequest")) {
            numberOfRetrieveRequest++;
            String[] h = line.split(" Sent request RetrieveRequest");
            String[] timestamp = h[0].replace("[","").replace("]","").split(",");
            String requestId = h[1].split(",")[0].split("=")[1];
            LocalTime time = LocalTime.parse(timestamp[0]).plus(Long.parseLong(timestamp[1]), MILLIS);
            retrieve.put(requestId, time);
        }
        else if(line.contains("Received reply StoreOKReply")) {
            String[] h = line.split(" Received reply StoreOKReply");
            String[] timestamp = h[0].replace("[","").replace("]","").split(",");
            String requestId = h[1].split(",")[0].split("=")[1];
            LocalTime oldTime = stored.remove(requestId);
            if(oldTime == null)
                System.out.println(requestId);
            LocalTime time = LocalTime.parse(timestamp[0]).plus(Long.parseLong(timestamp[1]), MILLIS);
            StoreLatency += MILLIS.between(oldTime, time);
        }
        else if(line.contains("Received reply RetrieveOKReply")) {
            String[] h = line.split(" Received reply RetrieveOKReply");
            String[] timestamp = h[0].replace("[","").replace("]","").split(",");
            String requestId = h[1].split(",")[0].split("=")[1];
            LocalTime oldTime = retrieve.remove(requestId);
            if(oldTime == null)
                System.out.println(requestId);
            LocalTime time = LocalTime.parse(timestamp[0]).plus(Long.parseLong(timestamp[1]), MILLIS);
            RetrieveLatency += MILLIS.between(oldTime, time);
        }
    }
}
