
import java.io.File;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import static java.time.temporal.ChronoUnit.MILLIS;

public class Metrics {

    public static final String result0 = "results_chord_20_nodes_1000_interval_100_size";


    public static float TotalMessagesReceived = 0;
    public static long BytesReceived = 0;

    public static float TotalMessagesTransmitted = 0;
    public static long BytesTransmitted = 0;

    public static float StoreLatency = 0;
    public static float RetrieveLatency = 0;

    public static Map<String, LocalTime> stored = new HashMap<>();
    public static Map<String, LocalTime> retrieve = new HashMap<>();

    public static float numberOfRetrieveRequest = 0;
    public static float numberOfStoreRequest = 0;


    public static void main(String[] args) throws Exception {
        final File folder = new File(result0);
        listFilesForFolder(folder);

        System.out.println("TotalMessagesReceived: " + TotalMessagesReceived);
        System.out.println("BytesReceived: " + BytesReceived);
        System.out.println("TotalMessagesTransmitted: " + TotalMessagesTransmitted);
        System.out.println("BytesTransmitted: " + BytesTransmitted);

        System.out.println("failedRetrieves: " + retrieve.keySet().size());
        System.out.println("numberOfRetrieveRequest: " + numberOfRetrieveRequest);

        System.out.println("RecallRate: " + (numberOfRetrieveRequest - retrieve.keySet().size()) / numberOfRetrieveRequest);
        System.out.println("StoreLatency: " + StoreLatency/numberOfStoreRequest);
        System.out.println("RetrieveLatency: " + RetrieveLatency/numberOfRetrieveRequest);
    }

    public static void listFilesForFolder(final File folder) {
        for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
            parseFile(fileEntry);
        }
    }

    public static void parseFile(File file) {
        try {
            Scanner myReader = new Scanner(file);
            while (myReader.hasNextLine()) {
                parseLine(myReader.nextLine());
            }
            myReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void parseLine(String line) {
        if(line.contains("Message received with size")) {
            TotalMessagesReceived++;
            BytesReceived = Long.parseLong(line.split("Message received with size ")[1]);
        }
        else if(line.contains("Message sent with size")) {
            TotalMessagesTransmitted++;
            BytesTransmitted = Long.parseLong(line.split("Message sent with size ")[1]);
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
            LocalTime time = LocalTime.parse(timestamp[0]).plus(Long.parseLong(timestamp[1]), MILLIS);
            StoreLatency += MILLIS.between(oldTime, time);
        }
        else if(line.contains("Received reply RetrieveOKReply")) {
            String[] h = line.split(" Received reply RetrieveOKReply");
            String[] timestamp = h[0].replace("[","").replace("]","").split(",");
            String requestId = h[1].split(",")[0].split("=")[1];
            LocalTime oldTime = retrieve.remove(requestId);
            LocalTime time = LocalTime.parse(timestamp[0]).plus(Long.parseLong(timestamp[1]), MILLIS);
            RetrieveLatency += MILLIS.between(oldTime, time);
        }
    }
}
