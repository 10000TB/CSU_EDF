import java.io.IOException;

import java.util.Calendar;
import java.util.Random;

import galileo.client.EventPublisher;
import galileo.comm.StorageRequest;
import galileo.dataset.Block;
import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.dataset.TemporalProperties;
import galileo.dataset.feature.Feature;
import galileo.dataset.feature.FeatureSet;
import galileo.net.ClientMessageRouter;
import galileo.net.NetworkDestination;
import galileo.util.GeoHash;
import galileo.util.PerformanceTimer;

public class GalileoConnector {

    private static Random randomGenerator = new Random(System.nanoTime());

    private ClientMessageRouter messageRouter;
    private EventPublisher publisher;

    public GalileoConnector() throws IOException {
        messageRouter = new ClientMessageRouter();
        publisher = new EventPublisher(messageRouter);
    }

    public void disconnect() {
        messageRouter.shutdown();
    }

    public void store(NetworkDestination destination, Block fb)
    throws Exception {
        StorageRequest store = new StorageRequest(fb);
        publisher.publish(destination, store);
    }

    public static int randomInt(int start, int end) {
        return randomGenerator.nextInt(end - start + 1) + start;
    }

    public static float randomFloat() {
        return randomGenerator.nextFloat();
    }

    public static Block generateData() {
        /* First, a temporal range for this data "sample" */
        Calendar calendar = Calendar.getInstance();
        int year, month, day;

        year = randomInt(2010, 2013);
        month = randomInt(0, 11);
        day = randomInt(1, 28);

        calendar.set(year, month, day);

        /* Convert the random values to a start time, then add 1ms for the end
         * time.  This simulates 1ms worth of data. */
        long startTime = calendar.getTimeInMillis();
        long endTime   = startTime + 1;

        TemporalProperties temporalProperties
            = new TemporalProperties(startTime, endTime);


        /* The continental US */
        String[] geoRand = { "c2", "c8", "cb", "f0", "f2",
                             "9r", "9x", "9z", "dp", "dr",
                             "9q", "9w", "9y", "dn", "dq",
                             "9m", "9t", "9v", "dj" };

        String geoPre = geoRand[randomInt(0, geoRand.length - 1)];
        String hash = geoPre;

        for (int i = 0; i < 10; ++i) {
            int random = randomInt(0, GeoHash.charMap.length - 1);
            hash += GeoHash.charMap[random];
        }

        SpatialProperties spatialProperties
            = new SpatialProperties(GeoHash.decodeHash(hash));

        String[] featSet = { "wind_speed", "wind_direction", "condensation",
                             "temperature", "humidity" };

        FeatureSet features = new FeatureSet();
        for (int i = 0; i < 5; ++i) {
            String featureName = featSet[randomInt(0, featSet.length - 1)];
            features.put(new Feature(featureName, randomFloat() * 100));
        }

        Metadata metadata = new Metadata();
        metadata.setTemporalProperties(temporalProperties);
        metadata.setSpatialProperties(spatialProperties);
        metadata.setAttributes(features);

        /* Now let's make some "data" to associate with our metadata. */
        Random r = new Random(System.nanoTime());
        byte[] blockData = new byte[8000];
        r.nextBytes(blockData);

        Block b = new Block(metadata, blockData);

        return b;
    }

    public static void main(String[] args)
    throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: galileo.client.TextClient " +
                    "<server-hostname> <server-port> <num-blocks>");
            return;
        }

        String serverHostName = args[0];
        int serverPort = Integer.parseInt(args[1]);
        int num = Integer.parseInt(args[2]);

        GalileoConnector client = new GalileoConnector();
        NetworkDestination server
            = new NetworkDestination(serverHostName, serverPort);

        System.out.println("Sending " + num + " blocks...");
        PerformanceTimer pt = new PerformanceTimer("Send operation time");
        pt.start();
        for (int i = 0; i < num; ++i) {
            Block block = GalileoConnector.generateData();
            client.store(server, block);
        }
        pt.stopAndPrint();

        client.disconnect();
    }
}