package kafkastreams.example.KStream;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class Consumer {
    private final static String API_GATEWAY_URL_BASE = "https://jgr795jq6l.execute-api.ap-northeast-2.amazonaws.com/default/triggerRequest?start=";
    private final static Logger log = LoggerFactory.getLogger(Consumer.class);
    private final static String BOOTSTRAP_SERVERS = "43.203.141.74:9092";  /* change ip */
    private final static String GROUP_ID = "kstream-application";  /* this can be anything you want */
    private final static String TOPIC_NAME = "stream_filter_sink";

    private final static String MONGODB_URI = "mongodb://ec2-43-203-141-74.ap-northeast-2.compute.amazonaws.com:27017";
    private final static String DATABASE_NAME = "youtube_hightlight";
    private final static String COLLECTION_NAME = "youtube_hightlight_title";



    public Consumer() {}
    public static void main(String[] args) {
        new Consumer().run();
    }

    public void run() {
        log.info("Starting Kafka consumer...");
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // Safe close
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping application...");
            log.info("Closing consumer...");
            consumer.close();
            log.info("Consumer closed. Done!");
        }));

        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        log.info("Consumer is ready");
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key (Timestamp): " + record.key() + ", Value (Count): " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                    // API Gateway 로 전송
                    sendKeyToApiGateway(record.value());
                    // Save to MongoDB
                    saveToMongoDB(record.value());
                }
            }
        } catch (Exception e) {
            log.error("Error in consuming records", e);
        } finally {
            consumer.close();
            log.info("Consumer closed");
        }
    }

    public KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(properties);
    }

    private void sendKeyToApiGateway(String key) {
        try {

            // key: "Threshold exceeded from 2024-05-28 04:31:09 to 2024-05-28 04:31:28"
            String[] parts = key.split(" to ");
            String startFull = parts[0].trim();
            String endFull = parts[1].trim();
            String urlStr = API_GATEWAY_URL_BASE + startFull + "&end=" + endFull;
            urlStr = urlStr.replace(" ", "%20");
            log.info(urlStr);
            URL url = new URL(urlStr);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type", "application/json; utf-8");
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);

            String jsonInputString = "{\"key\": \"" + key + "\"}";

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            log.info("Response Code from API Gateway: " + responseCode);

            connection.disconnect();
        } catch (Exception e) {
            log.error("Error sending key to API Gateway", e);
        }
    }

    private void saveToMongoDB(String value) {
        try {

            MongoClient mongoClient = MongoClients.create(MONGODB_URI);
            MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

            String[] parts = value.split(" to ");
            String startFull = parts[0].trim();
            String endFull = parts[1].trim();

            Document doc = new Document("start", startFull)
                    .append("end", endFull);
            collection.insertOne(doc);
            log.info("Successfully inserted document into MongoDB");
        } catch (Exception e) {
            log.error("Error inserting document into MongoDB", e);
        }
    }

}