package Example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacv.CanvasFrame;
import org.bytedeco.javacv.OpenCVFrameConverter;
import org.bytedeco.opencv.opencv_core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.opencv.core.CvType.CV_8UC;

public class ImageConsumer {
    static String KAFKA_BROKERS = "localhost:9093";
    static Logger log = LoggerFactory.getLogger(ImageConsumer.class);

    static String groupId = "smart-retail";
    static String topic = "image2";

    static int width = 1920 / 2;
    static int height = 1080 / 2;

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "2097152");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(topic));

        CanvasFrame canvas = new CanvasFrame("Consumer Canvas");
        canvas.setCanvasSize(width, height);

        OpenCVFrameConverter.ToMat converter = new OpenCVFrameConverter.ToMat();

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

            records.forEach(record -> {

                log.info("Record: \n" +
                        "Topic:" + record.topic() + "\n" +
                        "Partition:" + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        "Timestamp: " + record.timestamp()
                );

                Mat mat = BytesToMat(record.value());
                canvas.showImage(converter.convert(mat));
            });
        }
    }

    static Mat BytesToMat(byte[] b) {
        return new Mat(height, width, CV_8UC(3), new BytePointer(b));
    }
}
