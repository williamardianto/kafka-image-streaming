package Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.OpenCVFrameConverter;
import org.bytedeco.opencv.opencv_core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

import static org.opencv.core.CvType.CV_8UC;

public class WebcamConsumer {
    static String KAFKA_BROKERS = "localhost:9093";
    static Logger log = LoggerFactory.getLogger(WebcamConsumer.class);

    static String GROUPID = "smart-retail";
    static String TOPIC = "image3";

    static int WIDTH = 960;
    static int HEIGHT = 540;

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "2097152");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(TOPIC));

//        CanvasFrame canvas = new CanvasFrame("Consumer Canvas");
//        canvas.setCanvasSize(WIDTH, HEIGHT);

        OpenCVFrameConverter.ToMat converter = new OpenCVFrameConverter.ToMat();

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

            records.forEach(record -> {

                log.info("Record: \n" +
                        "Topic:" + record.topic() + "\n" +
                        "Partition:" + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        "Timestamp: " + record.timestamp() + "\n" +
                        "Date: " + new Date(record.timestamp())+ "\n" +
                        "Bytes size: "+ record.value().length
                );

                Mat mat = BytesToMat(record.value());
                Frame frame = converter.convert(mat);
//                canvas.showImage(frame);
            });
        }
    }

    static Mat BytesToMat(byte[] b) {
        Mat mat ;
            try{
            mat =  new Mat(HEIGHT, WIDTH, CV_8UC(3), new BytePointer(b));
        }catch (Exception ex){
            mat = null;
            log.error(ex.getMessage());
        }
        return mat;
    }
}
