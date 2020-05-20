package Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bytedeco.javacv.*;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

import static org.bytedeco.opencv.global.opencv_imgproc.resize;

public class WebcamProducer {
    static Logger log = LoggerFactory.getLogger(WebcamProducer.class);

    static String KAFKA_BROKERS = "10.10.10.23:9093";
    static String TOPIC = "image1";

//    static int WIDTH = 960;
//    static int HEIGHT = 540;

    static int WIDTH = 640;
    static int HEIGHT = 480;

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "2097152");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

        OpenCVFrameConverter.ToMat converter = new OpenCVFrameConverter.ToMat();

        try (OpenCVFrameGrabber grabber = new OpenCVFrameGrabber(0)) {
//            grabber.setImageWidth(WIDTH);
//            grabber.setImageHeight(HEIGHT);

            grabber.start();

            Frame frame;

            CanvasFrame canvasFrame = new CanvasFrame("Cam");
//            canvasFrame.setCanvasSize(WIDTH, HEIGHT);

            log.info("frame rate: " + grabber.getFrameRate() + "\n" +
                    "frame width: " + grabber.getImageWidth() + "\n" +
                    "frame height: " + grabber.getImageHeight() + "\n"
            );

            while (canvasFrame.isVisible() && (frame = grabber.grab()) != null) {
                Mat frameMat = converter.convert(frame);
                resize(frameMat, frameMat, new Size(WIDTH, HEIGHT));

//                canvasFrame.showImage(converter.convert(frameMat));

                byte[] frameBytes = MatToBytes(frameMat);

                log.info(String.valueOf(frameBytes.length));

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, frameBytes);
                producer.send(record, new ProducerCallback());

//                log.info(String.valueOf(frameMat.cols()));
                Thread.sleep(1000);
            }

        } catch (FrameGrabber.Exception | InterruptedException e) {
            log.error(e.getMessage());
        }
    }


    private static byte[] MatToBytes(Mat mat) {
        byte[] b = new byte[mat.channels() * mat.cols() * mat.rows()];
        mat.data().get(b);
        return b;
    }

    static class ProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e == null) {
                log.info("Successfully received the details as: \n" +
                        "Topic:" + recordMetadata.topic() + "\n" +
                        "Partition:" + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp() + "\n" +
                        "Date: " + new Date(recordMetadata.timestamp()) + "\n" +
                        "Bytes size: " + recordMetadata.serializedValueSize()
                );
            } else
                log.error(e.getMessage());
        }
    }
}
