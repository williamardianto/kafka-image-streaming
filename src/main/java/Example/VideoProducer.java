package Example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bytedeco.javacv.CanvasFrame;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.OpenCVFrameConverter;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_core.Size;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.bytedeco.opencv.global.opencv_imgproc.resize;

public class VideoProducer {
    static Logger log = LoggerFactory.getLogger(VideoProducer.class);

    static String KAFKA_BROKERS = "localhost:9093";

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

        String homePath = System.getProperty("user.home");
        try (FFmpegFrameGrabber frameGrabber = new FFmpegFrameGrabber(homePath + "/Desktop/timer.mp4")) {
            frameGrabber.start();

            int width = frameGrabber.getImageWidth() / 2;
            int height = frameGrabber.getImageHeight() / 2;
            long length = frameGrabber.getLengthInTime();

            Frame frame;

            CanvasFrame canvas = new CanvasFrame("Producer Canvas");
            canvas.setCanvasSize(width, height);

//            log.info("framerate = " + frameGrabber.getFrameRate());
//            System.out.println("length = "+ length);

            while (canvas.isVisible() && (frame = frameGrabber.grab()) != null) {
//                canvas.showImage(frame);

                Mat frameMat = converter.convert(frame); //1920 x 1080
                resize(frameMat, frameMat, new Size(width, height)); // 480x270

                canvas.showImage(converter.convert(frameMat));

                byte[] frameBytes = MatToBytes(frameMat);

//                Mat newFrame = BytesToMat(frameBytes);
//                imshow("image",newFrame);
//                waitKey(0);

                ProducerRecord<String, byte[]> record = new ProducerRecord<>("image", frameBytes);
                producer.send(record, new ProducerCallback());

//                producer.flush();

                Thread.sleep(100);
            }

            frameGrabber.stop();
            canvas.dispose();

        } catch (Exception ex) {
            ex.printStackTrace();
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
                        "Timestamp: " + recordMetadata.timestamp());
            } else
                log.error(e.getMessage());
        }
    }
}
