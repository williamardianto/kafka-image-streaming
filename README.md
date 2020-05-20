mvn clean install -DskipTests -Dexec.mainClass=Producer.WebcamProducer -Djavacpp.platform=windows-x86_64

mvn clean install -DskipTests -Dexec.mainClass=Producer.WebcamProducer -Djavacpp.platform=linux-armhf

mvn exec:java -Dexec.mainClass=Producer.WebcamProducer



## PI SETUP
sudo v4l2-ctl --list-devices

sudo chmod 777 /dev/video0