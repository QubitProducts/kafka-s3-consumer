kafka-s3-consumer
=================

Store batched Kafka messages in S3.

Build

  mvn package

Run

  java -jar kafka-s3-consumer-2.0.jar <props>

or (for EC2 instances)

   java -Djava.io.tmpdir=/mnt -jar kafka-s3-consumer-2.0.jar 
