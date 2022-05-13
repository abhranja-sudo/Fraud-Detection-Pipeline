# Fraud Detection Pipeline



## Running The Application 🔌


To test the application run the following commands.

* Download the repository files (project) from the download section or clone this project by typing in the bash the following command:

       git clone https://github.com/abhranja-sudo/Fraud-Detection-Pipeline.git
       

* To run the application, go inside project directory

        cd Fraud-Detection-Pipeline
        
* To Run Kafka Server locally, issue the below commands (credit: https://kafka.apache.org/quickstart )

        tar -xzf kafka_2.13-3.1.1-SNAPSHOT.tgz
        cd kafka_2.13-3.1.1-SNAPSHOT
        bin/zookeeper-server-start.sh config/zookeeper.properties
        
* Open other terminal, run
        bin/kafka-server-start.sh config/server.properties
        
* Create two topics with the below commands

        kafka-topics --create --partitions 1 --replication-factor 1 --topic transactions --bootstrap-server localhost:9092
        kafka-topics --create --partitions 1 --replication-factor 1 --topic predictions --bootstrap-server localhost:9092
