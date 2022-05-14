# Fraud Detection Pipeline



## Running The Application 🔌


### Kafka Setup
       
* Download the Kafka setup from below link

       https://drive.google.com/file/d/1FECUBb853MUVoWnMo--4bU_Tbo7Zv9kg/view?usp=sharing

        
* To Run Kafka Server locally, issue the below commands (credit: https://kafka.apache.org/quickstart )

        tar -xzf kafka_2.12-3.1.1.tgz
        cd kafka_2.12-3.1.1
        bin/zookeeper-server-start.sh config/zookeeper.properties
        
* Open other terminal, run inside kafka_2.12-3.1.1 directory

        bin/kafka-server-start.sh config/server.properties
        
        
* Open another terminal, run the below command to create two new topics

        kafka-topics --create --partitions 1 --replication-factor 1 --topic transactions --bootstrap-server localhost:9092
        kafka-topics --create --partitions 1 --replication-factor 1 --topic predictions --bootstrap-server localhost:9092
        
 
 * Download the repository files (project) from the download section or clone this project by typing in the bash the following command:

       git clone https://github.com/abhranja-sudo/Fraud-Detection-Pipeline.git
       
* Download the data csv file from the below link:

       https://drive.google.com/file/d/1dk7XPEG972WI_17brOxARNnW4TbvRoTD/view?usp=sharing 
     
* unzip the file and place the data folder under /Fraud-Detection-Pipeline/data
     
 * To run the application, go inside project directory

        cd Fraud-Detection-Pipeline
