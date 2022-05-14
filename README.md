# Fraud Detection Pipeline


## Requirements 🔧
* Spark 3.2
* Python 3.9(don't run on 3.10 as confluent kafka client is not supported)


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
        
 
 * Open the project folder that has been submitted or can be cloned by the link below

       git clone https://github.com/abhranja-sudo/Fraud-Detection-Pipeline.git
       
* Download the data csv file from the below link:

       https://drive.google.com/file/d/1dk7XPEG972WI_17brOxARNnW4TbvRoTD/view?usp=sharing 
     
* unzip the file and place the extracted package  under /Fraud-Detection-Pipeline/data (data folder may need to be created if project is downloaded from git)
     
 
        
### Starting Pipeline

* Create a new Conda Environment and run the below command

       cd Fraud-Detection-Pipeline
       pip install -r requirements.txt 


* Go inside the below directory

       cd FraudDetectionService/InitializeModel
       
 
* Run the below command to create a Initailize ML model

        python create_model.py
        
* Wait until the model is created(can take upto 1-2 min)

* To Run the Fraud Detection Engine, From the base proect directory

       cd FraudDetectionService
       python fraud_detection_engine.py  


* Run the below command in seperate terminal from the base project directory to run the consumer

       cd TransactionsGeneratorService
       python consumer.py


* Run the below command in seperate terminal from the base directory to start app that will start producing messages

       cd TransactionsGeneratorService
       python app.py
