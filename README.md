### Build

    mvn clean install

### Run     

Export in environment variables

    export BROKERS="192.168.99.100:9092"
    export ZK="192.168.99.100:2181"
    export SCHEMA_REGISTRY="http://192.168.99.100:8081"

Run apps:
    
    # Create topic `quotes` with 3 partitions - keep sending <String,String> messages
    java -cp target/kafka-training-jar-with-dependencies.jar com.landoop.produce.ContinuousTextTextProducer
    # Read from topic `quotes` into KTable with 3 partitions - keep sending <String,String> messages
    java -cp target/kafka-training-jar-with-dependencies.jar com.landoop.kstreams.ContinuousWordCount
    