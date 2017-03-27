### Build

    mvn clean install

### Run     

Export in environment variables

    export BROKERS="192.168.99.100:9092"
    export ZK="192.168.99.100:2181"
    export SCHEMA_REGISTRY="http://192.168.99.100:8081"

Run apps:
    
    # Create topic `quotes` with 3 partitions - keep sending Pinky & Brain quotes as String
    java -cp target/kafka-training-jar-with-dependencies.jar com.landoop.produce.ContinuousTextTextProducer
    # Read from topic `quotes` into KTable with 3 partitions - do word count into `quotes-wordcount`
    java -cp target/kafka-training-jar-with-dependencies.jar com.landoop.kstreams.ContinuousWordCount

Join-Streams
    
    # Create topic `clicks` and `searches` with 3 x partitions - keep sending 10% searches 90% clicks
    java -cp target/kafka-training-jar-with-dependencies.jar com.landoop.produce.ContinuousSearchClickProducer
    # Join `clicks` with `searches` into `search-click-join` 
    java -cp target/kafka-training-jar-with-dependencies.jar com.landoop.kstreams.ContinuousSearchClickJoinStreams
    
### Enable JMX
    
To enable JMX on a KStream application pass in the following parameters.
    
    -Dcom.sun.management.jmxremote
    -Dcom.sun.management.jmxremote.port=9010
    -Dcom.sun.management.jmxremote.local.only=false
    -Dcom.sun.management.jmxremote.authenticate=false
    -Dcom.sun.management.jmxremote.ssl=false

i.e. 

    
```cmd
java -Dcom.sun.management.jmxremote \
     -Dcom.sun.management.jmxremote.port=9010 \
     -Dcom.sun.management.jmxremote.local.only=false \
     -Dcom.sun.management.jmxremote.authenticate=false \
     -Dcom.sun.management.jmxremote.ssl=false \
     -cp target/kafka-training-jar-with-dependencies.jar com.landoop.kstreams.ContinuousSearchClickJoinStreams
```