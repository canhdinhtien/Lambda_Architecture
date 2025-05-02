

KAFKA_BROKERS_INTERNAL="kafka:9092" 


KAFKA_TOPIC_SMARTGRID="smart_grid_data"  
KAFKA_TOPIC_STOCK="stock_market_data"    


PARTITIONS=3
REPLICATION_FACTOR=1 

MAX_WAIT_SECONDS=120
WAIT_INTERVAL=5
ELAPSED_TIME=0

echo "Waiting up to $MAX_WAIT_SECONDS seconds for Kafka broker to be ready at $KAFKA_BROKERS_INTERNAL..."


while [ $ELAPSED_TIME -lt $MAX_WAIT_SECONDS ]; do

    if /usr/bin/kafka-topics --bootstrap-server $KAFKA_BROKERS_INTERNAL --list > /dev/null 2>&1; then
        echo "Kafka broker seems ready."
        break
    fi
    echo "Still waiting for Kafka..."
    sleep $WAIT_INTERVAL
    ELAPSED_TIME=$((ELAPSED_TIME + WAIT_INTERVAL))
done

if [ $ELAPSED_TIME -ge $MAX_WAIT_SECONDS ]; then
    echo "ERROR: Kafka broker did not become ready within $MAX_WAIT_SECONDS seconds."
    exit 1
fi

echo "Attempting to create Kafka topics if they do not exist..."
echo "" 


echo "Creating topic '$KAFKA_TOPIC_SMARTGRID'..."
/usr/bin/kafka-topics --create \
    --topic "$KAFKA_TOPIC_SMARTGRID" \
    --bootstrap-server "$KAFKA_BROKERS_INTERNAL" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --if-not-exists


if [ $? -eq 0 ]; then
    echo "Kafka topic '$KAFKA_TOPIC_SMARTGRID' created successfully or already exists."
else

    echo "WARNING: Command to create Kafka topic '$KAFKA_TOPIC_SMARTGRID' failed. It might already exist with different settings."
fi
echo "" 


echo "Creating topic '$KAFKA_TOPIC_STOCK'..."
/usr/bin/kafka-topics --create \
    --topic "$KAFKA_TOPIC_STOCK" \
    --bootstrap-server "$KAFKA_BROKERS_INTERNAL" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --if-not-exists


if [ $? -eq 0 ]; then
    echo "Kafka topic '$KAFKA_TOPIC_STOCK' created successfully or already exists."
else
    echo "WARNING: Command to create Kafka topic '$KAFKA_TOPIC_STOCK' failed. It might already exist with different settings."
fi
echo "" 


echo "Verifying topics currently present..."
/usr/bin/kafka-topics --list --bootstrap-server "$KAFKA_BROKERS_INTERNAL"
echo ""

echo "Kafka setup script finished."
exit 0