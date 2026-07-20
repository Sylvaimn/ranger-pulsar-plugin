#!/bin/bash

set -e

echo "=== Ranger Pulsar Plugin Integration Test ==="

PLUGIN_JAR="target/ranger-pulsar-plugin-1.0.0-SNAPSHOT.jar"

if [ ! -f "$PLUGIN_JAR" ]; then
    echo "Building plugin..."
    /opt/maven/apache-maven-3.8.6/bin/mvn package -DskipTests
fi

echo "Starting Pulsar standalone with Ranger plugin..."

docker run -d \
    --name pulsar-test \
    -p 6650:6650 \
    -p 8080:8080 \
    -v "$(pwd)/$PLUGIN_JAR:/pulsar/lib/ranger-pulsar-plugin.jar" \
    -v "$(pwd)/src/main/resources:/pulsar/conf/ranger" \
    -e PULSAR_MEM="-Xms512m -Xmx1g" \
    apachepulsar/pulsar:3.1.0 \
    pulsar standalone --advertised-address localhost

echo "Waiting for Pulsar to start..."
sleep 30

echo "Checking Pulsar status..."
curl -s http://localhost:8080/admin/v2/clusters | python3 -m json.tool

echo "Creating test topic..."
curl -X PUT http://localhost:8080/admin/v2/persistent/public/default/test-topic

echo "=== Test Complete ==="
echo "Pulsar is running with Ranger plugin loaded."
echo "You can test with:"
echo "  - Producer: bin/pulsar-client produce -t persistent://public/default/test-topic"
echo "  - Consumer: bin/pulsar-client consume -t persistent://public/default/test-topic"
