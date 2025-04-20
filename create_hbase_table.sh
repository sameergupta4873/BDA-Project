#!/bin/bash
echo "Creating HBase table..."

echo "create 'sensordata', 'metrics'" | hbase shell
