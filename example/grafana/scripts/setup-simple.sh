#!/bin/bash

set -e

echo "Waiting for MongoDB to start..."
sleep 10

echo "Initializing MongoDB Replica Set..."
mongosh --host mongodb:27017 --eval "
try {
  rs.status()
  print('Replica set already initialized')
} catch (e) {
  rs.initiate(
    {
      _id: 'rs0',
      members: [
        { _id: 0, host: 'mongodb:27017' }
      ]
    }
  )
  print('Replica set initialized')
}
"

echo "Waiting for replica set to become primary..."
sleep 10

echo "MongoDB setup completed!"

