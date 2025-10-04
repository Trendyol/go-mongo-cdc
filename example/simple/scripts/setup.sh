#!/bin/bash
echo "Waiting for MongoDB services to start..."
sleep 15

# Initialize Config Server Replica Set
echo "Initializing Config Server Replica Set..."
mongosh --host config-server:27019 <<EOF
rs.initiate(
  {
    _id: "configReplSet",
    configsvr: true,
    members: [
      { _id: 0, host: "config-server:27019" }
    ]
  }
)
EOF

echo "Waiting for Config Server replication to initialize..."
sleep 15

# Initialize Shard 1 Replica Set
echo "Initializing Shard 1 Replica Set..."
mongosh --host shard1-server:27018 <<EOF
rs.initiate(
  {
    _id: "shard1ReplSet",
    members: [
      { _id: 0, host: "shard1-server:27018" }
    ]
  }
)
EOF

# Initialize Shard 2 Replica Set
echo "Initializing Shard 2 Replica Set..."
mongosh --host shard2-server:27018 <<EOF
rs.initiate(
  {
    _id: "shard2ReplSet",
    members: [
      { _id: 0, host: "shard2-server:27018" }
    ]
  }
)
EOF

echo "Waiting for Shard replication to initialize..."
sleep 30

# Make sure mongos is ready
echo "Checking if mongos is ready..."
until mongosh --host mongos:27017 --eval "db.adminCommand('ping')" >/dev/null 2>&1; do
  echo "Waiting for mongos to be ready..."
  sleep 5
done

# Add shards to the cluster
echo "Adding shards to the cluster..."
mongosh --host mongos:27017 <<EOF
sh.addShard("shard1ReplSet/shard1-server:27018")
sh.addShard("shard2ReplSet/shard2-server:27018")
EOF

echo "MongoDB Sharded Cluster setup completed!"