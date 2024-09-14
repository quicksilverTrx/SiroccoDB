class DistributedKeyValueStore:
    def __init__(self, node_id, peers, replication_algorithm):
        self.node_id = node_id
        self.peers = peers
        self.replication_algorithm = replication_algorithm
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def put(self, key, value):
        self.data[key] = value
        self.replication_algorithm.replicate(key, value)

    def delete(self, key):
        if key in self.data:
            del self.data[key]
            self.replication_algorithm.replicate(key, None)
