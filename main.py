from kv_store import DistributedKeyValueStore
from replication import SingleLeaderReplication, LeaderlessReplication

def main():
    node_id = 1
    peers = [2, 3]

    # Choose the replication algorithm
    #replication_algorithm = SingleLeaderReplication(node_id, peers)
    replication_algorithm = LeaderlessReplication(node_id, peers)

    kv_store = DistributedKeyValueStore(node_id, peers, replication_algorithm)

    # Example usage
    kv_store.put("key1", "value1")
    kv_store.put("key1", "value1")
    print(kv_store.get("key1"))
    kv_store.delete("key1")
    print(kv_store.get("key1"))

if __name__ == "__main__":
    main()
