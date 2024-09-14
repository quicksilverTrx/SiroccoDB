import time

class SingleLeaderReplication:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers
        self.leader = self.elect_leader()

    def elect_leader(self):
        # Simulate leader election (e.g., the node with the smallest ID)
        return min(self.peers + [self.node_id])

    def replicate(self, key, value):
        if self.node_id == self.leader:
            for peer in self.peers:
                # Simulate replication to other nodes
                print(f"Replicating {key}:{value} to {peer}")
        else:
            print(f"Node {self.node_id} is not the leader. Ignoring replication request.")

class LeaderlessReplication:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers

    def replicate(self, key, value):
        for peer in self.peers:
            # Simulate replication to other nodes
            print(f"Replicating {key}:{value} to {peer}")
