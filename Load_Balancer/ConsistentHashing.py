from sortedcontainers import SortedSet


class OrderedSet:
    def __init__(self):
        self.ordered_set = SortedSet()

    def insert(self, value):
        self.ordered_set.add(value)

    def delete(self, value):
        self.ordered_set.discard(value)

    def upper_bound(self, value):
        upper_bound_index = self.ordered_set.bisect_right(value)
        if upper_bound_index < len(self.ordered_set):
            return self.ordered_set[upper_bound_index]
        else:
            return self.ordered_set[0]


class ConsistentHashing:
    def __init__(self, N: int, M: int, K: int):
        self.N = N  # Number of server containers
        self.M = M  # Total number of slots in the consistent hash map
        self.K = K  # Number of virtual servers for each server container

        # Initialize data structures
        self.serverIndices = OrderedSet()
        self.serverNameToIndex: dict[str, list[int]] = {}
        self.serverList = ['' for _ in range(M)]

    def calcServerHash(self, i: int, j: int):
        hash = i*i+j*j+2*j+25
        return hash % self.M

    def add_server(self, server_number: int, server_name: str):
        replicas = []

        for j in range(self.K):
            hash = self.calcServerHash(server_number, j+1)
            index = hash
            count = 1
            # quadratic probing
            while self.serverList[index] != '':
                index = (hash+count*count) % self.M
                count = count + 1

            replicas.append(index)
            self.serverList[index] = server_name
            self.serverIndices.insert(index)

        self.serverNameToIndex[server_name] = replicas

    def calcRequestHash(self, id: int):
        hash = id*id+2*id+17
        return hash % self.M

    def remove_server(self, server_number: int, server_name: str):
        if self.serverNameToIndex.get(server_name) is None:
            return
        for ind in self.serverNameToIndex[server_name]:
            self.serverList[ind] = ''
            self.serverIndices.delete(ind)
        self.serverNameToIndex.pop(server_name)

    def allocate(self, reqID: int):
        reqIndex = self.calcRequestHash(reqID)
        serverIndex = self.serverIndices.upper_bound(reqIndex)
        return self.serverList[serverIndex]
