# python3 -m Pyro4.naming

import Pyro4
import heapq

@Pyro4.expose
class Testcase:
    def __init__(self):
        self.__graphs = {}

    def add_graph(self,graph_identifier, n):
        self.__graphs[graph_identifier] = [n,[]]
        print(self.__graphs)
        return

    def add_edge(self,graph_identifier, u,v,w):
        self.__graphs[graph_identifier][1].append([u,v,w])
        self.__graphs[graph_identifier][1].append([v,u,w])
        # print(self.__graphs)

    def get_mst(self,graph_identifier):
        ans = 0
        n,graph = self.__graphs[graph_identifier]
        visited = [0 for i in range(n+1)]
        adj_list = {}
        for i in graph:
            if i[0] not in adj_list:
                adj_list[i[0]] = []
            if i[1] not in adj_list:
                adj_list[i[1]] = []
            adj_list[i[0]].append([i[2],i[1]])
            adj_list[i[1]].append([i[2],i[0]])
        
        li = []
        heapq.heapify(li)
        for i in adj_list[1]:
            heapq.heappush(li,i)
        visited[1] = 1
        while len(li) > 0:
            flag = 0
            a = heapq.heappop(li)
            while visited[a[1]] != 0:
                if len(li) == 0:
                    flag = 1
                    break
                a = heapq.heappop(li)
            if flag == 1:
                break
            visited[a[1]] = 1
            ans += a[0]
            for i in adj_list[a[1]]:
                heapq.heappush(li,i)
        print('Done mst')
        flag = 0
        for i in range(1,len(visited)):
            if visited[i] == 0:
                flag = 1
        if flag == 1:
            return -1
        return ans
    

obj = Testcase()
Pyro4.Daemon.serveSimple({obj : 'Graph'},host='localhost', port=9090)   

