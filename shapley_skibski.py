import math
from itertools import combinations, permutations
import v_calculator
import pandas as pd
import os
import random
import time
import json
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

class Shapley_Calculation:

    def __init__(self, path) -> None:
        
        self.raw_data_path = path + "raw/"
        self.log_path = path + "log/"
        self.temp_path = path + "temp/"

        self.graph = self.gen_graph(self.raw_data_path)
        # print(self.graph)
        
        self.ff_list = self.gen_ff_list(self.raw_data_path)

        self.v_calc = v_calculator.V_Calculator(self.graph, self.raw_data_path, self.temp_path)

        start_time = time.time()
        # start_time = time.process_time()

        # based on Skibski's paper
        self.gen_idx()
        self.gen_dfs_ind_subgraphs()
        shapley_df = self.gen_myerson_table()

        end_time = time.time()
        # end_time = time.process_time()
        print("elapsed time:", end_time-start_time)
        with open(self.log_path + "skibski_shapley_elapsed_time.json", "w") as f:
            json.dump({"elapsed_time": end_time-start_time}, f)
        
        print(shapley_df)
        shapley_df.to_csv(self.log_path + "skibski_shapley.csv", index=False, header=True)



    def gen_ff_list(self, path):
        ### read all files in path that ends with starts with sailing_data
        file_list = []
        for filename in os.listdir(path):
            if filename.startswith("sailing_data") and filename.endswith(".csv"):
                file_list.append(filename)
        
        ### get ff_list
        ff_list = []
        for filename in file_list:
            sailling_data = pd.read_csv(path + filename)
            ff_list += list(sailling_data["ff"].unique())
        
        # remove duplicates from ff_list
        ff_list = list(dict.fromkeys(ff_list))
        print("ff_list", ff_list)

        return ff_list


    
    def gen_graph(self, path):

        ### read all files in path that ends with starts with sailing_data
        file_list = []
        for filename in os.listdir(path):
            if filename.startswith("sailing_data") and filename.endswith(".csv"):
                file_list.append(filename)
                                                                                                                                                                                                                                                                            
        ### get num of forwarders
        num_forwarders = 0
        for filename in file_list:
            sailling_data = pd.read_csv(path + filename)
            ff_list = sailling_data["ff"].unique()
            num_forwarders = max(max(ff_list) + 1, num_forwarders)
            
        graph = [[] for i in range(num_forwarders)]

        for filename in file_list:
            sailling_data = pd.read_csv(path + filename)
            ff_list = sailling_data["ff"].unique()
            for i in range(len(ff_list)):
                for j in range(len(ff_list)):
                    if i==j:
                        continue
                    v = ff_list[i]
                    w = ff_list[j]
                    if w not in graph[v]:
                        graph[v].append(w)
                    if v not in graph[w]:
                        graph[w].append(v)
        
        # print("graph", graph)
        return graph



#################################################################################
#### based on Skibski's paper
#################################################################################

    def gen_idx(self):
        self.idx_map = []
        for i,nodes in enumerate(self.graph):
            mapping = {}
            for j, node in enumerate(nodes):
                mapping[node] = j
            self.idx_map.append(mapping)
        return self.idx_map
    

    def expand_subgraph(self, path, subgraph, forbidden_, idx):
        forbidden = forbidden_.copy()

        v = path[-1]
        for i in range(idx, len(self.graph[v])):
            u = self.graph[v][i]
            # print("u:",u)
            if not(u in subgraph or u in forbidden):
                self.expand_subgraph(path+[u], subgraph+[u], forbidden, 0)
                forbidden = forbidden + [u]
        
        path = path[:-1]
        if len(path)>0:
            w = path[-1]
            id = self.idx_map[w][v] + 1
            self.expand_subgraph(path, subgraph, forbidden, id)
        else:
            self.dfs_ind_subgraphs.append(subgraph)



    def gen_dfs_ind_subgraphs(self):
        self.dfs_ind_subgraphs = []
        forbids=[] 
        for i in self.ff_list:
            self.expand_subgraph([i], [i], forbids, 0)
            forbids = forbids + [i]       
    


    def dfs_myerson_wrapper(self, args):
        return self.dfs_myerson(*args)
    


    def dfs_myerson(self, path, subgraph, forbidden_, idx, neighbours_):

        # path, subgraph, forbidden_, idx, neighbours_ = args
        forbidden = forbidden_.copy()
        neighbours = neighbours_.copy()

        v = path[-1]
        for i in range(idx, len(self.graph[v])):
            u = self.graph[v][i]
            # print("u:",u)
            if not(u in subgraph or u in forbidden):
                self.dfs_myerson(path+[u], subgraph+[u], forbidden, 0, neighbours)
                forbidden = forbidden + [u]
                if not(u in neighbours):
                    neighbours = neighbours + [u]
            elif (u in forbidden) and not(u in neighbours):
                neighbours = neighbours + [u]
        
        path = path[:-1]
        if len(path)>0:
            w = path[-1]
            id = self.idx_map[w][v] + 1
            self.dfs_myerson(path, subgraph, forbidden, id, neighbours)
        else:
            # print(subgraph)
            self.dfs_ind_subgraphs.append(subgraph)
            c = len(subgraph)
            n = len(neighbours)
            f = self.v_calc.solve_specific_coalition(subgraph)

            # print("subgraph:", subgraph)
            # print("neighbours:", neighbours)
            for x in subgraph:
                self.myerson_dict[x] += math.factorial(c-1) * math.factorial(n) / math.factorial(n+c) * f
            for x in neighbours:
                self.myerson_dict[x] -= math.factorial(c) * math.factorial(n-1) / math.factorial(n+c) * f 
        return self.myerson_dict
    


    def gen_myerson_table(self):
        self.myerson_dict = {}
        for v in self.ff_list:
            self.myerson_dict[v] = 0

        args_list = []
        forbids=[] 
        for i in self.ff_list:
            args = ([i], [i], forbids, 0, [])
            args_list.append(args)
            # self.dfs_myerson([i], [i], forbids, 0, [])
            forbids = forbids + [i]
        
        
        for args in args_list:
            self.dfs_myerson_wrapper(args)
        
        # pool = mp.Pool(mp.cpu_count())
        # pool.map(self.dfs_myerson_wrapper, args_list)
        # pool.close()
        # pool.join()


        k = list(self.myerson_dict.keys())
        v = list(self.myerson_dict.values())
        df = pd.DataFrame({"forwarder":k, "myerson":v})
        return df



if __name__ == "__main__":

    ### python shapley_skibski.py data5 ID_015

    import sys
    script_name = sys.argv[0]
    arg1 = sys.argv[1]  # First argument
    arg2 = sys.argv[2]  # Second argument

    # path = "data5/ID_015/" 
    # python shapley_skibski.py data5 ID_015
    path = arg1 + "/" + arg2 + "/"

    shap = Shapley_Calculation(path)