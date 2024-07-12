import math
from itertools import combinations, permutations
import v_calculator
import pandas as pd
import os
import random
import time
import json
from decimal import Decimal
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

class Shapley_Calculation:

    ### this uses arbitrary precision to compute shapley values

    def __init__(self, path, sampling_rate) -> None:
        
        self.raw_data_path = path + "raw/"
        self.res_path = path + "log/"
        self.temp_path = path + "temp/"

        self.graph = self.gen_graph(self.raw_data_path)
        print(self.graph)
        
        self.ff_list = self.gen_ff_list(self.raw_data_path)

        self.sampling_rate = sampling_rate

        self.v_calc = v_calculator.V_Calculator(self.graph, self.raw_data_path, self.temp_path)



        ### compute shapley values

        start_time = time.time()
        # start_time = time.process_time()
        # self.shapley_df = self.compute_shapley_all_ff(self.graph, self.v_calc, self.sampling_rate)
        self.shapley_df = self.compute_shapley_all_ff_parallel(self.graph, self.v_calc, self.sampling_rate)
        end_time = time.time()
        # end_time = time.process_time()
        # print("elapsed time:", end_time-start_time)
        with open(self.res_path + "fast_shapley_elapsed_time.json", "w") as f:
            json.dump({"elapsed_time": end_time-start_time}, f)
    
        ### compute original ff cost
        for ff in self.ff_list:
            print("ff", ff, "cost", self.v_calc.solve_for_ff(ff))

        # remove rows from shapley_df where shapley_value is 0
        self.shapley_df = self.shapley_df[self.shapley_df["shapley_value"] > 0]
        print(self.shapley_df)
        # export shapley values to csv
        self.shapley_df.to_csv(self.res_path + "fast_shapley_values.csv", index=False)
        

    def compute_shapley_single(self, args):
        graph, v_calc, i, sampling_rate = args
        phi_1_value, phi_1_weight, phi_1_sample_count, phi_1_total_count = self.compute_phi_1(graph, v_calc, i)
        phi_2_value, phi_2_weight, phi_2_sample_count, phi_2_total_count = self.compute_phi_2(graph, v_calc, i, sampling_rate)            
        shapley_value = float((phi_1_value + phi_2_value) / (phi_1_weight + phi_2_weight))
        return shapley_value

    def compute_shapley_all_ff_parallel(self, graph, v_calc, sampling_rate):
        ### compute shapley value for all forwarders

        pool = mp.Pool()

        num_forwarders = len(graph)
        shapley_values = [0 for i in range(num_forwarders)]
        
        arg_list = []
        for i in range(num_forwarders):
            args = (graph, v_calc, i, sampling_rate)
            arg_list.append(args)

        shapley_values = pool.map(self.compute_shapley_single, arg_list)

        pool.close()
        pool.join()

        
        # with ProcessPoolExecutor() as executor:
        #     shapley_values = list(executor.map(self.compute_shapley_single, arg_list))

        
        df = pd.DataFrame({"forwarder": [i for i in range(num_forwarders)], "shapley_value": shapley_values})
        # print("count", phi_1_sample_count, phi_1_total_count, phi_2_sample_count, phi_2_total_count)

        ### export shapley values to csv                                                                                                                                                                          
        # df.to_csv(self.res_path + "shapley_values.csv", index=False)
        
        return df

    def compute_shapley_all_ff(self, graph, v_calc, sampling_rate):

        ### compute shapley value for all forwarders

        num_forwarders = len(graph)
        shapley_values = [0 for i in range(num_forwarders)]
        
        for i in range(num_forwarders):
            phi_1_value, phi_1_weight, phi_1_sample_count, phi_1_total_count = self.compute_phi_1(graph, v_calc, i)
            phi_2_value, phi_2_weight, phi_2_sample_count, phi_2_total_count = self.compute_phi_2(graph, v_calc, i, sampling_rate)            
            shapley_values[i] = float((phi_1_value + phi_2_value) / (phi_1_weight + phi_2_weight))
            print("shapley value for forwarder " + str(i) + " is " + str(shapley_values[i]))
        
        df = pd.DataFrame({"forwarder": [i for i in range(num_forwarders)], "shapley_value": shapley_values})
        print("count", phi_1_sample_count, phi_1_total_count, phi_2_sample_count, phi_2_total_count)

        ### export shapley values to csv                                                                                                                                                                          
        # df.to_csv(self.res_path + "shapley_values.csv", index=False)
        
        return df
    


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



    def gen_induced_subgraphs(self, g, vertex_set):
        ### gen induced subgraphs that contain vertex_set
        queue = [vertex_set]
        subgraphs = []
        while len(queue) > 0:
            subgraph = queue.pop(0)
            subgraphs.append(subgraph)
            for v in subgraph:
                for w in g[v]:
                    if w not in subgraph:
                        new_subgraph = subgraph.copy()
                        new_subgraph.add(w)
                        if new_subgraph not in queue:
                            queue.append(new_subgraph)
        return subgraphs



    def compute_phi_1(self, g, v_calc, agent):

        ### case where coalition g does not contain agent's neighbours
        num_forwarders = len(g)
        num_neighbours = len(g[agent])
        value_sum, weight_sum = Decimal(0), Decimal(0)
        total_count, sample_count = 0, 0

        v = v_calc.solve_for_ff(agent)
        # v = 1

        for k in range(num_forwarders-num_neighbours):
            value_sum += Decimal(math.comb(num_forwarders-num_neighbours-1, k)) * Decimal(math.factorial(k)) * Decimal(math.factorial(num_forwarders-k-1)) * Decimal(v)
            weight_sum += Decimal(math.comb(num_forwarders-num_neighbours-1, k)) * Decimal(math.factorial(k)) * Decimal(math.factorial(num_forwarders-k-1))
            sample_count += 1
            total_count += 1
        
  
        return value_sum, weight_sum, sample_count, total_count


    def compute_marginal_contributions(self, num_forwarders, subgraph, g, u):
        subgraph_size = len(subgraph)
        oth_neighbours_count = len(g[u]) - (subgraph_size-1)
        coalition = subgraph.copy()
        coalition.remove(u)
        v = v_calc.calc_marginal_contribution(coalition, u)
        # print("marginal contribution of", u, "to", coalition, "is", v, end=" ")
        for k in range(num_forwarders - subgraph_size - oth_neighbours_count + 1):

            total_count += 1
            # if random.random() > sampling_rate: # skip this sample with probability 1-sampling_rate
            #     continue
            # v = 1
            w = Decimal(math.comb(num_forwarders-subgraph_size-oth_neighbours_count ,k)) * Decimal(math.factorial(k+subgraph_size-1)) * Decimal(math.factorial(num_forwarders-k-subgraph_size)) 
            value_sum += w * Decimal(v)
            weight_sum += w 
            # sample_count += 1
        
        # print("with weight", weight_sum)

    def compute_phi_2(self, g, v_calc, u, sampling_rate):

        ### generate all combinations of neighbours of u and form subgraphs that contain u and its neighbours
        u_neighbours = g[u]
        all_comb = []
        for k in range(1, len(u_neighbours)+1):
            comb = list(combinations(u_neighbours, k))
            all_comb += comb

        ### generate all subgraphs that contain u and its neighbours only
        all_subgraphs = []
        for comb in all_comb:
            subgraph = set(comb)
            subgraph.add(u)
            all_subgraphs.append(subgraph)

        # for subgraph in all_subgraphs:
        #     print(subgraph)
        
        value_sum, weight_sum = Decimal(0), Decimal(0)
        total_count, sample_count = 0, 0
        num_forwarders = len(g)
        # print("num of subgraphs", len(all_subgraphs))
        for subgraph in all_subgraphs:
            subgraph_size = len(subgraph)
            oth_neighbours_count = len(g[u]) - (subgraph_size-1)
            coalition = subgraph.copy()
            coalition.remove(u)
            v = v_calc.calc_marginal_contribution(coalition, u)
            # print("marginal contribution of", u, "to", coalition, "is", v, end=" ")
            for k in range(num_forwarders - subgraph_size - oth_neighbours_count + 1):

                total_count += 1
                if random.random() > sampling_rate: # skip this sample with probability 1-sampling_rate
                    continue
                
                # v = 1
                w = Decimal(math.comb(num_forwarders-subgraph_size-oth_neighbours_count ,k)) * Decimal(math.factorial(k+subgraph_size-1)) * Decimal(math.factorial(num_forwarders-k-subgraph_size)) 
                value_sum += w * Decimal(v)
                weight_sum += w 
                sample_count += 1
            
            # print("with weight", weight_sum)
        
        return value_sum, weight_sum, sample_count, total_count


    def remove_vertices(self, g, subgraph, neighbours):
        # given g, remove vertices that are u's neighbours but not in subgraph
        g_prime = g.copy()
        for v in range(len(g)):
            if v in neighbours and v not in subgraph:
                g_prime[v] = []
                for vertices in g_prime:
                    if v in vertices:
                        vertices.remove(v)
        return g_prime        

    
    def compute_shapley_naive(self, ff_list):

        # init shapley values
        shapley_values = {}
        for ff in ff_list:
            shapley_values[ff] = 0

        # generate all permutations of ff_list
        all_perms = list(permutations(ff_list))
        n = 0
        for perm in all_perms:
            # print(n)
            n += 1
            perm_list = list(perm)
            for i in range(len(perm_list)):
                coalition = perm_list[:i]
                agent = perm_list[i]
                marginal_contribution = self.v_calc.calc_marginal_contribution(coalition, agent)
                # print("marginal contribution of", agent, "to", coalition, "is", marginal_contribution)
                shapley_values[agent] += marginal_contribution
        
        for ff in ff_list:
            shapley_values[ff] /= math.factorial(len(ff_list))
            print("shapley " + str(ff) + ": " + str(shapley_values[ff]))     



if __name__ == "__main__":

    # g = [[2,4],
    #     [2,3],
    #     [0,1,3],
    #     [1,2],
    #     [0,5,6,7],
    #     [4],
    #     [4,8],
    #     [4],
    #     [6]]

    # we sample subgraphs (at different rates)  rather than compute v-function for induced subgraphs of node and neighbours

    # example cmd: python shapley_fast_arb_prec.py data2 ID_001

    import sys
    script_name = sys.argv[0]
    arg1 = sys.argv[1]  # data2
    arg2 = sys.argv[2]  # ID_001

    # path = "data2/ID_015/" 
    path = arg1 + "/" + arg2 + "/"

    shap = Shapley_Calculation(path, sampling_rate=1.0)

    # for i in range(10,11):
    #     sampling_rate = i/10
    #     shap = Shapley_Calculation(path, sampling_rate)

    
    # sampling_rate = 1.
    # start_time = time.time()
    # shap = Shapley_Calculation(path, sampling_rate)
    # shap.compute_shapley_naive(shap.graph)
    # end_time = time.time()
    # print("elapsed time:", end_time-start_time)

    # ff = 0
    # phi_1 = shap.compute_phi_1(shap.graph, shap.v_calc, ff)
    # phi_2 = shap.compute_phi_2(shap.graph, shap.v_calc, ff)
    # print(phi_1+phi_2)

    # print(phi_1, phi_2, phi_1+phi_2)
    # print(math.factorial(len(g)))

    # print(shap.remove_vertices(g, [0,2], [2,4]))
    # shap.compute_phi_2(g, 0)