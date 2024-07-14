import networkx as nx
import random
import pandas as pd
import sys

def gen_network(file_path, nx_type="long-tailed", num_forwarders=10, seed=1):
    if nx_type == "long-tailed":
        G = nx.powerlaw_cluster_graph(num_forwarders, 2, 0.1, seed)
    elif nx_type == "small-world":
        average_degree = 2     # Target average degree
        # Calculate the initial degree for the regular ring lattice
        # k = max(average_degree, int(2 * average_degree / num_forwarders))
        # Set a small rewiring probability
        p = 0.3
        G = nx.watts_strogatz_graph(n=num_forwarders, k=average_degree, p=p, seed=seed)

    ffs = list(G.nodes())
    edges = list(G.edges())

    nx.write_gml(G, file_path+"graph.gml")

    return ffs, edges



def gen_service_to_edge_dict(ffs, edges, num_services=20):
    # num_services = 20
    # create a service for each edge
    edge_to_service_dict= {}

    # first case where there are more edges than services
    if len(edges) >= num_services:
        service_count = 0
        for edge in edges:
            if service_count < num_services:
                edge_to_service_dict[edge] = [service_count]
                service_count += 1
            else:
                edge_to_service_dict[edge] = [random.randint(0, num_services-1)]
        # print(edge_to_service_dict)

    # in case there are more services than edges, assign a random edge to each service
    if num_services > len(edges):
        print("num services > num edges")
        service_count = 0
        for edge in edges:
            edge_to_service_dict[edge] = [service_count]
            service_count += 1
        for i in range(service_count, num_services):
            edge_to_service_dict[random.choice(edges)] += [i]

        

    service_to_edge_dict = {}
    for edge, services in edge_to_service_dict.items():
        for service in services:
            if service not in service_to_edge_dict:
                service_to_edge_dict[service] = [edge]
            else:
                service_to_edge_dict[service] += [edge]
    
    return service_to_edge_dict



def gen_data_for_service(service_to_edge_dict, service, max_num_shipments=5, 
                         min_cost = 800, max_cost=1200, 
                         min_box = 5, max_box = 10):
    edges = service_to_edge_dict[service]
    # extract all ff from edges
    ffs = []
    for edge in edges:
        if edge[0] not in ffs:
            ffs += [edge[0]]
        if edge[1] not in ffs:
            ffs += [edge[1]]
    
    # generate shipments
    shipments = []
    req_id = 0
    for ff in ffs:
        num_shipments = random.randint(1, max_num_shipments)
        for i in range(num_shipments):
            cbm = random.randint(1, 29)
            shipment = [req_id, cbm, ff]
            shipments += [shipment]
            req_id += 1

    # generating sailings
    sailings = []
    sailing_id = 0
    for ff in ffs:
        cost = random.randint(min_cost, max_cost)
        boxes = random.randint(min_box, max_box)
        sailing = [sailing_id, cost, boxes, ff]
        sailings += [sailing]
        sailing_id += 1
    
    # generate assignments
    assignments = []
    for i in range(sailing_id):
        for j in range(req_id):
            assignments += [[j, i]]

    return shipments, sailings, assignments    
    


def gen_data_for_all_services(path, service_to_edge_dict, max_num_shipments=5, 
                         min_cost = 800, max_cost=1200, 
                         min_box = 5, max_box = 10):
    
    for service in service_to_edge_dict.keys():
        shipments_service, sailings_service, assignments_service = gen_data_for_service(service_to_edge_dict, service, 
                                                                                        max_num_shipments, min_cost, max_cost, 
                                                                                        min_box, max_box)

        shipments_df = pd.DataFrame(shipments_service, columns=["request", "cbm", "ff"])
        sailings_df = pd.DataFrame(sailings_service, columns=["sailing", "cost", "boxes", "ff"])
        assignments_df = pd.DataFrame(assignments_service, columns=["request", "sailing"])

        shipments_df.to_csv(path + "shipment_data_" + str(service) + ".csv", index=False)
        sailings_df.to_csv(path + "sailing_data_" + str(service) + ".csv", index=False)
        assignments_df.to_csv(path + "assignment_data_" + str(service) + ".csv", index=False)
    
    return



if __name__ == "__main__":

    # python gen_networks.py $dir ID_005 small-world 50
    
    data_dir = sys.argv[1]
    data_id = sys.argv[2]
    nx_type = sys.argv[3]
    num_forwarders = int(sys.argv[4])

    file_path = "./" + data_dir + "/" + data_id + "/"

    ffs, edges = gen_network(file_path + "log/", nx_type, num_forwarders, 1)
    factor = 1.2
    num_services = int(len(edges) * factor)
    service_to_edge_dict = gen_service_to_edge_dict(ffs, edges, num_services)
    gen_data_for_all_services(file_path + "raw/", service_to_edge_dict, 5, 800, 1200, 5, 10)