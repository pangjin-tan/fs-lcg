import glob
import os
import pandas as pd
from pulp import *
import time
import argparse
import math

class Model:

    def __init__(self, sailing_data, shipment_data, assignment_data):

        self.v_max = 30

        # convert dataframe to dictionary
        self.sailing_data = sailing_data.to_dict(orient='index')
        self.shipment_data = shipment_data.to_dict(orient='index')
        self.assignment_data = assignment_data.to_dict(orient='index')

        self.sailings = list(self.sailing_data.keys())
        self.shipments = list(self.shipment_data.keys())

        # create feasible_sailings
        self.feasible_sailings = {}
        for _,v in self.assignment_data.items():
            request = v['request']
            sailing = v['sailing']
            if request in self.feasible_sailings:
                self.feasible_sailings[request].append(sailing)
            else:
                self.feasible_sailings[request] = [sailing]

        # create feasible_shipments
        self.feasible_shipments = {}
        for _,v in self.assignment_data.items():
            request = v['request']
            sailing = v['sailing']
            if sailing in self.feasible_shipments:
                self.feasible_shipments[sailing].append(request)
            else:
                self.feasible_shipments[sailing] = [request]



    def solve_exact(self, sailing_data, shipment_data, sailings, shipments, feasible_sailings, feasible_shipments, v_max):

        # create y variables, assign shipment a sailing-box
        y = {}
        for shipment in shipments:
            for sailing in feasible_sailings[shipment]:
                for box in range(sailing_data[sailing]['boxes']):
                    y[shipment, sailing, box] = LpVariable(f'y_{shipment}_{sailing}_{box}', cat='Binary')

        # create z variable, 1 if a sailing-box is used
        z = {}
        for sailing in sailings:
            for box in range(sailing_data[sailing]['boxes']):
                z[sailing, box] = LpVariable(f'z_{sailing}_{box}', cat='Binary')
        
        # add objective function
        model = LpProblem('Asgn-Prob', LpMinimize)
        model += lpSum([z[sailing, box] * sailing_data[sailing]['cost'] for sailing in sailings for box in range(sailing_data[sailing]['boxes'])])

        # add constraint: each shipment is assigned to one sailing-box
        for shipment in shipments:
            model += lpSum([y[shipment, sailing, box] for sailing in feasible_sailings[shipment] for box in range(sailing_data[sailing]['boxes'])]) == 1

        
        # add constraint: total volume of shipments assigned to a sailing-box is less than or equal to the sailing-box's capacity (in cbm)
        for sailing in sailings:
            for box in range(sailing_data[sailing]['boxes']):
                model += lpSum([y[shipment, sailing, box] * shipment_data[shipment]['cbm'] for shipment in feasible_shipments[sailing]]) <= v_max * z[sailing, box]

        # print(model)
        
        # model.writeLP("model.lp")

        # solve the model
        solver = CPLEX_CMD(msg=0, options=['set timelimit 60'])
        status = model.solve(solver)

        # get solution
        if status == 1:
            obj = value(model.objective)
            # print("Objective value:", obj)
            return obj
        else:
            print("No solution found")




    # greedily assign shipments to sailing-boxes
    def get_bound(self, sailing_data, shipment_data, sailings, shipments, feasible_sailings, feasible_shipments, v_max, y_ones):

        
        def fill_box(sailing_vol, sailing, cbm):
            for i in range(sailing_data[sailing]['boxes']):
                if sailing_vol[sailing][i] + cbm <= v_max:
                    return i
            return -1

        def next_sailing(sailing, feasible_sailings_for_shipment):
            # find next cheapest sailing
            curr_cost = sailing_data[sailing]['cost']
            lowest = math.inf
            for s in feasible_sailings_for_shipment:
                if sailing_data[s]['cost'] > curr_cost and sailing_data[s]['cost'] < lowest:
                    lowest, next_sailing = sailing_data[s]['cost'], s
            return next_sailing

        
        # intialize sailing_vol
        sailing_vol = {}
        for sailing in sailings:
            sailing_vol[sailing] = [0] * sailing_data[sailing]['boxes']
        
        
        shipments =[(y[0], y[1], shipment_data[y[0]]["cbm"]) for y in y_ones]
        shipments.sort(key=lambda x: x[2], reverse=True)
        for shipment, sailing, cbm in shipments:
            while (True):
                box_no = fill_box(sailing_vol, sailing, cbm)
                if box_no>=0:
                    sailing_vol[sailing][box_no] += cbm
                    break
                else:
                    sailing = next_sailing(sailing, feasible_sailings[shipment])
        
        # generate dictionary to count non-zero boxes for each sailing based on sailing_vol
        box_count = {}
        for sailing in sailings:
            box_count[sailing] = sum([1 for v in sailing_vol[sailing] if v>0])

        # print(box_count)

        return box_count



    # adjust the box capacity in the data itself
    def update(self, old_sailing_data, shipment_data, assignment_data, box_count):

        # udpate box count and remove sailings with no boxes
        sailing_data = {}
        for sailing in old_sailing_data:
            if box_count.get(sailing, 0) > 0:
                sailing_data[sailing] = old_sailing_data[sailing]
                sailing_data[sailing]['boxes'] = box_count[sailing]


        sailings = list(sailing_data.keys())
        shipments = list(shipment_data.keys())        

        # create feasible_sailings
        feasible_sailings = {}
        for _,v in assignment_data.items():
            request = v['request']
            sailing = v['sailing']
            if sailing not in sailing_data.keys():
                continue
            if request in feasible_sailings:
                feasible_sailings[request].append(sailing)
            else:
                feasible_sailings[request] = [sailing]

        # create feasible_shipments
        feasible_shipments = {}
        for _,v in self.assignment_data.items():
            request = v['request']
            sailing = v['sailing']
            if sailing not in sailing_data.keys():
                continue
            if sailing in feasible_shipments:
                feasible_shipments[sailing].append(request)
            else:
                feasible_shipments[sailing] = [request]
            
        return sailing_data, shipment_data, sailings, shipments, feasible_sailings, feasible_shipments



    def init_asgn(self, assignment_data, sailing_data):
        asgn = {}
        y_ones = []
        # assign each request to the cheapest sailing
        for _, data in assignment_data.items():
            request = data['request']
            sailing = data['sailing']
            if request not in asgn:
                asgn[request] = sailing
            else:
                curr_sailing = asgn[request]
                if sailing_data[sailing]['cost'] < sailing_data[curr_sailing]['cost']:
                    asgn[request] = sailing
        y_ones = [[request, sailing] for request, sailing in asgn.items()]
        return y_ones



    def solve_two_steps(self, sailing_data, shipment_data, assignment_data, sailings, shipments, feasible_sailings, feasible_shipments, v_max):
        y_ones = self.init_asgn(assignment_data, sailing_data)
        box_count = self.get_bound(sailing_data, shipment_data, sailings, shipments, feasible_sailings, feasible_shipments, v_max, y_ones)
        new_sailing_data, new_shipment_data, new_sailings, new_shipments, new_feasible_sailings, new_feasible_shipments = self.update(sailing_data, shipment_data, assignment_data, box_count)
        obj = self.solve_exact(new_sailing_data, new_shipment_data, new_sailings, new_shipments, new_feasible_sailings, new_feasible_shipments, v_max)
        return obj



    def solve(self, method):
        if method == 'exact':
            return self.solve_exact(self.sailing_data, 
                             self.shipment_data,
                             self.sailings,
                             self.shipments,
                             self.feasible_sailings,
                             self.feasible_shipments,
                             self.v_max)
        elif method == 'two_steps':
            return self.solve_two_steps(self.sailing_data, 
                                   self.shipment_data, 
                                   self.assignment_data,
                                   self.sailings,
                                   self.shipments,
                                   self.feasible_sailings,
                                   self.feasible_shipments,
                                   self.v_max)


########################################


if __name__ == "__main__":

    # remove clone*.log files
    for f in glob.glob("clone*.log"):
        os.remove(f)

    # read arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--method', type=str, default='exact')
    parser.add_argument('--id', type=int)
    args = parser.parse_args()
    method = args.method
    id = args.id

    # read data2
    path = "data2/ID_001/temp/"
    sailing_data = pd.read_csv(f'{path}sailing_data_{id}.csv')
    shipment_data = pd.read_csv(f'{path}shipment_data_{id}.csv')
    assignment_data = pd.read_csv(f'{path}assignment_data_{id}.csv')

    model = Model(sailing_data, shipment_data, assignment_data)

    start_time = time.time()
    model.solve(method)
    print("Elapsed time:", time.time() - start_time)


########################################