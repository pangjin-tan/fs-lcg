import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import solver3
import itertools


class V_Calculator:

    def __init__(self, graph, raw_data_path, temp_path) -> None:
        self.graph = graph
        self.path = raw_data_path
        self.temp_path = temp_path

        file_list = os.listdir(self.path)
        self.num_groups = len([f for f in file_list if f.endswith('.csv')]) // 3
        self.load_data()



    def load_data(self):

        ### load data into three lists
        ### each list has num_groups elements, each element is a dataframe
        
        self.all_sailings = []
        self.all_shipments = []
        self.all_assignments = []

        for i in range(self.num_groups):
            self.all_sailings.append(pd.read_csv(self.path + "sailing_data_" + str(i) + ".csv"))
            self.all_shipments.append(pd.read_csv(self.path + "shipment_data_" + str(i) + ".csv"))



    def solve_scenario(self, groups, sailing_data_list, shipment_data_list, assignment_data_list):

        ### solve the scenario given in the temp_path
        total_cost = 0
        for group in groups:
            sailing_data = sailing_data_list[group]
            shipment_data = shipment_data_list[group] 
            assignment_data = assignment_data_list[group]

            model = solver3.Model(sailing_data, shipment_data, assignment_data)
            method = "two_steps"
            obj = model.solve(method)
            total_cost += obj
            
        return total_cost



    def solve_for_ff(self, ff):

        ff_sailings = []
        ff_shipments = []
        ff_assignments = []

        ### extract rows of from dataframe for a specific ff 
        for i in range(self.num_groups):
            ff_sailings.append(self.all_sailings[i][self.all_sailings[i]['ff'] == ff])
            ff_shipments.append(self.all_shipments[i][self.all_shipments[i]['ff'] == ff])
        

        ### replace the first column of shipment_data datafraame with running numbers
        for i, df in enumerate(ff_shipments):
            df.reset_index(drop=True, inplace=True)
            df['request'] = df.index
        
        ### replace the first column of sailing_data datafraame with 0
        for i, df in enumerate(ff_sailings):
            df['sailing'] = 0

        ### create the assignment dataframe
        for i, df in enumerate(ff_shipments):
            ff_assignments.append(pd.DataFrame({'request': df['request'], 'sailing': 0, 'ff': ff}))

        ### export to temp path as csv
        ff_sailings_list = {}
        ff_shipments_list = {}
        ff_assignments_list = {}
        req_groups = []
        for i in range(self.num_groups):
            if len(ff_sailings[i]) == 0 or len(ff_shipments[i]) == 0:
                continue
            req_groups.append(i)
            # ff_sailings[i].to_csv(self.temp_path + "sailing_data_" + str(i) + ".csv", index=False)
            # ff_shipments[i].to_csv(self.temp_path + "shipment_data_" + str(i) + ".csv", index=False)
            # ff_assignments[i].to_csv(self.temp_path + "assignment_data_" + str(i) + ".csv", index=False)
            ff_sailings_list[i] = ff_sailings[i].reset_index(drop=True)
            ff_shipments_list[i] = ff_shipments[i].reset_index(drop=True)
            ff_assignments_list[i] = ff_assignments[i].reset_index(drop=True)
        
        ff_cost = self.solve_scenario(req_groups, ff_sailings_list, ff_shipments_list, ff_assignments_list)

        return ff_cost



    # take a coalition and compute its v-function
    def solve_specific_coalition(self, coalition):

        coalition_sailings = []
        coalition_shipments = []
        coalition_assignments = []

        req_groups = []
        for i in range(self.num_groups):
            ff_values = list(self.all_sailings[i]['ff'])
            common_coalition = list(set(coalition).intersection(ff_values))
            if len(common_coalition) > 0:
                req_groups.append(i)
                coalition_sailings.append(self.all_sailings[i][self.all_sailings[i]['ff'].isin(common_coalition)])
                coalition_shipments.append(self.all_shipments[i][self.all_shipments[i]['ff'].isin(common_coalition)])
        
        if len(req_groups) == 0:
            return 0

        ### replace the first column of shipment_data dataframe with running numbers
        for i, df in enumerate(coalition_shipments):
            df.reset_index(drop=True, inplace=True)
            df['request'] = df.index
        

        ### replace the first column of sailing_data dataframe with running numbers
        for i, df in enumerate(coalition_sailings):
            df.reset_index(drop=True, inplace=True)
            df['sailing'] = df.index
        

        ### generate assignment_data frame with columns request, sailing
        ### each assignment_data[i] is an outerjoin of sailings_data[i] sailing column and shipments_data[i] request columnn
        for i, df in enumerate(coalition_shipments):
            request_df = df['request']
            sailing_df = coalition_sailings[i]['sailing']
            assignment_df = pd.DataFrame(list(itertools.product(request_df, sailing_df)), columns=['request', 'sailing'])
            coalition_assignments.append(assignment_df)

        ### delete all files in temp path
        # file_list = os.listdir(self.temp_path)
        # for f in file_list:
        #     os.remove(self.temp_path + f)

        coalition_sailings_list = {}
        coalition_shipments_list = {}
        coalition_assignments_list = {}

        ### export to temp path as csv
        for i in range(len(coalition_sailings)):
            group = req_groups[i]
            # coalition_sailings[i].to_csv(self.temp_path + "sailing_data_" + str(group) + ".csv", index=False)
            # coalition_shipments[i].to_csv(self.temp_path + "shipment_data_" + str(group) + ".csv", index=False)
            # coalition_assignments[i].to_csv(self.temp_path + "assignment_data_" + str(group) + ".csv", index=False)
            coalition_sailings_list[group] = coalition_sailings[i].reset_index(drop=True)
            coalition_shipments_list[group] = coalition_shipments[i].reset_index(drop=True)
            coalition_assignments_list[group] = coalition_assignments[i].reset_index(drop=True)

        coalition_cost = self.solve_scenario(req_groups, coalition_sailings_list, coalition_shipments_list, coalition_assignments_list)

        return coalition_cost



    def calc_marginal_contribution(self, subcoalition, agent):
        # compute marginal contribution of agent to subgraph
        cost_with_ff = self.solve_specific_coalition(list(subcoalition) + [agent])
        cost_without_ff = self.solve_specific_coalition(list(subcoalition))
        return cost_with_ff - cost_without_ff



    def calc_marginal_contribution_deprecated(self, subcoalition, agent):
        # deprecated
        # compute marginal contribution of agent to subgraph
        cost_with_ff = self.solve_for_coalition(subcoalition, agent, with_ff=True)
        cost_without_ff = self.solve_for_coalition(subcoalition, agent, with_ff=False)
        return cost_with_ff - cost_without_ff
    
