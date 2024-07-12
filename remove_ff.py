import random
import os
import pandas as pd
import json
import sys

class Remove_FF:

    def __init__(self, input_path, output_path, num) -> None:
        self.input_path = input_path + "raw/"
        self.output_path = output_path + "raw/"

        ### randomly select some ff from all ff
        self.ff_list = self.get_all_ff(self.input_path)
        self.new_ff_list = self.include_ff(self.ff_list, num)
        self.new_ff_list = [int(ff) for ff in self.new_ff_list]
        self.dump_data(self.new_ff_list, output_path + "log/", "new_ff_list")

        ### generate the relevant sailing_data and shipment_data
        self.gen_subgraph_data(self.input_path, self.output_path, self.new_ff_list, "sailing_data_")
        self.gen_subgraph_data(self.input_path, self.output_path, self.new_ff_list, "shipment_data_")

        ### remove files with empty df
        self.remove_files_with_empty_df(output_path + "raw/")

        ### relabel files
        self.relabel_files(output_path + "raw/", "sailing_data_")
        self.relabel_files(output_path + "raw/", "shipment_data_")

        ### generate assignment data
        self.generate_assignment_data(output_path + "raw/")
        
        

    def get_all_ff(self, input_path):
        ### iterate through all files with prefix "sailing_data_" and read them into a dataframe
        ### to get list of unique ff

        ff_list = []
        for file in os.listdir(input_path):
            if file.startswith("sailing_data_"):
                sailing_data_df = pd.read_csv(input_path + file)
                ff_list += list(sailing_data_df["ff"].unique())

        ### remove duplicates from ff_list
        ff_list = list(dict.fromkeys(ff_list))
        return ff_list



    def include_ff(self, ff_list, num):
        return random.sample(ff_list, num)



    def gen_subgraph_data(self, input_path, output_path, new_ff_list, file_prefix):
        ### from each file starting with file_prefix, select only rows where ff is in new_ff_list

        for file in os.listdir(input_path):
            if file.startswith(file_prefix):
                df = pd.read_csv(input_path + file)
                # select on rows where ff is in new_ff_list
                df = df[df["ff"].isin(new_ff_list)]
                df.to_csv(output_path + file, index=False)


    
    def remove_files_with_empty_df(self, path):
        for file in os.listdir(path):
            df = pd.read_csv(path + file)
            if df.empty:
                os.remove(path + file)



    def relabel_files(self, path, prefix):
        index = 0
        for i in range(0,1000):
            if os.path.isfile(path + prefix + str(i) + ".csv"):
                new_file_name = prefix + str(index) + ".csv"
                index += 1
                # print("rename", prefix + str(i) + ".csv", "to", new_file_name)
                os.rename(path + prefix + str(i) + ".csv", path + new_file_name)



    def gen_assignment_data(self, shipment_data_df, sailings):
        assignment_data_df = pd.DataFrame(columns=["request", "sailing", "ff"])

        ### iterate through all rows in sailing_data_df, and unique sailings
        ### generate the combination of request and unique sailings

        for index, row in shipment_data_df.iterrows():
            request_value = row['request']
            ff_value = row['ff']
            for sailing_value in sailings:
                ### append a row of request, ff, sailing to assignment_data_df
                row = pd.DataFrame({"request": [request_value], "sailing": [sailing_value], "ff": [ff_value]})
                # Concatenate the rows vertically using pd.concat
                assignment_data_df = pd.concat([assignment_data_df, row], ignore_index=True)

        return assignment_data_df

        

    def generate_assignment_data(self, path):
        ### generate assignment data from sailing data and shipment data
        ### iterate through all files with prefix "sailing_data_" and "shipment_data" read them into a dataframe

        sailing_data_prefix = "sailing_data_"
        shipment_data_prefix = "shipment_data_"
        assignment_data_prefix = "assignment_data_"

        for file in os.listdir(path):
            if file.startswith(sailing_data_prefix):
                index = file.split("_")[2].split(".")[0]

                sailing_data_df = pd.read_csv(path + file)
                shipment_data_df = pd.read_csv(path + shipment_data_prefix + index + ".csv")
                
                ### get list of unique sailings
                sailings = list(sailing_data_df["sailing"].unique())
                # print(index, ":", sailings)

                ### generate assignment data and export to csv    
                assignment_data_df = self.gen_assignment_data(shipment_data_df, sailings)
                assignment_data_df.to_csv(path + assignment_data_prefix + index + ".csv", index=False)
                




    def dump_data(self, data, output_path, file_name):
        ### dump data to json file

        output_path = output_path + file_name + ".json"
        with open(output_path, 'w') as json_file:
            json.dump(data, json_file)
    


if __name__ == "__main__":

    ### python remove_ff.py data12 ID_008 ID_007 35

    if len(sys.argv) == 5:
        dir = sys.argv[1]
        input_dir = sys.argv[2]
        output_dir = sys.argv[3]
        num = int(sys.argv[4])
    else:
        print("Usage: python remove_ff.py <input_path> <output_path> <num>")


    input_path = dir + "/" + input_dir + "/"
    output_path = dir + "/" + output_dir + "/"

    remove_ff = Remove_FF(input_path, output_path, num)