
import pandas as pd
from datetime import datetime
import os

def get_shimmer_file(subject,version):
    return [s for s in os.listdir( "../" + subject + "_" + version + "/") if any(xs in s for xs in ["Arm","Leg","Torso"])]

class ShimmerHandler:

    def __init__(self, subject, version):
        self.full_shimmer_file = {}
        self.subject = subject
        self.version = version
        self.shimmer_files = get_shimmer_file(subject,version)
        for file in self.shimmer_files:
            full_name = "../" + subject + "_" + version + "/"  + file
            # print(full_name)
            if (self.subject =="subject3" and self.version=="1"):
                self.full_shimmer_file[file] = pd.read_csv(full_name, skiprows=1, header=[0, 1],delimiter="/t").iloc[:, :-1]
            else:
                self.full_shimmer_file[file] = pd.read_csv(full_name, skiprows=1, header=[0, 1]).iloc[:, :-1]
            self.full_shimmer_file[file].columns = self.full_shimmer_file[file].columns.map('_'.join)

    def transform(self):
        for k, v in self.full_shimmer_file.items():
            shimmer_timestamp_col = [i for i in v.columns if "Timestamp" in i][0]
            if shimmer_timestamp_col != "":
                v["time"] = v[shimmer_timestamp_col].apply(lambda x: datetime.fromtimestamp(x/1000.0))
                v["time_no_milli"] = v["time"].apply(lambda x: datetime.strptime(str(x).split(" ")[1][:8], "%H:%M:%S").time())
                # testing_df = v[["time_no_milli","Arm_R_8A44_Mag_Z_CAL_local_flux"]]
                # replace_nan_shimmer = self.shimmer_data[self.shimmer_data["time_no_milli"].duplicated()]
                # replace_nan_shimmer["time_no_milli"] = ""
                # self.shimmer_data.update(replace_nan_shimmer)
                # self.shimmer_data["time"] = self.shimmer_data["time"].replace("", np.nan)
                # self.shimmer_data.drop(columns=[shimmer_timestamp_col],inplace=True)
        return self.full_shimmer_file

    def show_data(self, file):
        return self.full_shimmer_file[file]
