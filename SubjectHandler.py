import pandas as pd
import os
from datetime import datetime, timedelta
from CosmedHandler import CosmedHandler
from ShimmerHandler import ShimmerHandler


def remove_na_values(table):
    start_ind = table.index.tolist()[0]
    start_ind_ls = table[~table["indexing"].isna()].index.tolist()
    if (len(start_ind_ls) > 0):
        start_ind = start_ind_ls[0]

    end_ind = table.index.tolist()[-1]
    end_ind_ls = table[~table["indexing"].isna()].index.tolist()
    if (len(end_ind_ls) > 0):
        end_ind = end_ind_ls[-1]
    return table.loc[start_ind:end_ind, :]


class SubjectHandler:

    def __init__(self, subject, version, testing_log):
        self.subject = subject
        self.version = version
        self.testing_log = testing_log
        # Objects
        self.cosmed_object = None
        self.shimmer_object = None
        # Data
        self.cosmed_data = None
        self.shimmer_data = None
        self.verified_shimmer_files = []
        self.cosmed_shimmer = {}
        self.invalid_files = []

    def initialize_data(self):
        self.cosmed_object = CosmedHandler(subject, version, self.testing_log)
        self.shimmer_object = ShimmerHandler(subject, version)

    def transform(self):
        self.cosmed_data = self.cosmed_object.transform()
        self.shimmer_data = self.shimmer_object.transform()

    def verify_shimmer(self):
        log_data = self.cosmed_object.log_data
        start_time_temp = log_data[(log_data["Subject"] == self.subject) &
                                   (log_data["Sensor Name"] == "Cosmed") &
                                   (log_data["Visit"] == int(self.version[-1]))]["Local Time"]
        start_time = datetime.strptime(start_time_temp.tolist()[0], "%H:%M:%S").time()
        # print("         Log time", start_time)
        for k, v in self.shimmer_data.items():
            start_time_shimmer = v["time_no_milli"].tolist()[0]
            time_diff = abs((timedelta(hours=start_time.hour, minutes=start_time.minute, seconds=start_time.second)
                             - timedelta(hours=v["time_no_milli"].tolist()[0].hour,
                                         minutes=v["time_no_milli"].tolist()[0].minute,
                                         seconds=v["time_no_milli"].tolist()[0].second)) \
                            .total_seconds())
            if time_diff > 3600:
                self.invalid_files.append(k)
                print(k, start_time_shimmer, "Time difference is more than 1 hour. Invalid")

    def combine_cos_shim(self):
        for k, v in self.shimmer_data.items():
            self.cosmed_shimmer[k] = v.merge(self.cosmed_data, how='left', left_on="time_no_milli", right_on="time")
            self.cosmed_shimmer[k].reset_index(drop=True, inplace=True)
            # self.cosmed_shimmer[k] = remove_na_values(self.cosmed_shimmer[k])
            # self.cosmed_shimmer[k].fillna(method='ffill', inplace=True)

    def write_to_files(self):
        if not os.path.exists("../Result/" + subject + "_" + version + "/"):
            os.makedirs("../Result/" + subject + "_" + version + "/")
        for file in self.cosmed_shimmer.keys():
            self.cosmed_shimmer[file].to_csv("../Result/" + subject + "_" + version + "/" + file, index=False)
        with open("../Result/"+subject+"_"+version+"/invalid_files.txt","w") as f:
            for item in self.invalid_files:
                f.write("%s\n" % item)

    def show_cos_shim(self, file):
        return self.cosmed_shimmer[file]


if __name__ == '__main__':
    testing_log = "../allsubjects_testing_log.csv"
    for folder in os.listdir("../"):
        print("Folder", folder)
        if "subject" in folder and "allsubjects" not in folder :
            subject = folder.split("_")[0]
            version = folder.split("_")[1]
            print("         Initializing ...")
            subject_handle = SubjectHandler(subject, version, testing_log)
            subject_handle.initialize_data()
            print("         Transforming ...")
            subject_handle.transform()
            print("         Verifying Shimmer data ...")
            subject_handle.verify_shimmer()
            print("         Combining cosmed and shimmer ...")
            subject_handle.combine_cos_shim()
            print("         Write to files ...")
            subject_handle.write_to_files()
    print("done")
