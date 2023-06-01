import pandas as pd
import os
from datetime import datetime, timedelta
from CosmedHandler import CosmedHandler
from ShimmerHandler import ShimmerHandler
from SummaryHandler import SummaryHandler
from tsfresh.feature_extraction import feature_calculators
from tsfresh.feature_extraction import extract_features

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
        self.summary_object = None
        # Data
        self.cosmed_data = None
        self.shimmer_data = None
        self.verified_shimmer_files = []
        self.cosmed_shimmer = {}
        self.invalid_files = []
        self.compressed_data = {}


    def initialize_data(self):
        self.cosmed_object = CosmedHandler(subject, version, self.testing_log)
        self.shimmer_object = ShimmerHandler(subject, version)
        self.summary_object = SummaryHandler(subject,version)

    def transform(self):
        self.cosmed_data = self.cosmed_object.transform()
        self.shimmer_data = self.shimmer_object.transform()
        self.summary_data = self.summary_object.transform()

    def verify_shimmer(self):
        log_data = self.cosmed_object.log_data
        start_time_temp = log_data[(log_data["subject"] == subject) &
                                   (log_data["file_name"].str.contains("cosmed")) &
                                   (log_data["visit"] == version)]["actual_time"]
        # start_time = datetime.strptime(start_time_temp.tolist()[0], "%H:%M:%S").time()
        # print("         Log time", start_time)
        for k, v in self.shimmer_data.items():
            start_time_shimmer = v["time_no_milli"].dropna().tolist()[0]
            time_diff = abs(
                (timedelta(hours=start_time_temp.hour, minutes=start_time_temp.minute, seconds=start_time_temp.second)
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
            try:
                index_not_null = self.cosmed_shimmer[k][self.cosmed_shimmer[k]["t_s"].notna()].index.tolist()[0]
                self.cosmed_shimmer[k] = self.cosmed_shimmer[k].iloc[index_not_null:, :].reset_index(drop=True)
            except :
                print(k)
                self.invalid_files.append(k)

    def compress_data_btw_breath(self):
        for k,v in self.shimmer_data.items():
            all_data = self.cosmed_shimmer[k][self.cosmed_shimmer[k]["Marker_---"].notnull()]
            index_breaths = all_data.index.tolist()[0:]
            breath_ls = []
            for index in range(1,len(index_breaths)):
                breath_df = self.cosmed_shimmer[k].loc[index_breaths[index-1]:index_breaths[index]-1,:]
                tsfresh_columns = [col for col in breath_df.columns if "Accel" in col or "Timestamp" in col] # or col.contains("Gyro") or col.contains("Mag")
                timestamp_col = [col for col in tsfresh_columns if "Timestamp" in col][0]
                selected_col_breath_df = breath_df[tsfresh_columns].reset_index()
                lst_results = []
                for column in tsfresh_columns[1:]:
                    result = {
                        column+"_root_mean_square": feature_calculators.root_mean_square(selected_col_breath_df[column]),
                        column+"_variance":  feature_calculators.variance(selected_col_breath_df[column]),
                        column+"_mean": feature_calculators.mean(selected_col_breath_df[column]),
                        column+"_standard_deviation": feature_calculators.standard_deviation(selected_col_breath_df[column]),
                        # column+"_power": feature_calculators.spkt_welch_density(selected_col_breath_df[column],[2])
                    }
                    temp_result = pd.DataFrame.from_dict(result,orient="index").T
                    lst_results.append(temp_result)
                breath_compress = pd.concat(lst_results,axis=1)
                breath_compress["Rows"]=str(breath_df.index.tolist()[0])+"_"+str(breath_df.index.tolist()[-1])
                breath_ls.append(breath_compress)
            try:
                final_res = pd.concat(breath_ls).reset_index(drop=True)
                combine_with_orgi_data = pd.concat([all_data.reset_index(drop=True), final_res], axis=1)
                self.compressed_data[k]=combine_with_orgi_data
            except:
                print(k)

    def combine_summary(self):
        for k,v in self.compressed_data.items():
            self.compressed_data[k] = v.merge(self.summary_data[["time","HR","BR"]], how='left', left_on="time_no_milli", right_on="time")
            self.compressed_data[k].reset_index(drop=True, inplace=True)

    def write_to_files(self):
        if not os.path.exists("../Result/" + subject + "_" + version + "/"):
            os.makedirs("../Result/" + subject + "_" + version + "/")
        if not os.path.exists("../Result/" + subject + "_" + version + "/Compressed Data/"):
            os.makedirs("../Result/" + subject + "_" + version + "/Compressed Data/")

        for file in self.cosmed_shimmer.keys():
            self.cosmed_shimmer[file].to_csv("../Result/" + subject + "_" + version + "/" +file, index=False)
        try:
            for file in self.compressed_data.keys():
                self.compressed_data[file].to_csv("../Result/" + subject + "_" + version + "/Compressed Data/" + "compressed_"+file, index=False)
        except:
            print (self.compressed_data[file])
        with open("../Result/" + subject + "_" + version + "/invalid_files.txt", "w") as f:
            for item in self.invalid_files:
                f.write("%s\n" % item)

    def show_cos_shim(self, file):
        return self.cosmed_shimmer[file]


if __name__ == '__main__':
    testing_log = "../shimmer_timelog.xlsx"
    for folder in os.listdir("../"):
        print("Folder", folder)
        if "subject" in folder and "allsubjects" not in folder and "subject5" in folder:
            subject = folder.split("_")[0]
            version = folder.split("_")[1]
            print("         Initializing ...")
            subject_handle = SubjectHandler(subject, version, testing_log)
            subject_handle.initialize_data()
            print("         Transforming ...")
            subject_handle.transform()
            # print("         Verifying Shimmer data ...")
            # subject_handle.verify_shimmer()
            print("         Combining cosmed and shimmer ...")
            subject_handle.combine_cos_shim()
            print("         Compress data...")
            subject_handle.compress_data_btw_breath()
            print("         Integrate to summary file")
            subject_handle.combine_summary()
            print("         Write to files ...")
            subject_handle.write_to_files()
    print("done")
