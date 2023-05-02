import pandas as pd
from datetime import datetime, timedelta


def convert_time(starting_time, row):
    # if len(str(starting_time.values[0])) < 6:
    #     starting_time = starting_time.values[0]+":00"
    # measured_time = datetime.strptime(starting_time.values[0], '%H:%M:%S').time()
    measured_time = starting_time.values[0]
    delta_time = timedelta(hours=measured_time.hour, minutes=measured_time.minute, seconds=measured_time.second)
    x = delta_time + timedelta(hours=row.hour, minutes=row.minute, seconds=row.second)
    return datetime.strptime(str(x), "%H:%M:%S").time()


class CosmedHandler:
    cosmed_file = "cosmed_rawdata.xlsx"

    def __init__(self, subject, version,testing_log):
        self.starting_time = None
        self.subject = subject[0]+subject[-1]
        self.version = version
        self.full_cosmed_file = "../" + subject + "_" + version + "/" + subject + "_" + version +"_" + self.cosmed_file
        self.cosmed_data = pd.read_excel(self.full_cosmed_file, header=[0, 1]).iloc[:, 9:]
        self.log_data = pd.read_excel(testing_log)

    def transform_start_time(self):
        self.starting_time = self.log_data[(self.log_data["subject"] == self.subject) &
                                           (self.log_data["file_name"].str.contains("cosmed")) &
                                           (self.log_data["visit"] == self.version)]["actual_time"]

    def transform(self):
        self.cosmed_data.columns = self.cosmed_data.columns.map('_'.join)
        self.cosmed_data.drop([0], axis=0, inplace=True)
        self.cosmed_data.reset_index(inplace=True, drop=True)
        self.cosmed_data = self.cosmed_data[["t_s","Rf_1/min","VE_L/min","RQ_---","VO2/Kg_mL/min/Kg","METS_---","Amb. Temp._°C","mark Speed_m/s","Phase_---","Phase time_hh:mm:ss","Marker_---"]]
        self.transform_start_time()
        self.cosmed_data["time"] = self.cosmed_data["t_s"].apply(lambda x: convert_time(self.starting_time, x))
        return self.cosmed_data

    def show_data(self):
        return self.cosmed_data
