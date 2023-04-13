import pandas as pd
from datetime import datetime, timedelta


def convert_time(starting_time, row):
    if len(starting_time.values[0]) < 6:
        starting_time = starting_time.values[0]+":00"
    measured_time = datetime.strptime(starting_time.values[0], '%H:%M:%S').time()
    delta_time = timedelta(hours=measured_time.hour, minutes=measured_time.minute, seconds=measured_time.second)
    x = delta_time + timedelta(hours=row.hour, minutes=row.minute, seconds=row.second)
    return datetime.strptime(str(x), "%H:%M:%S").time()


class CosmedHandler:
    cosmed_file = "cosmed_rawdata.xlsx"

    def __init__(self, subject, version,testing_log):
        self.starting_time = None
        self.subject = subject
        self.version = version
        self.full_cosmed_file = "../" + subject + "_" + version + "/" + subject + "_" + version +"_" + self.cosmed_file
        self.cosmed_data = pd.read_excel(self.full_cosmed_file, header=[0, 1]).iloc[:, 9:]
        self.log_data = pd.read_csv(testing_log)

    def transform_start_time(self):
        self.starting_time = self.log_data[(self.log_data["Subject"] == self.subject) &
                                           (self.log_data["Sensor Name"] == "Cosmed") &
                                           (self.log_data["Visit"] == int(self.version[-1]))]["Local Time"]

    def transform(self):
        self.cosmed_data.columns = self.cosmed_data.columns.map('_'.join)
        self.cosmed_data.drop([0], axis=0, inplace=True)
        self.cosmed_data.reset_index(inplace=True, drop=True)
        self.transform_start_time()
        self.cosmed_data["time"] = self.cosmed_data["t_s"].apply(lambda x: convert_time(self.starting_time, x))
        return self.cosmed_data

    def show_data(self):
        return self.cosmed_data
