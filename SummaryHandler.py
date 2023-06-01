import pandas as pd
import os
from datetime import datetime, timedelta

class SummaryHandler:
    summary_file = "Summary.csv"

    def __init__(self, subject, version):
        self.subject = subject[0] + subject[-1]
        self.version = version
        self.full_summary_file = "../" + subject + "_" + version + "/" + subject + "_" + version + "_" + self.summary_file
        self.summary_data = pd.read_csv(self.full_summary_file)

    def transform(self):
        self.summary_data["time"] = self.summary_data["Time"].apply(lambda x: x.split(" ")[-1][:-4])
        self.summary_data["time"] = self.summary_data["time"].apply(
                    lambda x: datetime.strptime(x, "%H:%M:%S").time())
        return self.summary_data

    def show_data(self):
        return self.summary_data