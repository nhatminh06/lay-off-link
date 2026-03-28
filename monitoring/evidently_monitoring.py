"""Evidently Data Drift Monitoring"""

from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, DataQualityPreset
from evidently.test_suite import TestSuite
from evidently.tests import *
import pandas as pd
import os
from datetime import datetime


class DataDriftMonitor:
    def __init__(self, reference_data, feature_names):
        self.reference_data = reference_data
        self.feature_names = feature_names
        self.reports_dir = "monitoring/reports"
        os.makedirs(self.reports_dir, exist_ok=True)

    def create_drift_report(self, current_data):
        report = Report(metrics=[DataDriftPreset(), DataQualityPreset()])
        report.run(reference_data=self.reference_data, current_data=current_data)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f"{self.reports_dir}/drift_report_{timestamp}.html"
        report.save_html(report_path)
        return report_path, report.as_dict()

    def run_drift_tests(self, current_data):
        test_suite = TestSuite(
            tests=[
                TestNumberOfColumnsWithMissingValues(),
                TestNumberOfRowsWithMissingValues(),
                TestNumberOfDriftedColumns(),
            ]
        )
        test_suite.run(reference_data=self.reference_data, current_data=current_data)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        test_path = f"{self.reports_dir}/drift_tests_{timestamp}.html"
        test_suite.save_html(test_path)
        return test_path, test_suite.as_dict()

    def check_drift(self, current_data, threshold=0.5):
        _, report_dict = self.create_drift_report(current_data)
        metrics = report_dict.get("metrics", [])
        for metric in metrics:
            if "result" in metric and "drift_by_columns" in metric["result"]:
                drift_columns = metric["result"]["drift_by_columns"]
                drift_count = sum(1 for v in drift_columns.values() if v.get("drift_detected"))
                return (drift_count / len(drift_columns)) > threshold if drift_columns else False
        return False
