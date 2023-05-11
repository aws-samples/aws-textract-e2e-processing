import textractmanifest as tm
import csv
from typing import List


class CSVRow():

    rows: List[List[str]] = list()

    def __init__(self,
                 classification: str,
                 queries: List[List[str]],
                 textract_features: List[str],
                 page_number=None):
        self.classification = classification
        self.page_number = page_number
        self.queries = queries
        self.textract_features = textract_features

        self.queries_config: List[tm.Query] = self.get_queries_config()
        self.manifest: tm.IDPManifest = self.create_manifest()
        self.csv_row = self.get_csv_row()
        CSVRow.rows.append(self.csv_row)


    def get_queries_config(self):
        queries_config: List[tm.Query] = list()
        for query in self.queries:
            queries_config.append(tm.Query(alias=query[0], text=query[1]))
        return queries_config

    def create_manifest(self):
        return tm.IDPManifest(queries_config=self.queries_config,
                                  textract_features=self.textract_features)

    def get_csv_row(self):
        return [self.classification, tm.IDPManifestSchema().dumps(self.manifest)]


def get_csv_rows():
    # claimform
    claimform_queries: List[List[str]] = list()
    claimform_features: List[str] = ["FORMS", "TABLES", "SIGNATURES"]
    claimform_row = CSVRow("claimform",
                          claimform_queries,
                          claimform_features)

    # doctorsnote
    doctorsnote_queries: List[List[str]] = [
        ["doctorsnote_PATIENT", "Who is the patient?"],
        ["doctorsnote_PROVIDER", "Who is the attending provider?"]
    ]
    doctorsnote_features: List[str] = ["QUERIES", "SIGNATURES"]
    doctorsnote_row = CSVRow("doctorsnote",
                          doctorsnote_queries,
                          doctorsnote_features)

    # dischargesummary
    dischargesummary_queries: List[List[str]] = [
        ["dischargesummary_PATIENT", "What is the patient's name?"],
        ["dischargesummary_ADMIT", "When was the patient admitted?"],
        ["dischargesummary_DATE_DISCHARGE", "When was the patient discharged?"],
        ["dischargesummary_SYMPTOMS", "What are the symptoms?"],
        ["dischargesummary_SUMMARY", "What is the discharge studies summary?"],
    ]
    dischargesummary_features: List[str] = ["FORMS", "QUERIES"]
    dischargesummary_row = CSVRow("dischargesummary",
                             dischargesummary_queries,
                             dischargesummary_features)

    return CSVRow.rows


def write_csv():
    rows = get_csv_rows()

    # WRITE default_config.csv
    with open('default_config.csv', 'w') as f:
        csv_writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(rows)
