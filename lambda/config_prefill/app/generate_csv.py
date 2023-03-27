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
    # acord125
    acord125_queries: List[List[str]] = list()
    acord_125_features: List[str] = ["FORMS", "TABLES"]
    acord125_row = CSVRow("acord125",
                          acord125_queries,
                          acord_125_features)

    # acord126
    acord126_queries: List[List[str]] = list()
    acord126_features: List[str] = ["FORMS", "TABLES"]
    acord126_row = CSVRow("acord126",
                          acord126_queries,
                          acord126_features)

    # acord140
    acord140_queries: List[List[str]] = list()
    acord140_features: List[str] = ["FORMS", "TABLES"]
    acord140_row = CSVRow("acord140",
                          acord140_queries,
                          acord140_features)

    # property_affidavit
    property_affidavit_queries: List[List[str]] = [
        ["PROP_AFF_OWNER", "What is your name?"],
        ["PROP_AFF_ADDR", "What is the property's address?"],
        ["PROP_AFF_DATE_EXEC_ON", "When was this executed on?"],
        ["PROP_AFF_DATE_SWORN", "When was this subscribed and sworn to?"],
        ["PROP_AFF_NOTARY", "Who is the notary public?"],
    ]
    property_affidavit_features: List[str] = ["SIGNATURES", "QUERIES"]
    property_affidavit_row = CSVRow("property_affidavit",
                                    property_affidavit_queries,
                                    property_affidavit_features)


    return CSVRow.rows


def write_csv():
    rows = get_csv_rows()

    # WRITE default_config.csv
    with open('default_config.csv', 'w') as f:
        csv_writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(rows)
