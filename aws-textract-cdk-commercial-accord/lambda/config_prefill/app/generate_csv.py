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
        self.document_type: str = self.create_document_type()
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

    def create_document_type(self):
        if self.page_number:
            document_type = f"{self.classification}_page{self.page_number}"
        else:
            document_type = f"{self.classification}_default"
        return document_type

    def get_csv_row(self):
        return [self.document_type, tm.IDPManifestSchema().dumps(self.manifest)]


def get_csv_rows():
    # acord125
    acord125_default_queries: List[List[str]] = list()
    acord_125_default_features: List[str] = ["FORMS", "TABLES"]
    acord125_default_row = CSVRow("acord125",
                                  acord125_default_queries,
                                  acord_125_default_features)

    # acord126
    acord126_page1_queries: List[List[str]] = [
        ["ACORD_126_DATE", "What is the date?"],
        ["ACORD_126_EFFEC_DATE", "What is the effective date?"],
        ["ACORD_126_APPLICANT", "Who is the applicant?"],
        ["ACORD_126_GEN_AGG", "What is the general aggregate?"],
        ["ACORD_126_COMMER_GEN_LIAB", "What is the commercial general liability?"],
        ["ACORD_126_PROD_OPS_AGG", "What is the products and completed operations aggregate?"],
        ["ACORD_126_PERS_ADV_INJ", "What is the personal and advertising injury?"],
        ["ACORD_126_EACH_OCCUR", "What is each occurrence?"],
        ["ACORD_126_DMG_RENT_PREM", "What is the damage to rented premises (each occurrence)?"],
        ["ACORD_126_MED_EXP", "What is the medical expense (any one person)?"]
    ]
    acord126_page1_features: List[str] = ["FORMS", "TABLES", "QUERIES"]
    acord126_page1_row = CSVRow("acord126",
                                  acord126_page1_queries,
                                  acord126_page1_features,
                                  1)
    acord126_page3_queries: List[List[str]] = [
        ["ACORD_126_ITEM_DESC", "What is the item description?"],
    ]
    acord126_page3_features: List[str] = ["FORMS", "TABLES", "QUERIES"]
    acord126_page3_row = CSVRow("acord126",
                                acord126_page3_queries,
                                acord126_page3_features,
                                3)

    acord126_default_queries: List[List[str]] = list()
    acord126_default_features: List[str] = ["FORMS", "TABLES"]
    acord126_default_row = CSVRow("acord126",
                                  acord126_default_queries,
                                  acord126_default_features)

    # acord140
    acord140_page1_queries: List[List[str]] = [
        ["ACORD_140_DATE", "What is the date?"],
        ["ACORD_140_EFF_DATE", "What is the effective date?"],
        ["ACORD_140_NAMED_INSUREDS", "What are the named insured(s)?"],
        ["ACORD_140_PREM_NUM", "What is the premises #?"],
        ["ACORD_140_BLDG_NUM", "What is the building #?"],
        ["ACORD_140_STREET_ADDR", "What is the street address?"],
        ["ACORD_140_BLDG_DESC", "What is the bldg description?"],
        ["ACORD_140_CONSTR_TYPE", "What is the construction type?"],
        ["ACORD_140_NUM_STORIES", "What is the # stories?"],
        ["ACORD_140_NUM_BASMTS", "What is the # basm'ts?"],
        ["ACORD_140_YR_BUILT", "What is the yr built?"],
        ["ACORD_140_TOTAL_AREA", "What is the total area?"],
        ["ACORD_140_PAGE1_REMARKS", "What are the remarks?"]
    ]
    acord140_page1_features: List[str] = ["FORMS", "TABLES", "QUERIES"]
    acord140_page1_row = CSVRow("acord140",
                                acord140_page1_queries,
                                acord140_page1_features,
                                1)

    acord140_page2_queries: List[List[str]] = [
        ["ACORD_140_PAGE2_REMARKS", "What are the remarks?"]
    ]
    acord140_page2_features: List[str] = ["FORMS", "TABLES", "QUERIES"]
    acord140_page2_row = CSVRow("acord140",
                                acord140_page2_queries,
                                acord140_page2_features,
                                2)

    acord140_page3_queries: List[List[str]] = [
        ["ACORD_140_PAGE3_REMARKS", "What are the remarks?"]
    ]
    acord140_page3_features: List[str] = ["FORMS", "TABLES", "QUERIES"]
    acord140_page3_row = CSVRow("acord140",
                                acord140_page3_queries,
                                acord140_page3_features,
                                3)

    # property_affidavit
    property_affidavit_default_queries: List[List[str]] = [
        ["PROP_AFF_OWNER", "What is your name?"],
        ["PROP_AFF_ADDR", "What is the property's address?"],
        ["PROP_AFF_DATE_EXEC_ON", "When was this executed on?"],
        ["PROP_AFF_DATE_SWORN", "When was this subscribed and sworn to?"],
        ["PROP_AFF_NOTARY", "Who is the notary public?"],
    ]
    property_affidavit_default_features: List[str] = ["SIGNATURES", "QUERIES"]
    property_affidavit_default_row = CSVRow("property_affidavit",
                                  property_affidavit_default_queries,
                                  property_affidavit_default_features)

    return CSVRow.rows


def write_csv():
    rows = get_csv_rows()

    # WRITE default_config.csv
    with open('default_config.csv', 'w') as f:
        csv_writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(rows)

