import sqlite3
import os

LOOKUPS_DIR=os.path.dirname(os.path.abspath(__file__))
DRUGBANK_PATH=os.path.join(LOOKUPS_DIR, "drugbank.db")
CHEMBL_PATH=os.path.join(LOOKUPS_DIR, "chembl.db")

DRUGBANK_VERSION="DrugBank-5.1.12"
CHEMBL_VERSION="ChEMBL-34"

SAMPLE_DRUGBANK_DRUGS=[
    ("DB00945", "aspirin", "COX1|COX2", "acetylsalicylic acid|asa", "Bayer Aspirin|Ecotrin"),
    ("DB01050", "ibuprofen", "COX1|COX2|PTGS1", "ibuprofen|2-methylpropanoic acid", "Advil|Motrin"),
    ("DB00619", "imatinib", "BCR-ABL|KIT|PDGFRA", "imatinib|cgp57148b", "Gleevec|Glivec"),
    ("DB00877", "sirolimus", "FKBP12|MTOR", "rapamycin", "Rapamune"),
    ("DB01234", "dexamethasone", "NR3C1|ANXA1", "dexamethasone sodium phosphate", "Decadron|Maxidex"),
    ("DB00563", "methotrexate", "DHFR|TYMS", "amethopterin|mtx", "Trexall|Otrexup"),
    ("DB00173", "adalimumab", "TNF", "adalimumab-adbm", "Humira"),
    ("DB00398", "sorafenib", "VEGFR2|PDGFR|RAF1|BRAF", "bay43-9006", "Nexavar"),
    ("DB01592", "iron", "TFRC|FTH1", "ferrous sulfate|ferric", "Feosol"),
    ("DB00316", "acetaminophen", "COX3|PTGS1", "paracetamol|apap", "Tylenol|Panadol"),
    ("DB00741", "hydrocortisone", "NR3C1", "cortisol", "Solu-Cortef"),
    ("DB00091", "cyclosporine", "PPIA|PPP3CA", "ciclosporin|cyclosporin a", "Sandimmune|Neoral"),
    ("DB01183", "erlotinib", "EGFR", "osl-774|cp-358774", "Tarceva"),
    ("DB01248", "docetaxel", "TUBB|TUBB1", "taxotere", "Taxotere"),
    ("DB01229", "paclitaxel", "TUBB|TUBB3", "taxol", "Abraxane|Taxol"),
]

SAMPLE_CHEMBL_DRUGS=[
    ("CHEMBL25", "aspirin", "PTGS1|PTGS2", "acetylsalicylic acid"),
    ("CHEMBL941", "imatinib", "ABL1|KIT|PDGFRA|PDGFRB", "gleevec"),
    ("CHEMBL553", "sorafenib", "VEGFR1|VEGFR2|BRAF|RAF1", "nexavar"),
    ("CHEMBL1201585", "adalimumab", "TNF", "humira"),
    ("CHEMBL374478", "sirolimus", "MTOR", "rapamycin"),
    ("CHEMBL667", "methotrexate", "DHFR|ATIC", "amethopterin"),
    ("CHEMBL1229", "docetaxel", "TUBB|TUBB2A|TUBB3", "taxotere"),
    ("CHEMBL428690", "erlotinib", "EGFR|ERBB2", "tarceva"),
    ("CHEMBL1201576", "bevacizumab", "VEGFA", "avastin"),
    ("CHEMBL1743", "dexamethasone", "NR3C1|NR3C2", "decadron"),
]

def create_drugbank_db():
    conn=sqlite3.connect(DRUGBANK_PATH)
    c=conn.cursor()
    c.execute("DROP TABLE IF EXISTS drugs")
    c.execute("""
        CREATE TABLE drugs (
            drug_id TEXT PRIMARY KEY,
            common_name TEXT NOT NULL,
            targets TEXT,
            synonyms TEXT,
            brand_names TEXT,
            db_version TEXT
        )
    """)
    for row in SAMPLE_DRUGBANK_DRUGS:
        c.execute("INSERT INTO drugs VALUES (?,?,?,?,?,?)", tuple(row)+(DRUGBANK_VERSION,))
    conn.commit()
    conn.close()
    print(f"DrugBank sample DB created at {DRUGBANK_PATH}")
    print(f"PLACEHOLDER: Replace {DRUGBANK_PATH} with real DrugBank SQLite export")

def create_chembl_db():
    conn=sqlite3.connect(CHEMBL_PATH)
    c=conn.cursor()
    c.execute("DROP TABLE IF EXISTS drugs")
    c.execute("""
        CREATE TABLE drugs (
            chembl_id TEXT PRIMARY KEY,
            common_name TEXT NOT NULL,
            targets TEXT,
            synonyms TEXT,
            db_version TEXT
        )
    """)
    for row in SAMPLE_CHEMBL_DRUGS:
        c.execute("INSERT INTO drugs VALUES (?,?,?,?,?)", tuple(row)+(CHEMBL_VERSION,))
    conn.commit()
    conn.close()
    print(f"ChEMBL sample DB created at {CHEMBL_PATH}")
    print(f"PLACEHOLDER: Replace {CHEMBL_PATH} with real ChEMBL SQLite export")

if __name__=="__main__":
    create_drugbank_db()
    create_chembl_db()
    print("Sample lookup databases created.")
    print("PLACEHOLDER: Expected DrugBank table schema: drug_id, common_name, targets (pipe-delimited), synonyms (pipe-delimited), brand_names (pipe-delimited), db_version")
    print("PLACEHOLDER: Expected ChEMBL table schema: chembl_id, common_name, targets (pipe-delimited), synonyms (pipe-delimited), db_version")
