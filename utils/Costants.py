import enum

DFS_ROOT = "hdfs://localhost:9000/"
PARQUET_FILE_CARGO = DFS_ROOT + "tesi_siremar/siremar_cargo.parquet"
PARQUET_FILE_PRENOTATION = DFS_ROOT + "tesi_siremar/siremar_prenotation.parquet"
ASSETS = "./assets/"
DATAFRAME_APPLICATION = {
    "dataframe_prenotazioni": None,
    "dataframe_cargo": None,
    "dataframe_max_mq_occupati": None,
    "dataframe_tot_mq_occupati": None
}

class levelLog(enum.Enum):
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4