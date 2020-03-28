import sys
from controllers.CaricoManager import create_dataframe
from models.CaricoModel import getMaxCaricoNave
from utils.MyLogger import *
import argparse
import pandas as pd
from pyspark.sql import SQLContext, SparkSession
from utils.Costants import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Tesi_Siremar').getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    writeLog(levelLog.INFO, "Main","Inizio computazione dataframe")
    parser = argparse.ArgumentParser(description='Dataframe Creator')
    g = parser.add_mutually_exclusive_group()
    g.add_argument('-pre', help="computazione dei mq occupati in base ai dati rilevati dalle prenotazioni",
                   action='store_true')
    args = parser.parse_args()

    if args.pre:
        try:
            prenotazioni_csv = pd.read_csv("./prenotazioni.csv")
            prenotazioni_csv = prenotazioni_csv.loc[:, ~prenotazioni_csv.columns.str.contains('^Unnamed')]
            df_to_write = sqlContext.createDataFrame(prenotazioni_csv)
            df_to_write.write.parquet(PARQUET_FILE_PRENOTATION)
            writeLog(levelLog.INFO, "main", "Dataset creato correttamente")
        except:
            database_max_mq = getMaxCaricoNave()
            if not database_max_mq.empty:
                DATAFRAME_APPLICATION["dataframe_max_mq_occupati"] = database_max_mq
                writeLog(levelLog.INFO, "main", "dataframe max mq occupati caricato correttamente")
            else:
                writeLog(levelLog.ERROR, "main", "dataframe max mq occupati non trovato")
                sys.exit(1)
            try:
                dataframe_prenotazioni = sqlContext.read.parquet(PARQUET_FILE_PRENOTATION).toPandas()
            except:
                dataframe_prenotazioni = pd.DataFrame()
            if not dataframe_prenotazioni.empty:
                DATAFRAME_APPLICATION["dataframe_prenotazioni"] = dataframe_prenotazioni
                writeLog(levelLog.INFO, "main", "dataframe prenotazioni caricato correttamente. Non devo crearlo")
                sys.exit(1)
            else:
                writeLog(levelLog.INFO, "main", "dataframe prenotazioni non trovato. Lo devo creare")
                dataframe = create_dataframe()
                if not dataframe.empty:
                    dataframe.to_csv("./prenotazioni.csv")
                    pd.to_datetime(dataframe.booking_ticket_departure_timestamp, unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
                    df_to_write = sqlContext.createDataFrame(dataframe)
                    df_to_write.write.parquet(PARQUET_FILE_PRENOTATION)
                    writeLog(levelLog.INFO, "main", "Dataset creato correttamente")
    sys.exit(0)


