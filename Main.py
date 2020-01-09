import sys
from controllers.CaricoManager import create_dataframe
from models.CaricoModel import getMaxCaricoNave
from utils.MyLogger import *
import argparse
from pyspark.shell import spark
from pyspark.sql import SQLContext
from utils.Costants import *
if __name__ == "__main__":
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    writeLog(levelLog.INFO, "Main","Inizio computazione dataframe")
    parser = argparse.ArgumentParser(description='Dataframe Creator')
    g = parser.add_mutually_exclusive_group()
    g.add_argument('-pre', help="computazione dei mq occupati in base ai dati rilevati dalle prenotazioni",
                   action='store_true')
    args = parser.parse_args()

    if args.pre:
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
            dataframe_prenotazioni = None
        if not dataframe_prenotazioni == None:
            DATAFRAME_APPLICATION["dataframe_prenotazioni"] = dataframe_prenotazioni
            writeLog(levelLog.INFO, "main", "dataframe prenotazioni caricato correttamente. Non devo crearlo")
            sys.exit(1)
        else:
            writeLog(levelLog.INFO, "main", "dataframe prenotazioni non trovato. Lo devo creare")
            dataframe = create_dataframe()
            if not dataframe.empty:
                writeLog(levelLog.INFO, "main", "Dataset creato correttamente")
                dataframe.write.parquet(PARQUET_FILE_PRENOTATION)
                sys.exit(0)
            print(dataframe.head(10))


