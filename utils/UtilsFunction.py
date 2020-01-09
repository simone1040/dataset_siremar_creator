import pandas as pd


#Restituisce il dataframe che utilizziamo per stampare le informazioni
def get_dataframe_data_to_show():
    return pd.DataFrame(columns=["booking_ticket_departure_timestamp", "ship_code", "departure_port_name", "arrival_port_name",
                 "metri_garage_navi_spazio_totale","tot_mq_occupati"])