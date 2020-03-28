from utils.SQLManager import SQLManager

def getMaxCaricoNave():
    sql = "SELECT tab_metri_garage_navi.*,ship_name,ship_code " \
          "FROM tab_metri_garage_navi " \
          "INNER JOIN tab_ship on tab_metri_garage_navi.ship_id = tab_ship.ship_id " \
          "order by ship_id"
    df = SQLManager.get_istance().execute_query(string_sql=sql)
    return df

def get_dati_prenotazioni():
    sql = "SELECT `booking_ticket_departure_timestamp`,`ship_code`,`ship_id`, `tab_port_departure`.`port_name` AS `departure_port_name`, `tab_port_arrival`.`port_name` AS `arrival_port_name`, " \
      "tab_boardingcard_category.boardingcard_category_code,tab_boardingcard_category.boardingcard_category_name,tab_ticket.ticket_trip_code,tab_ticket.ticket_departure_route_code,tab_ticket.ticket_arrival_route_code " \
      "FROM `tab_booking_ticket` " \
      "LEFT JOIN `tab_ship` ON `tab_ship`.`ship_code` = `tab_booking_ticket`.`booking_ticket_ship_code`" \
      "LEFT JOIN `tab_itinerary` ON `tab_itinerary`.`itinerary_code` = `tab_booking_ticket`.`booking_ticket_itinerary_code`" \
      "LEFT JOIN `tab_port` AS `tab_port_departure` ON `tab_port_departure`.`port_code` = `tab_booking_ticket`.`booking_ticket_departure_port_code`" \
      "LEFT JOIN `tab_port` AS `tab_port_arrival` ON `tab_port_arrival`.`port_code` = `tab_booking_ticket`.`booking_ticket_arrival_port_code`" \
      "LEFT JOIN `tab_booking_vehicle` ON `tab_booking_vehicle`.`booking_vehicle_booking_boardingcard_id` = `tab_booking_ticket`.`booking_ticket_id`" \
      "LEFT JOIN `tab_booking_accessory` ON `tab_booking_accessory`.`booking_accessory_booking_boardingcard_id` = `tab_booking_ticket`.`booking_ticket_id`" \
      "INNER JOIN `tab_boardingcard_category` ON `tab_boardingcard_category`.`boardingcard_category_code` = `tab_booking_vehicle`.`booking_vehicle_category_code` OR `tab_boardingcard_category`.`boardingcard_category_code` = `tab_booking_accessory`.`booking_accessory_category_code`" \
      "INNER JOIN `tab_ticket` ON `tab_ticket`.`ticket_number` = `tab_booking_ticket`.`booking_ticket_number` " \
      "WHERE booking_ticket_status = 'F' AND DATE(booking_ticket_departure_timestamp)<= '2019-12-01' ORDER BY booking_ticket_departure_timestamp"
    df = SQLManager.get_istance().execute_query(string_sql=sql)
    return df

def get_distinct_tratte():
    sql = "SELECT booking_ticket_departure_port_code,booking_ticket_arrival_port_code " \
          "FROM tab_booking_ticket " \
          "GROUP BY booking_ticket_departure_port_code,booking_ticket_arrival_port_code "
    df = SQLManager.get_istance().execute_query(string_sql=sql)
    return df

def get_distinct_ship():
    sql = "SELECT ship_code,ship_name " \
          "FROM tab_ship " \
          "ORDER BY ship_name"
    df = SQLManager.get_istance().execute_query(string_sql=sql)
    return df

def get_ship_name(ship_code):
    sql = "SELECT ship_name " \
          "FROM tab_ship " \
          "WHERE ship_code='{}'" \
          "ORDER BY ship_code".format(ship_code)
    df = SQLManager.get_istance().execute_query(string_sql=sql)
    if not df.empty:
        return df["ship_name"].iloc[0]
    else:
        return ship_code

def from_port_code_get_name(port_code):
    sql = "SELECT port_name " \
          "FROM tab_port " \
          "WHERE port_code='{}' ".format(port_code)
    df = SQLManager.get_istance().execute_query(string_sql=sql)
    port_name = df["port_name"].iloc[0]
    return port_name

#Tramite la route Cappelli prendo indistintamente solo i primi viaggi per il range selezionato
def get_distinct_first_trip():
    sql = "SELECT route_cappelli_trip_code,route_cappelli_departure_timestamp,route_cappelli_route_code,route_cappelli_next_route_code," \
          "route_cappelli_departure_port_code,route_cappelli_arrival_port_code,route_cappelli_ship_code " \
          "FROM tab_route_cappelli " \
          "WHERE route_cappelli_progressive = 1 ORDER BY route_cappelli_departure_timestamp"
    df = SQLManager.get_istance().execute_query(string_sql=sql)
    return df

def get_trip_from_route_code(route_code):
    sql = "SELECT route_cappelli_trip_code,route_cappelli_departure_timestamp,route_cappelli_route_code,route_cappelli_next_route_code," \
          "route_cappelli_departure_port_code,route_cappelli_arrival_port_code,route_cappelli_ship_code " \
          "FROM tab_route_cappelli " \
          "WHERE route_cappelli_route_code = '{}' ".format(route_code)
    df = SQLManager.get_istance().execute_query(string_sql=sql)
    return df.iloc[0]

def get_all_bording_category_mq_occupati():
    sql = "SELECT boardingcard_category_code,(boardingcard_category_lunghezza * boardingcard_category_larghezza + boardingcard_category_delta_larghezza) AS mq_occupati,boardingcard_category_s18 " \
      "FROM tab_boardingcard_category"
    df = SQLManager.get_istance().execute_query(string_sql=sql)
    return df