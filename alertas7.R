library(ROracle)
library(DBI)
library(readr)
library(dplyr)
library(openair)
library(tidyr)
library(lubridate)
library(jsonlite)

setwd("C:/Users/abrios/Desktop/alerta03/Alertas_ECA")

# idstation <- 11
# parametro <- "SO2"
f1 <- format(Sys.Date()-7, "%Y-%m-%d")
f2 <- format(Sys.Date(), "%Y-%m-%d")

fun_tm <- function (){

  # Parámetros de conexión a Oracle
  host <- "odaprod-scan"
  port <- 1534
  service <- "nexoefa"
  username <- "SHINY"
  password <- "j46wMmcvz0Qp"
  connect.string <- paste0("(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))(CONNECT_DATA=(SERVICE_NAME=", service, ")))")
  
  # Establecimiento de la conexión
  drv <- dbDriver("Oracle")
  connection <- dbConnect(drv, username = username, password = password, dbname = connect.string)
  
  # Construir la consulta SQL completa
  query <- paste0("
SELECT ID_STATION,
       COD_STATION,
       STATION,
       X_PM10,
       X_PM25,
       X_SO2,
       X_H2S,
       X_CO,
       X_NO2,
       X_PBAR,
       X_PP,
       X_TEMP,
       X_HR,
       X_WS,
       X_WD,
       X_RS
FROM VIGAMB.CCA_STATION_DETAIL
WHERE 
  STATUS = 'operativo'
  ORDER BY ID_STATION
")
  
  # cat(query)
  
  bd_tm <- 
    dbGetQuery(connection, query) %>% 
    as_tibble()
  
  dbDisconnect(connection)
  
  return(bd_tm)
  
}

bd_tm <- fun_tm()

fun_bd <- function (idstation, parametro, f1, f2){
  
  # Parámetros de conexión a Oracle
  host <- "odaprod-scan"
  port <- 1534
  service <- "nexoefa"
  username <- "SHINY"
  password <- "j46wMmcvz0Qp"
  connect.string <- paste0("(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))(CONNECT_DATA=(SERVICE_NAME=", service, ")))")
  
  # Establecimiento de la conexión
  drv <- dbDriver("Oracle")
  connection <- dbConnect(drv, username = username, password = password, dbname = connect.string)
  
  # Construir la consulta SQL completa
  query <- paste0("
SELECT
    ID_STATION,
    COD_STATION,
    FECHA,
    CASE '", parametro, "' 
    WHEN 'PM10' THEN \"PM10\"
    WHEN 'PM25' THEN \"PM25\"
    WHEN 'SO2' THEN \"SO2\"
    WHEN 'H2S' THEN \"H2S\"
    WHEN 'CO' THEN \"CO\"
    WHEN 'NO2' THEN \"NO2\"
    WHEN 'PBAR' THEN \"PBAR\"
    WHEN 'PP' THEN \"PP\"
    WHEN 'TEMP' THEN \"TEMP\"
    WHEN 'HR' THEN \"HR\"
    WHEN 'WS' THEN \"WS\"
    WHEN 'WD' THEN \"WD\"
    WHEN 'RS' THEN \"RS\"
    END AS \"", parametro, "\"
FROM (
    SELECT
        CSD.ID_STATION,
        CSD.COD_STATION,
        TO_CHAR(TO_DATE(VIG.FECHA_DATA_LOGER || ' ' || VIG.HORA_DATA_LOGER, 'MM/DD/YYYY HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') || ' UTC' AS \"FECHA\",
        CASE WHEN CSD.X_PM10 = 0 THEN NULL ELSE ROUND(VIG.PM10_CONC * CSD.C_PM10, 1) END AS \"PM10\",
        CASE WHEN CSD.X_PM25 = 0 THEN NULL ELSE ROUND(VIG.PM25_CONC * CSD.C_PM25, 1) END AS \"PM25\",
        CASE WHEN CSD.X_SO2 = 0 THEN NULL ELSE ROUND(VIG.SO2_CONC * CSD.C_SO2, 2) END AS \"SO2\",
        CASE WHEN CSD.X_H2S = 0 THEN NULL ELSE ROUND(VIG.H2S_CONC * CSD.C_H2S, 2) END AS \"H2S\",
        CASE WHEN CSD.X_CO = 0 THEN NULL ELSE ROUND(VIG.CO_CONC * CSD.C_CO, 2) END AS \"CO\",
        CASE WHEN CSD.X_NO2 = 0 THEN NULL ELSE ROUND(VIG.NO2_CONC * CSD.C_NO2, 2) END AS \"NO2\",
        CASE WHEN CSD.X_PBAR = 0 THEN NULL ELSE ROUND(VIG.PBAR * CSD.C_PBAR, 1) END AS \"PBAR\",
        CASE WHEN CSD.X_PP = 0 THEN NULL ELSE ROUND(VIG.PP * CSD.C_PP, 1) END AS \"PP\",
        CASE WHEN CSD.X_TEMP = 0 THEN NULL ELSE ROUND(VIG.TEMP * CSD.C_TEMP, 1) END AS \"TEMP\",
        CASE WHEN CSD.X_HR = 0 THEN NULL ELSE ROUND(VIG.HR * CSD.C_HR, 1) END AS \"HR\",
        CASE WHEN CSD.X_WS = 0 THEN NULL ELSE ROUND(VIG.WS * CSD.C_WS, 1) END AS \"WS\",
        CASE WHEN CSD.X_WD = 0 THEN NULL ELSE ROUND(VIG.WD * CSD.C_WD, 1) END AS \"WD\",
        NULL AS \"RS\"
    FROM
        VIGAMB.VIGAMB_TRAMA_AIRE VIG
    JOIN
        VIGAMB.CCA_STATION_DETAIL CSD ON VIG.ID_ESTACION = CSD.ID_STATION
    WHERE
        CSD.ID_STATION = ", idstation, " AND
        CSD.ID_STATION IN (2, 4, 5, 7, 9, 38, 39, 40, 41, 42, 19, 22, 23, 24, 25, 26, 27, 28, 29, 32, 33, 34, 36, 37) AND
        CSD.STATUS = 'operativo' AND
        TO_DATE(VIG.FECHA_DATA_LOGER, 'MM/DD/YYYY') BETWEEN TO_DATE('", f1, "', 'YYYY-MM-DD') AND TO_DATE('", f2, "', 'YYYY-MM-DD')
    UNION ALL
    SELECT
        CSD.ID_STATION,
        CSD.COD_STATION,
        TO_CHAR(TO_DATE(SIS.DES_FECHA_DATA || ' ' || SIS.DES_HORA_DATA, 'DD/MM/YYYY HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') || ' UTC' AS \"FECHA\",
        CASE WHEN CSD.X_PM10 = 0 THEN NULL ELSE ROUND(PM10.DBL_FD17 * CSD.C_PM10, 1) END AS \"PM10\",
        CASE WHEN CSD.X_PM25 = 0 THEN NULL ELSE ROUND(PM25.DBL_FD32 * CSD.C_PM25, 1) END AS \"PM25\",
        CASE WHEN CSD.X_SO2 = 0 THEN NULL ELSE ROUND(SO2.DBL_FD47 * CSD.C_SO2, 2) END AS \"SO2\",
        CASE WHEN CSD.X_H2S = 0 THEN NULL ELSE ROUND(H2S.DBL_FD62 * CSD.C_H2S, 2) END AS \"H2S\",
        CASE WHEN CSD.X_CO = 0 THEN NULL ELSE ROUND(CO.DBL_FD77 * CSD.C_CO, 2) END AS \"CO\",
        CASE WHEN CSD.X_NO2 = 0 THEN NULL ELSE ROUND(NO2.DBL_FD92 * CSD.C_NO2, 2) END AS \"NO2\",
        CASE WHEN CSD.X_PBAR = 0 THEN NULL ELSE ROUND(MET.DBL_FD2 * CSD.C_PBAR, 1) END AS \"PBAR\",
        CASE WHEN CSD.X_PP = 0 THEN NULL ELSE ROUND(MET.DBL_FD3 * CSD.C_PP, 1) END AS \"PP\",
        CASE WHEN CSD.X_TEMP = 0 THEN NULL ELSE ROUND(MET.DBL_FD4 * CSD.C_TEMP, 1) END AS \"TEMP\",
        CASE WHEN CSD.X_HR = 0 THEN NULL ELSE ROUND(MET.DBL_FD5 * CSD.C_HR, 1) END AS \"HR\",
        CASE WHEN CSD.X_WS = 0 THEN NULL ELSE ROUND(MET.DBL_FD6 * CSD.C_WS, 1) END AS \"WS\",
        CASE WHEN CSD.X_WD = 0 THEN NULL ELSE ROUND(MET.DBL_FD7 * CSD.C_WD, 1) END AS \"WD\",
        CASE WHEN CSD.X_RS = 0 THEN NULL ELSE ROUND(MET.DBL_FD8 * CSD.C_RS, 1) END AS \"RS\"
    FROM
        VIGAMB.CCA_STATION_DETAIL CSD
    JOIN
        VIGAMB.VIGAMB_T_AIRE_CABECERA SIS ON CSD.ID_STATION = SIS.ID_ESTACION
    JOIN
        VIGAMB.VIGAMB_T_AIRE_METEREOLOGIA MET ON MET.ID_CORRELATIVO = SIS.ID_CORRELATIVO
    JOIN
        VIGAMB.VIGAMB_T_AIRE_PM10 PM10 ON PM10.ID_CORRELATIVO = SIS.ID_CORRELATIVO
    JOIN
        VIGAMB.VIGAMB_T_AIRE_PM25 PM25 ON PM25.ID_CORRELATIVO = SIS.ID_CORRELATIVO
    JOIN
        VIGAMB.VIGAMB_T_AIRE_SO2 SO2 ON SO2.ID_CORRELATIVO = SIS.ID_CORRELATIVO
    JOIN
        VIGAMB.VIGAMB_T_AIRE_H2S H2S ON H2S.ID_CORRELATIVO = SIS.ID_CORRELATIVO
    JOIN
        VIGAMB.VIGAMB_T_AIRE_CO CO ON CO.ID_CORRELATIVO = SIS.ID_CORRELATIVO
    JOIN
        VIGAMB.VIGAMB_T_AIRE_NO2 NO2 ON NO2.ID_CORRELATIVO = SIS.ID_CORRELATIVO
    WHERE
        CSD.ID_STATION = ", idstation, " AND
        CSD.ID_STATION IN (10, 11, 12, 13, 47, 48, 49, 50, 51, 52) AND
        CSD.STATUS = 'operativo' AND
        TO_DATE(SIS.DES_FECHA_DATA, 'DD/MM/YYYY') BETWEEN TO_DATE('", f1, "', 'YYYY-MM-DD') AND TO_DATE('", f2, "', 'YYYY-MM-DD')
) ")
  
  # cat(query)
  
  bd_5m <- 
    dbGetQuery(connection, query)
  
  dbDisconnect(connection)
  
  if (nrow(bd_5m) == 0) {
    
    return(list(
      bd_5m = NULL,
      bd_1h = NULL,
      bd_24h = NULL
    ))
  
  }else{
  
bd_5m <- 
  bd_5m %>%
  mutate(date = ymd_hms(FECHA)) %>%
  select(date, everything(), -ID_STATION, -COD_STATION, -FECHA) %>% 
  arrange(date) %>%
  complete(date = seq(
    from = ymd_hms(paste(as.Date(min(date, na.rm = TRUE)), hour(min(date, na.rm = TRUE)), ":00:00")),
    to = ymd_hms(paste(as.Date(max(date, na.rm = TRUE)), hour(max(date, na.rm = TRUE)), ":55:00")),
    by = "5 mins"
  )) %>% 
  mutate(
    !!parametro := if_else(
      (parametro == "TEMP" & .data[[parametro]] <= -30) |
      (parametro %in% c("HR", "WS", "WD", "RS") & .data[[parametro]] < 0) |
      (parametro %in% c("PM10", "PM25", "SO2", "H2S", "CO", "NO2", "PBAR", "PP") & .data[[parametro]] <= 0),
      NA_real_,
      .data[[parametro]]
    )
  )

  hoy <- as.POSIXct(format(Sys.time()), tz = "UTC")
  fecha_max <- floor_date(hoy, "1 day") - days(1)
  tiempo_max <- floor_date(hoy, "1 hour") - hours(1)
  
  if(parametro == "PP"){

    bd_1h <- 
      bd_5m %>%
      timeAverage(mydata = ., avg.time = "1 hour", data.thresh = 75, statistic = "sum") %>% 
      mutate(PP = round(PP, 1))
  
  }else{
    
    bd_1h <- 
      bd_5m %>%
      timeAverage(mydata = ., avg.time = "1 hour", data.thresh = 75, statistic = "mean") %>%
      mutate(
        !!parametro := if (parametro %in% c("SO2", "CO", "NO2", "H2S")) {
          round(.data[[parametro]], 2)
        } else {
          round(.data[[parametro]], 1)
        }
      )
  
  }
  
  if(idstation == 2 && parametro == "SO2"){
    
    bd_1h <- 
      bd_1h %>%
      rollingMean(., pollutant = "SO2", new.name = "SO2_M3H", width = 3, data.thresh = 75, align = "right") %>%
      mutate(SO2_M3H = round(SO2_M3H, 2)) %>% 
      mutate(A_SO2_M3H = case_when(
        SO2_M3H >= 500 & SO2_M3H < 1500 ~ 1L, 
        SO2_M3H >= 1500 & SO2_M3H <= 2500 ~ 1L, 
        SO2_M3H > 2500 ~ 1L,
        SO2_M3H < 500 ~ 0L,
        TRUE ~ NA_integer_
      ))

  }
  
  if(idstation %in% c(10, 11, 12, 50, 51) && parametro == "CO"){
    
    bd_1h <- 
      bd_1h %>%
      rollingMean(., pollutant = "CO", new.name = "CO_M8H", width = 8, data.thresh = 75, align = "right") %>%
      mutate(CO_M8H = round(CO_M8H, 2)) %>% 
      mutate(
        A_CO = case_when(CO >= 30000 ~ 1L, CO < 30000 ~ 0L, TRUE ~ NA_integer_),
        A_CO_M8H = case_when(CO_M8H >= 10000 ~ 1L, CO_M8H < 10000 ~ 0L, TRUE ~ NA_integer_)
      )

  }
  
  eca <- c(PM10 = 100, PM25 = 50, SO2 = 250, H2S = 150)
  
  if(parametro %in% names(eca)){
    
    bd_24h <-
      bd_1h %>%
      select(date, !!parametro) %>% 
      complete(
        date = seq(
          from = ymd_hms(paste(as.Date(min(date, na.rm = T)),"00:00:00")),
          to = ymd_hms(paste(as.Date(max(date, na.rm = T)),"23:00:00")),
          by = "1 hour"
        )
      ) %>% 
      timeAverage(mydata = ., avg.time = "1 day", data.thresh = 75, statistic = "mean") %>%
      mutate(
        !!parametro := if (parametro %in% names(eca)) {
          round(.data[[parametro]], 2)
        } else {
          round(.data[[parametro]], 1)
        }
      ) %>% 
      mutate(date = as.Date(date, format = "%Y-%m-%d")) %>% 
      mutate(!!paste0("A_", parametro) := case_when(
        .data[[parametro]] >= eca[[parametro]] ~ 1L,
        .data[[parametro]] < eca[[parametro]] ~ 0L,
        TRUE ~ NA_integer_
      ))
    
  }else{
    
    bd_24h <- NULL
    
  }

  return(
    list(
      bd_5m = bd_5m %>% drop_na(!!parametro) %>% slice(n()),
      bd_1h = bd_1h %>% filter(date == tiempo_max),
      bd_24h = if (!is.null(bd_24h)) { bd_24h %>% filter(date == fecha_max) } else { bd_24h }
      )
    )
  
  }
  
}

vec_idsta <- bd_tm$ID_STATION

ruta_python <- "C:\\Users\\abrios\\Desktop\\alerta03\\Alertas_ECA\\.venv\\Scripts\\python.exe"
ruta_script <- "C:\\Users\\abrios\\Desktop\\alerta03\\Alertas_ECA\\script.py"

ultima_ejecucion <- readLines("ultima_ejecucion.txt", n = 1, warn = FALSE)

for (i in vec_idsta) {
  
  print("////////////////////////////////////////////////")
  
  print(i)
  
  vec_par <- bd_tm %>% filter(ID_STATION == i) %>% select(starts_with("X_")) %>% gather(Variable, Valor) %>% filter(Valor == 1) %>% pull(Variable) %>% sub("^X_", "", .)
  
  for(j in vec_par) {
    
    print(j)
    
    print("------------------------------------------------")
    
    if((i == 2 && j == "SO2") || (i %in% c(10, 11, 12, 50, 51) && j == "CO")){

      list_bd <- fun_bd(idstation = i, parametro = j, f1, f2)

      bd_1h <- list_bd$bd_1h

      if (!is.null(bd_1h) && nrow(bd_1h) == 1) {

        col_aler_1h <- bd_1h %>% select(starts_with("A_")) %>% names(.)

        for (k in col_aler_1h) {

          if (!is.na(bd_1h[[k]]) && (bd_1h %>% select(!!sym(k)) %>% pull() == 1)) {

            arg1 <- bd_1h %>% select(date, !!gsub("^A_", "", k)) %>% toJSON() %>% gsub("^\\[|\\]$", "", .) %>% gsub("\"", "#", .) %>% gsub(" ", "$", .)
            arg2 <- bd_tm %>% filter(ID_STATION == i) %>% pull(COD_STATION) %>% shQuote()

            print(arg1)
            print(arg2)

            system2(ruta_python, args = c(ruta_script, arg1, arg2))

          }

        }

      }

    }

    if (!exists("ultima_ejecucion") || as.Date(Sys.Date()) != as.Date(ultima_ejecucion)) {

      if(j %in% c("PM10", "PM25", "SO2", "H2S")) {

      list_bd <- fun_bd(idstation = i, parametro = j, f1, f2)

      bd_24h <- list_bd$bd_24h

      if (!is.null(bd_24h) && nrow(bd_24h) == 1) {

        col_aler_24h <- bd_24h %>% select(starts_with("A_")) %>% names(.)

        for (k in col_aler_24h) {

          if (!is.na(bd_24h[[k]]) && (bd_24h %>% select(!!sym(k)) %>% pull() == 1)) {

            arg1 <- bd_24h %>% select(date, !!gsub("^A_", "", k)) %>% toJSON() %>% gsub("^\\[|\\]$", "", .) %>% gsub("\"", "#", .) %>% gsub(" ", "$", .)
            arg2 <- bd_tm %>% filter(ID_STATION == i) %>% pull(COD_STATION) %>% shQuote()

            print(arg1)
            print(arg2)

            system2(ruta_python, args = c(ruta_script, arg1, arg2))

          }

        }

      }

      }

      writeLines(as.character(as.Date(Sys.Date())), "ultima_ejecucion.txt")

    }

    print("------------------------------------------------")

  }
  
}
