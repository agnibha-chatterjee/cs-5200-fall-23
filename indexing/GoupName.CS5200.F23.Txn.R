library(RMySQL)
library(DBI)
dbConn <- NULL

main <- function() {
  txnFailed <- FALSE
  
  connectToDb <- function() {
    db.user <- 'admin'      
    db.password <- 'omagarwal86'
    db.name <- 'dbbirdstrikes'
    db.host <- 'dbms-practicum-1.ckaxw7lvptoz.us-east-2.rds.amazonaws.com'
    db.port <- 3306
    
    dbConn <<-
      dbConnect(
        RMySQL::MySQL(),
        user = db.user,
        password = db.password,
        dbname = db.name,
        host = db.host,
        port = db.port
      )
  }
  
  checkIfAirportExists <- function(df.row) {
    airport <- df.row[, 'airport']
    origin <- df.row[, 'origin']
    sqlQuery <- sprintf(
      "
                  SELECT aid
                  FROM airports
                  WHERE airportName = '%s' AND airportState = '%s'
                  LIMIT 1;
                 ",
      airport,
      origin
    )
    result <- dbGetQuery(dbConn, sqlQuery)
    return(result[, 'aid'])
  }
  
  insertIntoAirport <- function(df.row) {
    airport <- df.row[, 'airport']
    origin <- df.row[, 'origin']
    sqlQuery <- sprintf(
      "
                  INSERT INTO airports(airportName, airportState)
                    VALUES ('%s', '%s');
                 ",
      airport,
      origin
    )
    ps <- dbSendStatement(dbConn, sqlQuery)
    
    if (dbGetRowsAffected(ps) < 1) {
      txnFailed <- TRUE
    }
    
    dbClearResult(ps)
    
    sqlQuery <-  "
                  SELECT MAX(aid) as aid
                  FROM airports;
                 "
    result <- dbGetQuery(dbConn, sqlQuery)
    return(result[, 'aid'])
  }
  
  cleanDate <- function(date) {
    split.timestamp <- strsplit(date, " ")
    split.date <- unlist(strsplit(split.timestamp[[1]][1], '/'))
    split.date[1:2] <-
      sprintf("%02d", as.integer(split.date[1:2]))
    return(paste0(split.date[3], "-", split.date[1], "-", split.date[2]))
  }
  
  checkIfFlightExists <- function(df.row) {
    date <- cleanDate(df.row[, "flight_date"])
    airline <- gsub("*", "", df.row[, "airline"])
    aircraft <- df.row[, "aircraft"]
    altitude <- as.integer(gsub(",", "", df.row[, "altitude_ft"]))
    heavy <- !grepl("Unknown|No", df.row[, "heavy_flag"])
    sqlQuery <- sprintf(
      "
                SELECT fid
                FROM flights f
                WHERE f.date = '%s'
                    AND LOWER(f.airline) = LOWER('%s')
                    AND LOWER(f.aircraft) = LOWER('%s')
                    AND f.altitude = %d
                    AND f.HEAVY = %s
                LIMIT 1;
                ",
      date,
      airline,
      aircraft,
      altitude,
      heavy
    )
    result <- dbGetQuery(dbConn, sqlQuery)
    sqlQuery <-  "
                  SELECT MAX(fid) as fid
                  FROM flights;
                 "
    result <- dbGetQuery(dbConn, sqlQuery)
    return(result[, 'fid'])
  }
  
  insertIntoFlight <- function(df.row, airport) {
    date <- cleanDate(df.row[, "flight_date"])
    airline <- gsub("*", "", df.row[, "airline"])
    aircraft <- df.row[, "aircraft"]
    altitude <- df.row[, "altitude_ft"]
    altitude <- as.integer(gsub(",", "", altitude))
    heavy <- df.row[, "heavy_flag"]
    heavy <- !grepl("Unknown|No", heavy)
    sqlQuery <- sprintf(
      'INSERT INTO flights(date, origin, airline, aircraft, altitude, heavy)
           VALUES ("%s", %d, "%s", "%s", %d, %s)',
      date,
      airport,
      airline,
      aircraft,
      altitude,
      heavy
    )
    
    ps <- dbSendStatement(dbConn, sqlQuery)
    
    if (dbGetRowsAffected(ps) < 1) {
      txnFailed <- TRUE
    }
    
    dbClearResult(ps)
    
    return(fid)
  }
  
  checkIfConditionExists <- function(df.row) {
    condition <- df.row[, 'sky_conditions']
    sqlQuery <- sprintf(
      "
                  SELECT cid
                  FROM conditions
                  WHERE LOWER(sky_condition) = LOWER('%s')
                  LIMIT 1;
                 ",
      condition
    )
    result <- dbGetQuery(dbConn, sqlQuery)
    return(result[, 'cid'])
  }
  
  insertIntoConditions <- function(df.row) {
    condition <- df.row[, 'sky_conditions']
    sqlQuery <- sprintf("
                  INSERT INTO conditions(sky_condition)
                    VALUES ('%s');
                 ",
                        condition)
    ps <- dbSendStatement(dbConn, sqlQuery)
    
    if (dbGetRowsAffected(ps) < 1) {
      txnFailed <- TRUE
    }
    
    dbClearResult(ps)
    
    sqlQuery <-  "
                  SELECT MAX(cid) as cid
                  FROM conditions;
                 "
    result <- dbGetQuery(dbConn, sqlQuery)
    return(result[, 'cid'])
  }
  
  insertNewStrike <- function(df.row, flight, condition) {
    numbirds <- as.integer(df.row[, 'wildlife_struck'])
    impact <- df.row[, 'impact']
    damage <- df.row[, 'damage']
    altitude <- as.integer(gsub(",", "", df.row[, "altitude_ft"]))
    sqlQuery <-
      sprintf(
        'INSERT INTO strikes(fid, numbirds, impact, damage, altitude, conditions)
         VALUES (%d, %d, "%s", "%s", %d, %d)',
        flight,
        numbirds,
        impact,
        damage,
        altitude,
        condition
      )
    
    ps <- dbSendStatement(dbConn, sqlQuery)
    
    if (dbGetRowsAffected(ps) < 1) {
      txnFailed <- TRUE
    }
  }
  
  connectToDb()
  raw.df <- read.csv('sample-subset-agnibha.csv')
  nrows <- nrow(raw.df)
  
  dbExecute(dbConn, "SET autocommit = 0;")
  dbExecute(dbConn, "START TRANSACTION;")
  
  for (i in 1:nrows) {
    df.row <- raw.df[i,]
    airport = checkIfAirportExists(df.row)
    if (!length(airport)) {
      airport = insertIntoAirport(df.row)
    }
    
    flight = checkIfFlightExists(df.row)
    if (!length(flight)) {
      flight = insertIntoFlight(df.row, airport)
    }
    
    condition = checkIfConditionExists(df.row)
    if (!length(condition)) {
      condition = insertIntoConditions(df.row)
    }
    
    insertNewStrike(df.row, flight, condition)
    
  }
  
  if (txnFailed == TRUE) {
    print('There was a problem inserting data into one of the tables, rolling back')
    dbExecute(dbConn, "ROLLBACK;")
  } else {
    print('Successfully committed transaction')
    dbExecute(dbConn, "COMMIT;")
  }
  
  dbDisconnect(dbConn)
}

main()
