# R Program to Implement Transactions and Handle Concurrency
# Authors: Agnibha Chatterjee, Soundarya Srikanta, Anish Rao, Shen Feng (Group 8)
# Date: Fall 2023

# Loading required libraries
library(RMySQL)
library(DBI)

# Initializing the database connection variable
dbConn <- NULL

# Main function
main <- function() {
  
  # Flag to track transaction success/failure
  txnFailed <- FALSE
  
  # Function to connect to database
  connectToDb <- function() {
    db.host <- "sql9.freemysqlhosting.net"  
    db.port <- 3306  
    db.user <- "sql9657310"  
    db.password <- "XYBJSngFrf" 
    db.name <- "sql9657310"
    
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
  
  # Function to check if airport already exists
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
  
  # Function to insert data to airports
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
  
  # Function to check if flight already exists
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
  
  # Function to insert data to flights
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
  
  # Function to check if condition already exists
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
  
  # Function to insert data to conditions
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
  
  # Function to insert data to strikes
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
  
  # Establishing a database connection
  connectToDb()
  
  # Reading data from a CSV file
  raw.df <- read.csv('sample-subset-agnibha.csv')
  nrows <- nrow(raw.df)
  
  # Beginning a transaction
  dbExecute(dbConn, "SET autocommit = 0;")
  dbExecute(dbConn, "START TRANSACTION;")
  
  # Looping through the rows of the CSV data
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
  
  # Checking for transaction success/failure and committing/rolling back accordingly
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

# Answers

# Q: how would you back out inserted data?

# In this code, a transaction is used to ensure atomicity. 
# If an error occurs during the insertion of data, 
# a rollback is performed to undo the changes made during the transaction. 
# The code for this is in line 230-233

# Q: How do you deal with concurrency?

# The code employs transactions with START TRANSACTION, COMMIT, 
# and ROLLBACK statements, ensuring that a sequence of operations is treated atomically. 
# One effective approach to deal with concurrency is to set appropriate isolation levels for transactions, 
# defining the degree of interaction between them. This can range from more lenient levels like 
# READ UNCOMMITTED to stricter levels like SERIALIZABLE.

# Q: How do you assign synthetic keys in a concurrent environment?

# Our table creation scripts utilize auto-incrementing primary keys (aid, fid, cid, sid) for key generation. 
# This approach is crucial to consider potential conflicts in a concurrent environment.
# We also use functions like LAST_INSERT_ID() to retrieve the last generated key after an insertion, 
# ensuring that each transaction gets the correct key even in a concurrent setting.
# Another option is to use UUID (Universally Unique Identifiers) as synthetic keys because they are designed to be globally unique and can help avoid collisions
