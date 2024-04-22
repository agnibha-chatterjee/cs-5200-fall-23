# title: "Transactions"
# author: "Group 5 - Mohan Sri Chollangi"
# date: "Fall Full 2023"

# Connect to MySQL
library(RMySQL)
library(RSQLite)
library(DBI)
library(sqldf)


connect_to_db <- function() {
  # connect to MySQL
  rm(list = ls(all.names = TRUE))
  gc()
  db_user <- 'sql9655018' 
  db_password <- 'zgFmnQs1LC' 
  db_name <- 'sql9655018' 
  db_host <- 'sql9.freemysqlhosting.net' 
  db_port <- 3306
  
  
  mydb.fh <-  dbConnect(RMySQL::MySQL(), user = db_user, password = db_password,
                        host = db_host, port = db_port, dbname=db_name)
  
  dbcon <- dbListConnections(MySQL())[[1]]
  
  return(dbcon)
}

# connect to database
dbcon <- connect_to_db()

# Write R code to load the CSV and then start a transaction and 
# within that transaction add each new bird strike from the CSV 
# to the (same) database (by looping through the CSV). 

loadCSV <- function(CSV) {
  # Function load CSV loads CSV into a dataframe 
  # to then be inserted into the appropriate tables.
  
  # insert code here
  bds.raw <- read.csv(CSV, header=TRUE)
  
  return(bds.raw)
  # return a dataframe? / return dataframes for each table?
}


getSQLStatementStrikes <- function(df, dbcon, i) {
  # for each, collect the values to insert and craft a SQL INSERT Statement.
  # nrows <- nrow(df)
  # take each dataframe and insert it into the appropriate tables?
  # pull columns names for SQL INSERT
  # sid <- seq(1, nrows) #50000 + i
  fid <- df$rid[i]
  numbirds <- df$wildlife_struck[i]
  impact <- df$impact[i]
  damage <- df$damage[i]
  altitude <- gsub(",", "", df$altitude_ft[i])
  if (altitude == "") {
    altitude <- 'NULL'
  }
  
  # get cid from conditions table
  condition <- df$sky_conditions[i]
  cid.db <- dbGetQuery(dbcon, paste0("select cid from conditions where sky_condition = '", condition, "'"))
  cid <- cid.db$cid
  
  # sqlCode <- paste0("insert into strikes (sid, fid, numbirds, impact, damage, altitude, conditions) values ", "(", sid, ",", fid, ",", numbirds, ",", "'", impact, "'", ",'", damage, "',", altitude, ",", cid, ")")
  sqlCode <- paste0("insert into strikes (fid, numbirds, impact, damage, altitude, conditions) values ", "(", fid, ",", numbirds, ",", "'", impact, "'", ",'", damage, "',", altitude, ",", cid, ")")
  
  return(sqlCode)
  
}


doTransaction <- function(df, db) {
  # Function doTransaction takes a dataframe and loads
  # it into the table in a dataframe.
  
  # start transaction
  dbExecute(dbcon, "START TRANSACTION;")
  
  # insert each row into strikes table
  nrows <- nrow(df)
  for (i in 1:nrows) {
    # get insert statement to use for loading new rows in Strikes
    sqlCodeStrikes <- getSQLStatementStrikes(df, db, i)
    print(sqlCodeStrikes)
    rs <- dbExecute(dbcon, sqlCodeStrikes)
  }
  
  
  dbExecute(dbcon, "commit;")
  
  # commit transaction if no failure, otherwise rollback
  #if (txnFailed == TRUE)
  #  dbExecute(dbcon, "rollback transaction;")
  #else
  #  dbExecute(dbcon, "commit transaction;")
  
  
}


# Execute Code
csv <- loadCSV("NewBirdStrikesData.csv")
print(csv)

# Start transaction of loading data
doTransaction(csv, dbcon)

dbGetQuery(dbcon, "select * from strikes where sid = 50001")

# disconnect from database
dbDisconnect(dbcon)
