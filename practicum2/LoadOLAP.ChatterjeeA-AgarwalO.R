# This code imports necessary libraries for working with relational databases in R, including SQLite and MySQL.
# The DBI library provides a common interface for database operations,
# making it easier to switch between different database systems.
library(RSQLite)
library(DBI)
library(RMySQL)

main <- function() {
  # Path to the SQLite database file.
  cwd <- getwd()
  dbFile <- "data.db"
  pathToDb <- paste0(cwd,  .Platform$file.sep, dbFile)
  
  # Connect to the sqlite data base present in the local storage system.
  dbConnSqlite <- dbConnect(RSQLite::SQLite(), pathToDb)
  
  # Remote MySQL database connection to store the data fetched from the local sqlite to the remote server.
  # The database is used to implement the OLTP part of the assignment where we store the data in form of star schema.
  db.user <- 'admin'
  db.password <- 'practicum25200'
  db.name <- 'practicum2'
  db.host <- 'practicum2.cwtq29inp2km.us-east-2.rds.amazonaws.com'
  db.port <- 3306
  dbConnMySQL <-
    dbConnect(
      RMySQL::MySQL(),
      user = db.user,
      password = db.password,
      dbname = db.name,
      host = db.host,
      port = db.port
    )
  print('Connected to SQLite!')
  print('Connected to MySQL!')
  
  # Dropping the table before its creation to ensure every functionality implementation works correctly.
  dbExecute(dbConnMySQL, "DROP TABLE IF EXISTS sales_facts;")
  dbExecute(dbConnMySQL, "DROP TABLE IF EXISTS rep_facts;")
  dbExecute(dbConnMySQL, "DROP TABLE IF EXISTS date_dimension;")
  dbExecute(dbConnMySQL, "DROP TABLE IF EXISTS reps_dimension;")
  dbExecute(dbConnMySQL, "DROP TABLE IF EXISTS product_dimension;")
  dbExecute(dbConnMySQL, "DROP TABLE IF EXISTS sales_dimension;")
  
  print('Dropped all tables before they are created again')
  
  
  # Creation of date dimension table to store the information of the date like it's id, year, quarter etc.
  dbExecute(
    dbConnMySQL,
    "CREATE TABLE IF NOT EXISTS date_dimension(
                        date_id INTEGER PRIMARY KEY AUTO_INCREMENT,
                        date DATE NOT NULL,
                        year INTEGER NOT NULL,
                        quarter INT NOT NULL
);"
  )
  
  print('Created date_dimenstion')
  
  # Fetching out all the dates from the sales table stored in sqlite.
  all.dates.df <-
    dbGetQuery(dbConnSqlite, "SELECT DISTINCT date from sales;")
  
  # Creating an empty dataframe with different col name date, year and quarter.
  date.df <- data.frame(matrix(ncol = 3, nrow = 0))
  colnames(date.df) <- c('date', 'year', 'quarter')
  
  # Assigning the quarter values based on month number.
  for (i in 1:length(all.dates.df$date)) {
    date.df[i, 'year'] <-
      as.numeric(format(as.Date(all.dates.df$date[i]), "%Y"))
    month <- as.numeric(format(as.Date(all.dates.df$date[i]), "%m"))
    
    if (month %in% c(1, 2, 3)) {
      date.df$quarter[i] <- 1
    } else if (month %in% c(4, 5, 6)) {
      date.df$quarter[i] <- 2
    } else if (month %in% c(7, 8, 9)) {
      date.df$quarter[i] <- 3
    } else {
      date.df$quarter[i] <- 4
    }
  }
  date.df$date <- all.dates.df$date
  
  # Write the date dimension table in the MySQL remote server using appropriate flags like append, row names.
  dbWriteTable(dbConnMySQL,
               'date_dimension',
               date.df,
               append = T,
               row.names = F)
  
  local.sql.Reps.Query <- dbGetQuery(
    dbConnSqlite,
    "SELECT
                                  rid as rep_id,
                                  f_name as first_name,
                                  s_name as last_name,
                                  territory,
                                  commission
                                  FROM reps;"
  )
  
  # This SQL query is intended to create a table called 'reps_dimension' if it doesn't already exist.
  remote.sql.Reps.Query <-
    "CREATE TABLE IF NOT EXISTS reps_dimension (
   reps_key INT PRIMARY KEY AUTO_INCREMENT,
   rep_id VARCHAR(7) NOT NULL,
   first_name VARCHAR(255),
   last_name VARCHAR(255),
   territory VARCHAR(255),
   commission DECIMAL(10, 2),
   UNIQUE(rep_id)
);"
  
  print('Created reps_dimenstion')
  
  # The first line executes a SQL query on the MySQL database using the 'dbExecute' function.
  # It appears to execute the SQL query defined in 'remote.sql.Reps.Query' on the database.
  dbExecute(dbConnMySQL, remote.sql.Reps.Query)
  
  # The second line writes data to a table in the MySQL database using the 'dbWriteTable' function.
  # It seems to write data from 'local.sql.Reps.Query' to a table named 'reps_dimension' in the database.
  dbWriteTable(
    dbConnMySQL,
    "reps_dimension",
    local.sql.Reps.Query,
    append = TRUE,
    row.names = FALSE
  )
  
  # This SQL query is intended to create a table called 'product_dimension' if it doesn't already exist.
  remote.sql.product.Query <-
    "CREATE TABLE IF NOT EXISTS product_dimension (
   product_key INT PRIMARY KEY AUTO_INCREMENT,
   product_id INT NOT NULL,
   product_name VARCHAR(255),
   UNIQUE(product_id)
 );"
  
  print('Created product_dimenstion')
  
  # The following R code retrieves data from a SQLite database using the 'dbGetQuery' function.
  # It executes a SQL query on the SQLite database connection 'dbConnSqlite'.
  # The query selects two columns from the 'product' table and assigns aliases to them.
  # The 'pid' column is aliased as 'product_id', and the 'p_name' column is aliased as 'product_name'.
  local.sql.product.Query <-
    dbGetQuery(dbConnSqlite,
               "SELECT pid as product_id, p_name as product_name FROM product;")
  
  # The first line executes an SQL query on the MySQL database using the 'dbExecute' function.
  # It appears to execute the SQL query defined in 'remote.sql.product.Query' on the database.
  dbExecute(dbConnMySQL, remote.sql.product.Query)
  
  # It seems to write data from 'local.sql.product.Query' to a table named 'product_dimension' in the database.
  # The 'append=TRUE' parameter indicates that data should be appended to the table if it already exists.
  # The 'row.names = FALSE' parameter suggests that row names should not be included in the table.
  dbWriteTable(
    dbConnMySQL,
    "product_dimension",
    local.sql.product.Query,
    append = TRUE,
    row.names = FALSE
  )
  
  # This SQL query creates a table named 'sales_facts' in the database if it doesn't already exist.
  # The table is intended to store sales-related information with various fields.
  dbExecute(
    dbConnMySQL,
    "CREATE TABLE IF NOT EXISTS sales_facts (
    sales_fact_id INT PRIMARY KEY AUTO_INCREMENT,
    date_id INT,
    reps_key INT,
    product_key INT,
    year INT,
    quarter INT,
    total_sales_amount DECIMAL(10, 2),
    total_units_sold INT,
    FOREIGN KEY (date_id) REFERENCES date_dimension(date_id),
    FOREIGN KEY (reps_key) REFERENCES reps_dimension(reps_key),
    FOREIGN KEY (product_key) REFERENCES product_dimension(product_key)
);"
  )
  
  print('Created sales_facts')
  
  # This code executes an SQL query using the 'dbExecute' function to create a table named 'sales_dimension'.
  # The 'IF NOT EXISTS' clause ensures that the table is only created if it doesn't already exist.
  dbExecute(
    dbConnMySQL,
    "CREATE TABLE IF NOT EXISTS sales_dimension (
    sales_key INTEGER PRIMARY KEY AUTO_INCREMENT,
    sid INTEGER,
    date DATE NOT NULL,
    pid INTEGER,
    quantity INTEGER NOT NULL,
    sales_amt REAL NOT NULL,
    rid VARCHAR(7) NOT NULL
);"
  )
  
  print('Created sales_dimension')
  
  # The following R code involves data retrieval from a SQLite database and data writing to a MySQL database.
  # The first line fetches data from the SQLite database using the 'dbGetQuery' function.
  # It executes an SQL query on the SQLite database connected as 'dbConnSqlite'.
  df.sales_dim <-
    dbGetQuery(dbConnSqlite,
               "SELECT sid, date, pid, quantity, sales_amt, rid FROM sales;")
  
  # The line writes data to a table in the MySQL database using the 'dbWriteTable' function.
  # It writes the data stored in 'df.sales_dim' to a table named 'sales_dimension' in the MySQL database.
  dbWriteTable(
    dbConnMySQL,
    "sales_dimension",
    df.sales_dim,
    append = T,
    row.names = F
  )
  
  # The following SQL query inserts data into a table named 'sales_facts' in a MySQL database using a database connection 'dbConnMySQL'.
  # It combines data from various dimensions (date_dimension, product_dimension, and reps_dimension) and aggregates sales-related information.
  dbExecute(
    dbConnMySQL,
    "INSERT INTO sales_facts (date_id, reps_key, product_key, year, quarter, total_sales_amount, total_units_sold)
SELECT
    dd.date_id,
    rd.reps_key,
    pd.product_key,
    dd.year,
    dd.quarter,
    SUM(s.sales_amt) AS total_sales_amount,
    SUM(s.quantity) AS total_units_sold
FROM
    sales_dimension s
JOIN
    date_dimension dd ON dd.date = s.date
JOIN
    product_dimension pd ON pd.product_id = s.pid
JOIN
    reps_dimension rd ON rd.rep_id = s.rid
GROUP BY
    dd.date_id, rd.reps_key;"
  )
  
  print('Inserted into sales_facts')
  
  # This SQL query is intended to create a table named 'rep_facts' in a MySQL database if it doesn't already exist.
  dbExecute(
    dbConnMySQL,
    "CREATE TABLE IF NOT EXISTS rep_facts (
    rep_fact_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    rep_name VARCHAR(255),
    rep_id VARCHAR(7),
    total_sold DECIMAL(10, 2),
    year INTEGER,
    quarter INTEGER,
    product_name VARCHAR(255),
    total_sold_per_product DECIMAL(10, 2)
);
"
  )
  
  print('Created rep_facts')
  
  # Insert aggregated sales data into 'rep_facts' table
  # This query combines information from various dimensions (sales, date, product, reps)
  # and calculates the total sales and total sales per product, grouped by representative name,
  # year, quarter, and product name. The result is stored in the 'rep_facts' table.
  dbExecute(
    dbConnMySQL,
    "INSERT INTO rep_facts (rep_name, rep_id, total_sold, year, quarter, product_name, total_sold_per_product)
SELECT
    CONCAT(rd.first_name, ' ', rd.last_name) AS rep_name,
    rd.rep_id,
    SUM(sd.sales_amt) AS total_sold,
    dd.year,
    dd.quarter,
    pd.product_name,
    SUM(sd.sales_amt) AS total_sold_per_product
FROM
    sales_dimension sd
JOIN
    date_dimension dd ON dd.date = sd.date
JOIN
    product_dimension pd ON pd.product_id = sd.pid
JOIN
    reps_dimension rd ON rd.rep_id = sd.rid
GROUP BY
    rd.first_name, rd.last_name, dd.year, dd.quarter, pd.product_name;"
  )
  
  
  print('Inserted into rep_facts')
  
  # Extracts the total sales amount for each quarter in the year 2022 from the 'sales_facts' table.
  # The result is stored in the 'total_sold' variable using the dbGetQuery function.
  result <- dbGetQuery(
    dbConnMySQL,
    "SELECT
    quarter,
    SUM(total_sales_amount) AS total_sold
FROM
    sales_facts
WHERE
    year = 2022
GROUP BY
    quarter;"
  )
  
  # This R code retrieves the total sales amount for the product 'Alaraphosol' in each quarter of the year 2021.
  # It uses SQL queries to join relevant tables and aggregate the data by quarter.
  result2 <- dbGetQuery(
    dbConnMySQL,
    "SELECT
    sf.quarter,
    SUM(sf.total_sales_amount) AS total_sold
FROM
    sales_facts sf
JOIN
    reps_dimension rd ON sf.reps_key = rd.reps_key
JOIN
    product_dimension pd ON pd.product_key = sf.product_key
WHERE
    sf.year = 2021 AND pd.product_name = 'Alaraphosol'
GROUP BY
    sf.quarter;"
  )
  
  # This R code retrieves the total sales amount for the EMEA region in the year 2022
  # By joining the sales facts, reps dimension, and date dimension tables in a MySQL database.
  result3 <-
    dbGetQuery(
      dbConnMySQL,
      "SELECT SUM(sf.total_sales_amount) AS total_sales_in_EMEA_2022
FROM sales_facts sf
JOIN reps_dimension rd ON sf.reps_key = rd.reps_key
JOIN date_dimension dd ON sf.date_id = dd.date_id
WHERE rd.territory = 'EMEA' -- assuming the reps_dimension table has a 'region_name' column
  AND dd.year = 2022;"
    )
  
  # This R code retrieves the total sales for each sales representative in the year 2022 from a MySQL database and identifies the top-performing representative.
  # It then limits the result to only the top-performing representative with the highest total sales.
  result4 <- dbGetQuery(
    dbConnMySQL,
    "SELECT
    rep_name,
    SUM(total_sold) AS total_sales
FROM
    rep_facts
WHERE
    year = 2022
GROUP BY
    rep_name
ORDER BY
    total_sales DESC
LIMIT 1;"
  )
  
  # Disconnect from the SQLite database connection.
  dbDisconnect(dbConnSqlite)
  
  # Disconnect from the MySQL database connection.
  dbDisconnect(dbConnMySQL)
  
  print('Disconnected from SQLite!')
  print('Disconnected from MySQL!')
}

main()
