

library(RSQLite)
library(XML)
################# Reps Data reading and writing to DB #####################
# Function to convert an XML 'rep' node to a dataframe row
reps_to_df <- function(rep_node) {
  # Extract the 'rID' attribute
  rID <- xmlGetAttr(rep_node, "rID")
  
  # Extract elements inside the 'rep' node
  first_name <- xmlValue(rep_node[["name"]][["first"]])
  sur_name <- xmlValue(rep_node[["name"]][["sur"]])
  territory <- xmlValue(rep_node[["territory"]])
  commission <- as.numeric(xmlValue(rep_node[["commission"]]))
  
  # Return as a one-row dataframe
  data.frame(
    rid = rID,
    f_name = first_name,
    s_name = sur_name,
    territory = territory,
    commission = commission,
    stringsAsFactors = FALSE
  )
}

get_reps_XML_dataframe <- function() {
  reps.Data <- xmlParse("txn-xml/pharmaReps-F23.xml")
  rep.Nodes <- getNodeSet(reps.Data, "//rep")
  reps.DataFrame <- do.call(rbind, lapply(rep.Nodes, reps_to_df))
  return (reps.DataFrame)
}

create_reps_schema <- function(con) {
  create_table_sql <- "
  CREATE TABLE IF NOT EXISTS reps (
    rid VARCHAR(7) PRIMARY KEY,
    f_name TEXT,
    s_name TEXT,
    territory TEXT,
    commission REAL
  );"
  dbExecute(con, create_table_sql)
}

drop_reps_schema <- function(con) {
  drop_table_sql <- "DROP TABLE IF EXISTS reps;"
  dbExecute(con, drop_table_sql)
}


################# Product Data reading #####################

get_product_XML_dataframe <- function(product_xml_file_path) {
  # Parse the XML file
  xml_data <- xmlParse(product_xml_file_path)
  
  # Extract product data from the XML
  sale_nodes <- getNodeSet(xml_data, "//sale")
  products <-
    sapply(sale_nodes, function(node)
      xmlValue(node[["product"]]))
  
  # Create a dataframe for products with unique product names
  product.DataFrame <-
    data.frame(
      pid = NA,
      p_name = unique(products),
      stringsAsFactors = FALSE
    )
  
  return (product.DataFrame)
}

create_product_schema <- function(con) {
  create_table_sql <- "
  CREATE TABLE IF NOT EXISTS product (
    pid INTEGER PRIMARY KEY AUTOINCREMENT,
    p_name TEXT NOT NULL);"
  dbExecute(con, create_table_sql)
}

drop_product_schema <- function(con) {
  drop_table_sql <- "DROP TABLE IF EXISTS product;"
  dbExecute(con, drop_table_sql)
}


################# Customer Data reading #####################

get_customer_XML_dataframe <- function(product_xml_file_path) {
  # Parse the XML file
  xml_data <- xmlParse(product_xml_file_path)
  
  # Extract transaction data from the XML
  txn_nodes <- getNodeSet(xml_data, "//txn")
  customers <-
    sapply(txn_nodes, function(node)
      xmlValue(node[["customer"]]))
  countries <-
    sapply(txn_nodes, function(node)
      xmlValue(node[["country"]]))
  
  # Create a dataframe for customers with unique customer names and their countries
  customer_data <-
    data.frame(c_name = customers,
               country = countries,
               stringsAsFactors = FALSE)
  customer.DataFrame <- customer_data[!duplicated(customer_data),]
  
  return (customer.DataFrame)
}

create_customer_schema <- function(con) {
  create_table_sql <-
    "CREATE TABLE IF NOT EXISTS customer (
    cid INTEGER PRIMARY KEY AUTOINCREMENT,
    c_name TEXT NOT NULL,
    country TEXT
  );"
  dbExecute(con, create_table_sql)
}

drop_customer_schema <- function(con) {
  drop_table_sql <- "DROP TABLE IF EXISTS customer;"
  dbExecute(con, drop_table_sql)
}

################# Sales and Juntion Data reading #####################
get_sales_customername_dataframe <-
  function(product.Dataframe,
           product_xml_file_path) {
    xml_data <- xmlParse(product_xml_file_path)
    txn_nodes <- getNodeSet(xml_data, "//txn")
    
    sales.cname.Dataframe <-
      do.call(rbind, lapply(txn_nodes, function(txn) {
        product.Name <- xmlValue(txn[['sale']][['product']])
        pid.Value <- which(product.Dataframe$p_name == product.Name)
        result <- data.frame(
          date = as.character(as.Date(xmlValue(txn[["sale"]][["date"]]), format =
                                        "%m/%d/%Y")),
          pid = pid.Value,
          quantity = as.integer(xmlValue(txn[["sale"]][["qty"]])),
          sales_amt = as.numeric(xmlValue(txn[["sale"]][["total"]])),
          txnid = as.integer(xmlGetAttr(txn, "txnID")),
          rid = paste0('r', xmlGetAttr(txn, "repID")),
          c_name = xmlValue(txn[["customer"]])
        )
      }))
    
    return (sales.cname.Dataframe)
  }

create_sales_schema <- function(con) {
  create_table_sql <-
    "CREATE TABLE IF NOT EXISTS sales (
    sid INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    pid INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    sales_amt REAL NOT NULL,
    rid VARCHAR(7) NOT NULL
  );"
  dbExecute(con, create_table_sql)
}

drop_sales_schema <- function(con) {
  drop_table_sql <- "DROP TABLE IF EXISTS sales;"
  dbExecute(con, drop_table_sql)
}

################# get junction table #####################

get_junction_table <-
  function(sales_cname.Dataframe,
           customer.Dataframe) {
    junction.Dataframe <- data.frame(matrix(ncol = 3, nrow = 0))
    colnames(junction.Dataframe) <- c('txnid', 'sid', 'cid')
    
    junction.Dataframe <-
      do.call(rbind, lapply(1:nrow(sales_cname.Dataframe), function(dataframe.row) {
        txnID <- sales_cname.Dataframe[dataframe.row, "txnid"]
        sID <- sales_cname.Dataframe[dataframe.row, "sid"]
        name <- sales_cname.Dataframe[dataframe.row, "c_name"]
        cID <- which(customer.Dataframe$c_name == name)
        result <-
          data.frame(
            txnid = txnID,
            sid = sID,
            cid = cID,
            stringsAsFactors = FALSE
          )
      }))
    
    return (junction.Dataframe)
  }

create_sales_customer_junction_schema <- function(con) {
  create_table_sql <-
    "CREATE TABLE IF NOT EXISTS sales_customer_junction (
    txnid INTEGER NOT NULL,
    sid INTEGER,
    cid INTEGER,
    PRIMARY KEY(txnid,sid,cid),
    CONSTRAINT fk_junction_sales_link FOREIGN KEY(sid) references sales(sid),
    CONSTRAINT fk_junction_customer_link FOREIGN KEY(cid) references customer(cid)
  );"
  
  dbExecute(con, create_table_sql)
}

drop_sales_customer_junction_schema <- function(con) {
  drop_table_sql <- "DROP TABLE IF EXISTS sales_customer_junction;"
  dbExecute(con, drop_table_sql)
}

################# Main Method #####################

main <- function() {
  cwd <- getwd()
  dbFile <- "data.db"
  pathToDb <- paste0(cwd,  .Platform$file.sep, dbFile)
  # Create SQLite connection
  con <- dbConnect(SQLite(), dbname = pathToDb)
  
  print('Connected to SQLite')
  
  ### Dropping of all the relational schema if they already exists in the memory.
  drop_reps_schema(con)
  
  drop_customer_schema(con)
  
  drop_product_schema(con)
  
  drop_sales_schema(con)
  
  drop_sales_customer_junction_schema(con)
  
  
  ### Creation of the relational schema in the passed sqlite database.
  create_reps_schema(con)
  
  create_product_schema(con)
  
  create_customer_schema(con)
  
  create_sales_schema(con)
  
  create_sales_customer_junction_schema(con)
  
  
  # Define the directory containing the XML files.
  dir_path <- paste0(cwd,  .Platform$file.sep, "txn-xml")
  
  # Getting list of files from the directory with the reg-ex pattern.
  # As mentioned in the practicum the file names are checked with a particular pattern.
  file_list <-
    list.files(dir_path, pattern = "pharmaSalesTxn.*\\.xml", full.names = TRUE)
  
  # Creating empty data frames to store values for the respective schemas.
  
  ## Empty data frame for sales representatives.
  reps.Dataframe <- data.frame(matrix(ncol = 5, nrow = 0))
  colnames(reps.Dataframe) <-
    c('rid', 'f_name', 's_name', 'territory', 'commission')
  
  ## As the sales file is different from others it is read at once using the fixed file path.
  reps.Dataframe <- get_reps_XML_dataframe()
  
  ## Empty data frame for products schema that are sold by the pharma companies.
  product.Dataframe <- data.frame(matrix(ncol = 2, nrow = 0))
  colnames(product.Dataframe) <- c('pid', 'p_name')
  
  ## Empty data frame for customers schema that buy particular products.
  customer.Dataframe <- data.frame(matrix(ncol = 3, nrow = 0))
  colnames(customer.Dataframe) <- c('cid', 'c_name', 'country')
  
  ## Empty data frame for sales schema that represents the sales transactions of products and their customer names.
  sales.cname <- data.frame(matrix(ncol = 8, nrow = 0))
  colnames(sales.cname) <-
    c('sid',
      'date',
      'pid',
      'quantity',
      'sales_amt',
      'txnid',
      'rid',
      'c_name')
  
  # Parsing through list of files to fill the data in product and customer data frame.
  for (file in file_list) {
    print(paste0("Procesing Files for product & customer tables: ", file))
    
    # Getting value of product from a method call using a local data frame field.
    # It then appends the data to the main data frame using the rbind call.
    product.Dataframe.SingleFile <- get_product_XML_dataframe(file)
    product.Dataframe <-
      rbind(product.Dataframe.SingleFile, product.Dataframe)
    
    # Getting value of customers from a method call using a local data frame field.
    # It then appends the data to the main data frame using the rbind call.
    customer.Dataframe.SingleFile <- get_customer_XML_dataframe(file)
    customer.Dataframe <-
      rbind(customer.Dataframe.SingleFile, customer.Dataframe)
  }
  
  # Removing the duplicates from the product data frame.
  # While parsing several files there might be same products purchased by different customers.
  product.Dataframe <-
    product.Dataframe[!duplicated(product.Dataframe), ]
  
  # Assigning a surrogate key value to the product data frame before storing it in sqlite.
  product.Dataframe$pid = 1:length(product.Dataframe$p_name)
  
  # Removing the duplicates from the customer data frame.
  customer.Dataframe <-
    customer.Dataframe[!duplicated(customer.Dataframe), ]
  
  # Assigning a surrogate key value to the customer data frame before storing it in sqlite.
  customer.Dataframe$cid = 1:length(customer.Dataframe$c_name)
  
  # Parsing the sales data frame using again similar for loop over the list of files.
  # The sales schema needed values from the other tables like product for it's creation.
  # So, sales table was created after complete processing of the other schemas.
  for (file in file_list) {
    print(paste0("Procesing Files for sales and junction tables: ", file))
    sales.cname.Dataframe.SingleFile <-
      get_sales_customername_dataframe(product.Dataframe, file)
    sales.cname <- rbind(sales.cname.Dataframe.SingleFile, sales.cname)
  }
  sales.cname <- sales.cname[!duplicated(sales.cname), ]
  sales.cname$sid = 1:length(sales.cname$pid)
  
  junction.Dataframe <-
    get_junction_table(sales.cname, customer.Dataframe)
  sales.Dataframe <-
    subset(sales.cname,
           select = c('sid', 'date', 'pid', 'quantity', 'sales_amt', 'rid'))
  
  
  ### Write the dataframes to the sqlite db. For our case the db is "data.db"
  dbWriteTable(con,
               "reps",
               reps.Dataframe,
               append = TRUE,
               row.names = FALSE)
  
  dbWriteTable(con,
               "product",
               product.Dataframe,
               append = TRUE,
               row.names = FALSE)
  
  dbWriteTable(con,
               "customer",
               customer.Dataframe,
               append = TRUE,
               row.names = FALSE)
  
  dbWriteTable(con,
               "sales",
               sales.Dataframe,
               append = TRUE,
               row.names = FALSE)
  
  dbWriteTable(
    con,
    "sales_customer_junction",
    junction.Dataframe,
    append = TRUE,
    row.names = FALSE
  )
  
  print('Successfully inserted all data into SQLite!')
  
  dbDisconnect(con)
}

main()
