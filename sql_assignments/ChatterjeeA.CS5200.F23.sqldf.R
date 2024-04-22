# Query Tabular Data with SQL
# Agnibha Chatterjee
# Fall 2023
# Date 10/09/2023

# Importing necessary packages
library(sqldf)
library(knitr)

# Here's the approach I was going for
# I wanted to limit the inputs to the script to only be the urls
# I didn't want the user interacting with the script to declare variables manually
# For example - svJanToMarch = read.csv(url('URL_TO_FILE'))
# This is is something I didn't want as this was increase the no of contact points between the user and the script, making it more maintainable


main <- function() {
  # The uris variable is a vector containing the urls of the csv files, this is the only input from the user - the user could add to or remove urls from the vector
  uris <- c(
    'https://s3.us-east-2.amazonaws.com/artificium.us/assignments/80.xml/a-80-305/gen-xml/synthsalestxns-Nov2Dec.csv',
    'https://s3.us-east-2.amazonaws.com/artificium.us/assignments/80.xml/a-80-305/gen-xml/synthsalestxns-Sep2Oct.csv',
    'https://s3.us-east-2.amazonaws.com/artificium.us/assignments/80.xml/a-80-305/gen-xml/synthsalestxns-Jan2Mar.csv'
  )
  
  # Initializing an empty datafram, I will be adding each dataframe generated from the urls to this dataframe
  combinedDf <- data.frame()
  
  # Loop through each url
  # Read the incoming data
  # Use sqldf to pull out data from the urls into a dataframe
  # Append the dataframe to combinedDf
  for (uri in uris) {
    csv <- read.csv(url(uri))
    
    # Each amount has $ prepended to it's value, removing that with SUBSTR
    # I manually inspected the csv files and figured out that the cc column is unique in every file, I will be using it to keep count of a visit
    sqlq <- "
  SELECT restaurant as restaurant_name, CAST(SUBSTR(amount, 2, LENGTH(amount)) AS NUMERIC) as revenue, cc
  FROM `csv`
  ORDER BY restaurant ASC;
  "
    
    df <- sqldf(sqlq)
    
    # Appending the dataframe to combinedDF
    combinedDf <- rbind(combinedDf, df)
  }
  
  # The aggregate function is used to perform aggregations on data in R
  # revenue ~ restaurant_name tells R that I wish to run an aggregation on revenue against the restaurant_name column (i.e., run an aggregation on revenue grouped by restaurant_name)
  # data = combinedDf is where I tell R that I wish to perform the aggregation on combinedDf
  # FUN = mean is the kind of aggregation (i.e., average, length, sum, etc...)
  # Similarly for totalNoOfVisits
  avgRevenue <-
    aggregate(revenue ~ restaurant_name, data = combinedDf, FUN = mean)
  totalNoOfVisits <-
    aggregate(cc ~ restaurant_name, data = combinedDf, FUN = length)
  
  # The merge function is used to merge dataframes on a common column
  # The first two inputs to the function specify the dataframes that I wish to merge
  # The by = 'restaurant_name' argument specifies that I wish to join both the input dataframes on the restautant_name column
  # all = TRUE specifies that I intend to include all rows from the input dataframes
  resultantDf <-
    merge(totalNoOfVisits, avgRevenue, by = "restaurant_name", all = TRUE)
  
  # Renaming columns before showing the output
  names(resultantDf)[names(resultantDf) == 'revenue'] <-
    'avg_revenue'
  names(resultantDf)[names(resultantDf) == 'cc'] <- 'total_visits'
  
  # Using kable to pretty print in a tabular format
  kable(resultantDf, format = "simple")
  
}

main()