# Author: Agnibha Chatterjee
# Date: Fall 2023
# Assignment: Build XML Structure

library(XML)

main <- function() {
  # Building path to the XML file
  currDir <- getwd()
  xmlFile <- "ChatterjeeA.CS5200.Grants.xml"
  pathToFile <- paste0(currDir, .Platform$file.sep, xmlFile)
  
  # Parsing the XML file
  parsedXML <- xmlParse(pathToFile, validate = T)
  
  
  # Taking input from user
  idOfResearcher <- paste0('r', 203)
  
  
  # Explanation of xpath expression
  # 1. //Researcher[@rid='%s']/@gidFKs: This part of the expression selects the gidFKs attribute of the Researcher element with a specific rid attribute value equal to the placeholder %s. This is a placeholder for a specific researcher's ID
  # 2. string-length(...): This function returns the length of a string
  # 3. translate(//Researcher[@rid='%s']/@gidFKs, ' ', ''): This portion removes spaces from the selected gidFKs attribute using the translate function (i.e., a string replace)
  # 4. string-length(translate(...)): This calculates the length of the string after removing spaces.
  # 5. The two string-length expressions are subtracted to determine the number of characters that were spaces.
  # 6. Finally, 1 is added to the result to get the count of space-separated values. This accounts for the fact that the number of values in a space-separated string is always one more than the number of spaces.
  # 7. Doing this because gidFKs is a string of space separated values
  
  xpathEx <-
    sprintf(
      "string-length(//Researcher[@rid='%s']/@gidFKs) - string-length(translate(//Researcher[@rid='%s']/@gidFKs, ' ', '')) + 1",
      idOfResearcher,
      idOfResearcher
    )
  
  # Executing the xpath expression
  noOfGrants <- xpathSApply(parsedXML, xpathEx)
  
  # Printing results
  print(paste(
    "Researcher with ID:",
    idOfResearcher,
    "has",
    noOfGrants,
    "grants"
  ))
}

main()
