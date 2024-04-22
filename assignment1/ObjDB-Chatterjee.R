# Author: Agnibha Chatterjee
# Date: September 14, 2023
# Description: This R script implements a hierarchical document database

rootDir <- 'docDB'

#' @description The main function is the entry point to the program
#' @return void
main <- function() {
  
  #' @description This function checks whether a directory exists or it creates the directory
  #' @param directoryName The name of the directory whose existence is to be checked or created
  #' @return void
  createDirectoryIfNotExists <- function(directoryName) {
    directoryName <- trimws(directoryName, which = 'both')
    if (!dir.exists(directoryName))
    {
      dir.create(directoryName)
    }
  }
  
  #' @description This function is used to extract the extension from a fileName
  #' @param fileName A string containing the name of the file
  #' @return The extension from the fileName with a prepended period
  #' @example getFileExtension('demo #tag1 #tag2.txt') returns '.txt'
  #' @example getFileExtension('#tag1 #tag2 demo.mp3') returns '.mp3'
  getFileExtension <- function(fileName) {
    # Split the file name on period
    fileExt <- strsplit(basename(fileName), split = '\\.')[[1]]
    
    if (length(fileExt[-1]) > 1) {
      # sample2.py #tag is being read as sample2.py and #tag.py, which is why I'm having to pull out the element at [-1][-1]
      fileExt <- fileExt[-1][-1]
    } else {
      fileExt <- fileExt[-1]
    }
    
    # pick the last element from the character vector
    actualEx <- gsub(' ', '', fileExt)
    
    
    # if a file without an extension is detected, terminate the program
    if (identical(actualEx, character(0))) {
      stop(
        '[ERROR] A file without an extension was detected in the source folder, The program cannot guess the extension of the file and so is being terminated, remove all files without extensions and retry'
      )
    }
    
    if (grepl('#', actualEx)) {
      tempSplit <- strsplit(actualEx, '#')
      actualEx <- gsub(' ', '', tempSplit[[1]][1])
    }
    actualEx <- paste('.', actualEx, sep = '')
    return(actualEx)
  }
  
  #' @description This function is used to extract just the fileName and extension from a string containing the filename, tags and file extension
  #' @param fileName A string containing the file name, tags and file extension
  #' @return A string which is just the name of the file along with its extension
  #' @example getFileName('demo #tag1 #tag2.jpg') returns 'demo.jpg'
  #' @example getFileName('demo.py #program1 #program2') returns 'demo.py'
  getFileName <- function(fileName) {
    extension <- getFileExtension(fileName)
    
    # Remove extension from file name
    fileNameWithoutExtension <- gsub(extension, '', fileName)
    tags <- strsplit(fileNameWithoutExtension, '#')
    
    # Filename is always present in the beginning of the string, hence pulling out the first element
    onlyFileName <- trimws(tags[[1]][1], which = 'both')
    
    if (!nchar(onlyFileName)) {
      warningMsg <-
        paste(
          '[WARNING] A file without a file name with the extension',
          extension,
          'was detected, this will not obstruct the program but please consider adding a name.',
          'If on a MacOS/Unix machine please use the ls -a command to view such a file as their visibility will be hidden because their name starts with a period'
        )
      print(warningMsg)
    }
    
    finalFileName <- paste(onlyFileName, extension, sep = '')
    return(finalFileName)
  }
  
  #' @description This function is used to extract the tags from a string containing the filename, tags and file extension
  #' @param fileName A string containing the file name, tags and file extension
  #' @return A character vector containing the tags (each tag is prepended with #)
  #' @example getTags('demo #tag1 #tag2.jpg') returns ('tag1', '#tag2')
  #' @example getTags('demo.py #program1 #program2') returns ('#program1', '#program2')
  getTags <- function(fileName) {
    extension <- getFileExtension(fileName)
    
    # Remove extension from file name
    fileNameWithoutExtension <- gsub(extension, '', fileName)
    
    # Split on # to pull out tags
    tags <- strsplit(fileNameWithoutExtension, '#')
    tags <- tags[[1]][-1]
    tagsVec <- c()
    for (tag in tags) {
      tag <- gsub(' ', '', tag)
      
      # Adding a '#' before each tag
      tagsVec <- c(tagsVec, paste('#', tag, sep = ''))
    }
    return(tagsVec)
  }
  
  #' @description This function is used to strip the leading '#' from a tag
  #' @param tag A fully qualified tag (with the '#')
  #' @return A trimmed version of the tag without '#'
  #' @example stripHashTag('#tag1') returns 'tag1'
  stripHashTag <- function(tag) {
    tag <- sub('#', '', tag)
  }
  
  #' @description This function is used to check whether a folder exists
  #' @param name A string containing the name/path of the file
  #' @param kindOfFolder A string which is an alias of the folder, this param will help print a compact message
  #' @return A boolean depending on whether the folder exists or not
  checkIfFolderExists <- function(name, kindOfFolder) {
    name <- trimws(name, which = 'both')
    if (!dir.exists(name)) {
      stop(
        paste(
          '[ERROR]',
          kindOfFolder,
          'directory does not exist! Please create it and try again.'
        )
      )
    }
    
    return(TRUE)
  }
  
  #' @description This function is used to scan the source folder and copy files into their respective destination folders in the database
  #' @param folder A string containing the name of the source folder to be scanned
  #' @param root A string pointing the the root folder housing the database
  #' @param verbose [OPTIONAL] A boolean that enables verbose logging
  #' @return void
  storeObjs <- function(folder, root, verbose = FALSE) {
    root <- trimws(root, which = 'both')
    folder <- trimws(folder, which = 'both')
    
    if (!nchar(folder)) {
      stop('[Error] Source directory cannot be an empty string, please try again.')
    }
    
    # Check if root exists
    checkIfFolderExists(root, 'Root database')
    # Check if source folder exists
    checkIfFolderExists(folder, 'Source')
    
    print('Source and root folders have been located, beginning to copy files...')
    files <- list.files(path = folder,
                        include.dirs = TRUE,
                        recursive = TRUE)
    
    if (!length(files)) {
      stop(
        '[ERROR] There are no files in the source directory, please add a few files and try again'
      )
    }
    
    # Pseudocode
    # go through the files in the specified folder
    # for each file check the tags present (there may be files without tags)
    # check if folder specified by the tag is present, else create one
    # copy paste files from source folder into the destination folder
    
    for (file in files) {
      tags <- getTags(file)
      fileName <- getFileName(file)
      cwd <- getwd()
      fullPathToCurrentFile = file.path(cwd, folder, file)
      
      if (verbose) {
        msg <-
          paste('Copying',
                fileName,
                'to',
                paste(lapply(tags, stripHashTag), collapse = ', '))
        
        if (!length(tags)) {
          msg <-
            paste('Copying',
                  fileName,
                  'to the untagged directory, as it had no tags')
        }
        
        print(msg)
      }
      
      if (!length(tags)) {
        pathToUntaggedFolder <- file.path(root, 'untagged')
        createDirectoryIfNotExists(pathToUntaggedFolder)
        file.copy(fullPathToCurrentFile,
                  file.path(cwd, pathToUntaggedFolder, fileName))
      }
      
      for (tag in tags) {
        finalPathToDir <- genObjPath(root, tag)
        createDirectoryIfNotExists(finalPathToDir)
        file.copy(fullPathToCurrentFile,
                  file.path(cwd, finalPathToDir, fileName))
      }
    }
    
    print('Successfully copied all files!')
  }
  
  #' @description This function is used to generate and return the path of the folders represented by the tags
  #' @param root A string containing the root path of the database
  #' @param tag A string containing the tag
  #' @return A string which is the path to the folder specified by the tag
  #' @example genObjectPath('docDb', '#temp') returns 'docDb/temp'
  #' @example genObjectPath('docDb/demo', '#cats') returns 'docDb/demo/cats'
  genObjPath <- function(root, tag) {
    strippedTag <- sub('#', '', tag)
    finalPath <- file.path(rootDir, strippedTag)
    return(finalPath)
  }
  
  #' @description This function is used to reset the database
  #' @param root A string pointing to the root directory of the database
  #' @return void
  clearDB <- function(root) {
    checkIfFolderExists(root, 'Root database')
    unlink(file.path(root, '*'), recursive = TRUE)
    print('Database was cleared')
  }
  
  #' @description This function is used to set up the database under the parent folder specified by root and updates the global rootDir variable to point to the parent directory (docDb in our case)
  #' @param root A string containing the parent directory of the database
  #' @param path A string that can/cannot be empty, used to specify the path to another directory within the parent directory whether the database is to be initialized
  #' @return void
  #' @example calling configDB('docDb', '') will initialize the database under the folder docDb
  #' @example calling configDB('docDb', 'temp') will initialize the database under the folder temp/docDb
  configDB <- function(root, path) {
    path <- trimws(path, which = 'both')
    
    temporaryRelativePath <- '.'
    
    if (nchar(path) >= 1) {
      # This is to handle paths such as /dir1/dir2
      # I'm splitting /dir1/dir2 and creating both folders before creating the docDB directory
      splitPath <- strsplit(path, .Platform$file.sep)[[1]]
      splitPath <- splitPath[splitPath != '']
      for (pathChunk in splitPath) {
        pathChunk <- trimws(pathChunk, which = 'both')
        temporaryRelativePath <-
          file.path(temporaryRelativePath, pathChunk)
        createDirectoryIfNotExists(temporaryRelativePath)
      }
    }
    
    constructedPath <- file.path(temporaryRelativePath, root)
    
    # Finally creating the docDB directory
    createDirectoryIfNotExists(constructedPath)
    print('Database successfully initialized')
    
    # Updating the global variable
    rootDir <<- constructedPath
    
    fullPathToDB <- file.path(getwd(), constructedPath)
    fullPathToDB <- gsub('/./', '/', fullPathToDB)
    print(paste('Database was initialized at', fullPathToDB))
  }
  
  configDB(rootDir, 'database')
  
  #' Please create a folder called source in the directory where this R file will be present
  #' You can include all source files in this folder
  #' *****Please don't include an sub-directories in the source folder*****
  storeObjs('source', rootDir, TRUE)
  
  #' *****To run the clearDB function please compile the following*****
  #' 1. checkIfFolderExists
  #' 2. clearDB
  #clearDB(rootDir)
}

main()
#quit()
