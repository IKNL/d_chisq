#!/usr/bin/env Rscript

#' ----------------------------------------------------------------------------
#' title: dl_chisq.R
#' description:
#'   Implementation of the distributed Chi Squared algorithm based on the
#'   article from [Damiani et al](https://www.learntechlib.org/p/182311/).
#'
#'   This implementation can be used with the PyTaskManager distributed
#'   learning infrastructure (see https://github.com/IKNL/pytaskmanager).
#'
#' authors:
#'   Carlotta Masciocchi <>
#'   Frank Martin <f.martin@iknl.nl>
#'   Gijs Geleijnse <g.geleijnse@iknl.nl>
#'   Melle Sieswerda <m.sieswerda@iknl.nl>
#' date: 26-juli-2018
#' license: MIT License
#' ----------------------------------------------------------------------------


# External libraries
library(rjson)
library(dplyr)

# ******************************************************************************
# ---- Helper functions ----
# ******************************************************************************

#' Write a string to STDOUT without the standard '[1]' prefix.
writeln <- function(x="", sep=" ") {
  cat(paste(paste(x, collapse=sep), "\n"))
}


# ******************************************************************************
# ---- RPC entry points ----
# ******************************************************************************

#' Return local row count of the dataset.
#'
#' This corresponds to the description in Table 1, iteration 2 in the article.
#'
#' Params:
#'   df: data frame containing the *local* dataset.
#'
#' Return:
#'   number of rows in the dataset
RPC_get_row_count <- function(df) {
  return(nrow(df))
}

#' Return local covariate ranges in the dataset.
#'
#' This corresponds to the description in Table 1, iteration 2 in the article.
#'
#' Params:
#'   df: data frame containing the *local* dataset.
#'   expl_vars: list of (numerical) explanatory variables (covariates) to return
#'              the minumum and maximum for.
#'
#' Return:
#'   list with named members 'min', 'max'
RPC_get_ranges <- function(df, expl_vars) {

  if (length(expl_vars) > 1) {
    local_minimum <- apply(df[, expl_vars], 2, min)
    local_maximum <- apply(df[, expl_vars], 2, max)

  } else {
    local_minimum <- min(df[, expl_vars])
    names(local_minimum) <- expl_vars

    local_maximum <- max(df[, expl_vars])
    names(local_maximum) <- expl_vars
  }

  # local_count <- nrow(df)

  return(data.frame(
    min = local_minimum,
    max = local_maximum
    # count = local_count
  ))
}



# ******************************************************************************
# ---- Server/orchestrator functions ----
# ******************************************************************************

#' Compute the *global* range for each covariate.
#'
#' Params:
#'   ranges: list of *local* min's and max's, indexed by site nr.
#'
#' Return:
#'   ...
compute.global.range <- function(ranges) {
  global.min <- ranges[[1]]$min
  global.max <- ranges[[1]]$max

  if (length(ranges) > 1) {
    for (i in 2:length(ranges)) {
      global.min <- pmin(global.min, ranges[[i]]$min)
      global.max <- pmax(global.max, ranges[[i]]$max)
    }
  }

  names(global.max) <- rownames(ranges[[1]])
  names(global.min) <- rownames(ranges[[1]])

  return(list(
    global.min = global.min,
    global.max = global.max
  ))
}

#' Compute the combined (global) number of rows.
#'
#' Params:
#'   ranges: list of row counts, indexed by site nr.
#'
#' Return:
#'   ...
compute.global.nrows <- function(row.counts) {
  global.nrows <- row.counts[[1]]

  if (length(row.counts) > 1) {
    for (i in 2:length(row.counts)) {
      global.nrows <- global.nrows + row.counts[[i]]
    }
  }

  return(global.nrows)
}


#'
#' Stub for the distributed Chi-squared
#'
dchisq <- function(client, expl_vars, call.method=call) {
  writeln("Retrieving row counts ...")
  row.counts <- call.method(client, "get_row_count")
  writeln()

  writeln("Retrieving ranges ...")
  ranges <- call.method(client, "get_ranges", expl_vars)

  global.row.counts <- compute.global.nrows(row.counts)
  global.range <- compute.global.range(ranges)

  return(list(
    global.row.counts = global.row.counts,
    global.range = global.range
  ))
}

# ******************************************************************************
# ---- Infrastructure functions ----
# ******************************************************************************

#' Run the method requested by the server
#'
#' Params:
#'   df: data frame containing the *local* dataset
#'   input_data: string containing serialized JSON; JSON should contain
#'               the keys 'method', 'args' and 'kwargs'
#'
#' Return:
#'   Requested method's output
dispatch_RPC <- function(df, input_data) {
  # Determine which method was requested and combine arguments and keyword
  # arguments in a single variable
  input_data <- fromJSON(input_data)
  method <- sprintf("RPC_%s", input_data$method)

  input_data$args <- readRDS(textConnection(input_data$args))
  input_data$kwargs <- readRDS(textConnection(input_data$kwargs))

  args <- c(list(df), input_data$args, input_data$kwargs)

  # Call the method
  writeln(sprintf("Calling %s", method))
  result <- do.call(method, args)

  # Serialize the result
  writeln("Serializing result")
  fp <- textConnection("result_data", open="w")
  saveRDS(result, fp, ascii=T)
  close(fp)
  result <- result_data
  writeln("Serializing complete")

  return(result)
}


#' Wait for the results of a distributed task and return the task,
#' including results.
#'
#' Params:
#'   client: ptmclient::Client instance.
#'   task: list with the key id (representing the task id)
#'
#' Return:
#'   task (list) including results
wait_for_results <- function(client, task) {

  path = sprintf('/task/%s', task$id)

  while(TRUE) {
    r <- client$GET(path)

    if (content(r)$complete) {
      break

    } else {
      # Wait 30 seconds
      writeln("Waiting for results ...")
      Sys.sleep(5)
    }
  }

  path = sprintf('/task/%s?include=results', task$id)
  r <- client$GET(path)

  return(content(r))
}


#' Create a data structure used as input for a call to the distributed
#' learning infrastructure.
create_task_input = function(method, ...) {
  # Construct the input_data list from the ellipsis.
  arguments <- list(...)

  if (is.null(names(arguments))) {
    args <- arguments
    kwargs <- list()

  } else {
    args <- arguments[names(arguments) == ""]
    kwargs <- arguments[names(arguments) != ""]
  }

  # Serialize the argument values to ASCII
  fp <- textConnection("arg_data", open="w")
  saveRDS(args, fp, ascii=T)
  close(fp)

  # Serialize the keyword argument values to ASCII
  fp <- textConnection("kwarg_data", open="w")
  saveRDS(kwargs, fp, ascii=T)
  close(fp)

  # Create the data structure
  input_data <- list(
    method=method,
    args=arg_data,
    kwargs=kwarg_data
  )

  return(input_data)
}


#' Execute a method on the distributed learning infrastructure.
#'
#' This entails ...
#'  * creating a task and letting the hubs execute the method
#'    specified in the 'input' parameter
#'  * waiting for all results to arrive
#'  * deserializing each sites' result using readRDS
#'
#' Params:
#'   client: ptmclient::Client instance.
#'   method: name of the method to call on the distributed learning
#'           infrastructure
#'   ...: (keyword) arguments to provide to method. The arguments are serialized
#'        using `saveRDS()` by `create_task_input()`.
#'
#' Return:
#'   return value of called method
call <- function(client, method, ...) {
  # Create the json structure for the call to the server
  input <- create_task_input(method, ...)

  task = list(
    "name"="ChiSq",
    "image"="docker-registry.distributedlearning.ai/dl_chisq",
    "collaboration_id"=client$get("collaboration_id"),
    "input"=input,
    "description"=""
  )

  # Create the task on the server; this returs the task with its id
  r <- client$POST('/task', task)

  # Wait for the results to come in
  result_dict <- wait_for_results(client, content(r))

  # result_dict is a list with the keys _id, id, description, complete, image,
  # collaboration, results, etc. the entry "results" is itself a list with
  # one entry for each site. The site's actual result is contained in the
  # named list member 'result' and is encoded using saveRDS.
  sites <- result_dict$results
  results <- list()

  for (k in 1:length(sites)) {
    results[[k]] <- readRDS(textConnection(sites[[k]]$result))
  }

  return(results)
}


#' Mock an RPC call to all sites.
#'
#' Params:
#'   client: ptmclient::Client instance.
#'   method: name of the method to call on the distributed learning
#'           infrastructure
#'   ...: (keyword) arguments to provide to method. The arguments are serialized
#'        using `saveRDS()` by `create_task_input()`.
#'
#' Return:
#'   return value of called method
mock.call <- function(client, method, ...) {

  writeln(sprintf('** Mocking call to "%s" **', method))
  datasets <- client$datasets
  input_data <- create_task_input(method, ...)
  input_data <- toJSON(input_data)

  # Create a list to store the responses from the individual sites
  results <- list()

  # Mock calling the RPC method on each site
  for (k in 1:length(datasets)) {
    result <- dispatch_RPC(datasets[[k]], input_data)
    results[[k]] <- readRDS(textConnection(result))
  }

  writeln()
  return(results)
}



#' Entrypoint when excecuting this script using Rscript
#'
#' Wraps the docker input/output for `dispatch_RPC()`.
#' Deserialization/serialization is performed in `dipatch_RPC()` to enable
#' testing.
docker.wrapper <- function() {
  database_uri <- Sys.getenv("DATABASE_URI")
  writeln(sprintf("Using '%s' as database", database_uri))
  df <- read.csv(database_uri)

  # Read the contents of file input.txt into 'input_data'
  writeln("Loading input.txt")
  filename <- 'input.txt'
  input_data <- readChar(filename, file.info(filename)$size)

  writeln("Dispatching ...")
  result <- dispatch_RPC(df, input_data)

  # Write result to disk
  writeln("Writing result to disk .. ")
  writeLines(result, "output.txt")

  writeln("")
  writeln("[DONE!]")
}


# ******************************************************************************
# ---- main() ----
# ******************************************************************************
if (!interactive()) {
  docker.wrapper()
}

