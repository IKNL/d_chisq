#!/usr/bin/env Rscript

#' ----------------------------------------------------------------------------
#' title: dl_chisq.R
#' description:
#'   Implementation of the Chi Squared algorithm.
#'
#'   This implementation can be used with the Vantage federated
#'   learning infrastructure (see https://github.com/IKNL/vantage).
#'
#' authors:
#'   Frank Martin <f.martin@iknl.nl>
#'   Gijs Geleijnse <g.geleijnse@iknl.nl>
#'   Melle Sieswerda <m.sieswerda@iknl.nl>
#' date: 20-nov-2019
#' license: MIT License
#' ----------------------------------------------------------------------------

# This seems to be equivalent to "import x as y"
library(namespace)
tryCatch({
  invisible(registerNamespace('vtg', loadNamespace('vantage.infrastructure')))
}, error = function(e) {
  vtg::writeln("Package 'vantage.infrastructure' already loaded.")
})


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

#' Return counts for each (binned) value for a variable.
#'
#' Chi2 analysis is run for each variable independently. This function expects
#' the dataset to be 1-hot encoded. As such, we can sum over rows/columns to get
#' counts.
#'
#' Return: counts for each variable (vector)
RPC_colSums <- function(df, expl_vars) {
  # S[i]: nr. of occurrences in dataset S, bin i (scalar)
  # S: vector of occurrences for all bins
  # sum(S): total number of occurrences across bins; this should correspond
  # to the number of rows, since the chi2 should be run against a single
  # variable.
  return(colSums(df[expl_vars]))
}


# ******************************************************************************
# ---- Server/orchestrator functions ----
# ******************************************************************************

compute.chisq <- function(s, d) {
  x <- rbind(s, d)
  result <- chisq.test(x)

  # return(result)

  # The 'unname' calls are for compatability with the alternative implementation.
  return(list(
    X2 = unname(result$statistic),
    df = unname(result$parameter),
    p.value = unname(result$p.value)
  ))
}

#' Run a distributed Chi^2 analysis
#'
#' FIXME: need to handle > 2 sites.
dchisq <- function(client, expl_vars) {
  writeln("Retrieving colSums")

  client$set.task.image(
    "docker-registry.distributedlearning.ai/dl_chisq",
    task.name="ChiSq"
  )

  colsums <- client$call("colSums", expl_vars)

  if (length(colsums) == 1) {
    msg <- paste(
      "Computing the Chi^2 statistic between sites, requires at least",
      "two sites!"
    )

    stop(msg)
  }

  writeln("Computing statistics")

  s <- colsums[[1]]$result
  d <- colsums[[2]]$result
  result <- compute.chisq(s, d)
  return(result)
}

#' Mock a distributed Chi^2 analysis.
dchisq.mock <- function(df, expl_vars, splits=5) {
  datasets <- list()

  for (k in 1:splits) {
    datasets[[k]] <- df[seq(k, nrow(df), by=splits), ]
  }

  client <- vtg::MockClient(datasets)
  results <- dchisq(client, expl_vars)
  return(results)
}


# ******************************************************************************
# ---- main() ----
# ******************************************************************************
if (!interactive()) {
  vtg::docker.wrapper()
}