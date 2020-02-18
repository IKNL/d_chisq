<img src="https://github.com/IKNL/guidelines/blob/master/resources/logos/vantage6.png?raw=true" width=200 align="right">

# d_chisq
## About
Implementation of the Chi-Squared algorithm that can be used in [VANTAGE6](https://github.com/IKNL/VANTAGE6).

## Usage
Example use:
```R
source('install_packages.R')

# Sourcing this also loads the `vtg` namespace, which is an alias for 
# the package `vantage.infrastructure`.
source('dl_chisq.R')

setup.client <- function() {
  # Define parameters
  username <- "username@example.com"
  password <- "password"
  collaboration_id <- 1
  host <- 'https://api-test.distributedlearning.ai'
  api_path <- ''
  
  # Create the client
  client <- vtg::Client(host, username, password, collaboration_id, api_path)
  client$authenticate()

  return(client)
}

# Create a client
client <- setup.client()

# The explanatory variables should correspond to a single, 
# one-hot encoded variable.
expl_vars <- c()
result <- dchisq(client, expl_vars)
```

Example use for testing:
```R
source('install_packages.R')

# Sourcing this also loads the `vtg` namespace, which is an alias for 
# the package `vantage.infrastructure`.
source('dl_chisq.R')

# Load a dataset
df <- read.csv("SeerMetHeader.csv")

# Select the columns corresponding to the variable "Mar"
expl_vars <- c("Mar2","Mar3","Mar4","Mar5","Mar9")

# Run the Chi^2 analysis
dchisq.mock(df, expl_vars)
```
