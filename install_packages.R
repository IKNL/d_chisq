packages <- c(
  "abind",
  "dplyr",
  "httr",
  "rjson",
  "namespace",
  "devtools"
)

install.packages(packages)

library("devtools")
install_github("mellesies/vantage.infrastructure")