
library(tidyverse)
library(glue)


# Coffre
BUCKET <- "zg6dup"

# Départements OSM

liste_dep <- c(
  "01", "02", "03", "04", "05", "06", "07", "08", "09", 
  "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
  "2A", "2B", "21", "22", "23", "24", "25", "26", "27", "28", "29",
  "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
  "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
  "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
  "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
  "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
  "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
  "90", "91", "92", "93", "94", "95", "971", "972", "973", "974", "976"
)

# Déplacer les output
if(TRUE) {
  old <- "osm-polygones-com-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet"
  new <-  "at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet"
  for(code_dep in liste_dep) {
    data <- aws.s3::s3read_using(
      FUN = arrow::read_parquet,
      object = glue(old),
      bucket = BUCKET,
      opts = list("region" = "")
    )
    aws.s3::s3write_using(
      data,
      FUN = arrow::write_parquet,
      object = glue(new),
      bucket = BUCKET,
      opts = list("region" = "")
    )
    print(glue(new))
  }
}
