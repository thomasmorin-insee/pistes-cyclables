################################################################################
#   
# Schéma des aménagements cyclables
# 
# Entrée : "at36vc/schema-ac-com/schema-ac-com-{annee}.parquet"
#
################################################################################

library(tidyverse)
library(arrow)
library(duckdb)
library(DBI)
library(glue)

# Coffre
BUCKET <- "zg6dup"

# Input/output
annee <- 2025
input_file <- "at36vc/schema-ac-com/schema-ac-com-{annee}.parquet"

# Connexion duckdb
# DBI::dbDisconnect(con, shutdown = TRUE)
con <- DBI::dbConnect(duckdb::duckdb())

# Longueur des voies cyclables
longueurs_com <- aws.s3::s3read_using(
  FUN = arrow::read_parquet,
  object = glue(input_file),
  bucket = BUCKET,
  opts = list("region" = "")
)

# Création d'une vue pour appliquer le schéma sql
DBI::dbExecute(con, "DROP VIEW IF EXISTS table_sql")
duckdb::duckdb_register(con, "data_for_sql", longueurs_com)

# Requête sql des aménagements cyclables sur la base des longueur par commune
file_sql <- "sql/ame_insee.sql"
requete_sql <- readChar(file_sql, file.info(file_sql)$size)

# Applique le schéma
tbl_parquet <- tbl(con, sql(requete_sql))
colnames(tbl_parquet)

