################################################################################
#   
# Longueur des voies des communes par dépt : schéma des aménagements cyclables
# 
# Entrée : at36vc/api-ohsome-com-par-depts-{annee}/lines-com-dep{code_dep}.parquet
# Sortie : at36vc/longueur-com/longueur-com-{annee}.parquet
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
input_file <- "at36vc/api-ohsome-com-par-depts-{annee}/lg-com-dep{code_dep}.parquet"
output_file <- "at36vc/longueur-com/longueur-com-{annee}.parquet"
output_file_light <- "at36vc/longueur-com/longueur-com-{annee}-light.parquet"


# Connexion duckdb
# DBI::dbDisconnect(con, shutdown = TRUE)
con <- DBI::dbConnect(duckdb::duckdb())

# liste des tags utiles 
file_tags <- "_tags-osm.R"
liste_tags <- eval(parse(text = paste(readLines(file_tags), collapse = "\n")))

# Requête sql
file_sql <- "_schema-amenagements-cyclables.sql"
requete_sql <- readChar(file_sql, file.info(file_sql)$size)

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
  "90", "91", "92", "93", "94", "95",
  "971", "972", "973", "974"
)

# Boucle sur les départements
dt <- tibble(dep = character(), codgeo = character())
for(code_dep in liste_dep) {

  message("Chargement du fichier ", glue(input_file))
  dt_dep <- aws.s3::s3read_using(
    FUN = arrow::read_parquet,
    object = glue(input_file),
    bucket = BUCKET,
    opts = list("region" = "")
  )
  
  # Simplification
  dt_dep <- dt_dep %>% group_by(pick(-"longueur")) %>% 
    summarise(longueur = sum(longueur, na.rm = TRUE), .groups = "drop")
  
  dt_dep <- dt_dep %>% mutate(dep = !!code_dep, .after = codgeo) 
  
  dt <- bind_rows(dt, dt_dep)
}


message("Enregistrement du fichier ", glue(output_file))
aws.s3::s3write_using(
  dt,
  FUN = arrow::write_parquet,
  object = glue(output_file),
  bucket = BUCKET,
  opts = list("region" = "")
)

tag_non_present <- c()
for(tag in liste_tags) {
  # Vérifie si le tag existe
  if(!tag %in% colnames(dt)) {
    tag_non_present <- c(tag_non_present, tag)
  } else {
    # Vérifie qu'il n'y a pas de chaîne vide
    nb_chaine_vide <- dt %>% filter(.data[[tag]] == "") %>% nrow()
    if(nb_chaine_vide > 0) {
      warning(glue("tag {tag} : {nb_chaine_vide} chaînes vide"), immediate. = TRUE)
    }
  }
}
warning("tags non présent en ", annee, " :\n - ", 
        paste0(tag_non_present, collapse = " \n - "))

# tags non présent en 2025 :
#   - primary 
# - secondary 
# - tertiary 
# - cycleway.both.est_width 
# - smoothness.left 
# - smoothness.right 
# - osm_timestamp 
# - route_icn_ref 
# - route_ncn_ref 
# - route_rcn_ref 
# - route_lcn_ref 


DBI::dbDisconnect(con)
