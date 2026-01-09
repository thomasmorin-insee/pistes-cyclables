################################################################################
#   
# Longueur des voies des communes par dépt : schéma des aménagements cyclables
# 
# Entrée : at36vc/api-ohsome-com-par-depts-{annee}/lines-com-dep{code_dep}.parquet
# Sortie : at36vc/schema-ac-com-par-depts-{annee}/dept-{code_dep}.parquet
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
output_file <- "at36vc/schema-ac-com/schema-ac-com-{annee}.parquet"

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
  longueurs_com <- aws.s3::s3read_using(
    FUN = arrow::read_parquet,
    object = glue(input_file),
    bucket = BUCKET,
    opts = list("region" = "")
  )
  
  # setdiff(liste_tags, colnames(longueurs_com))
  # intersect(liste_tags, colnames(longueurs_com))
  
  for(tag in liste_tags) {
    # Ajoute les variables manquantes pour le sql
    if(!tag %in% colnames(longueurs_com)) {
      longueurs_com <- longueurs_com %>% mutate({{tag}} := as.character(NA))
    }
    # Vérifie qu'il n'y a pas de chaîne vide
    nb_chaine_vide <- longueurs_com %>% filter(.data[[tag]] == "") %>% nrow()
    if(nb_chaine_vide > 0) {
      warning(glue("{code_dep} : tag {tag} : {nb_chaine_vide} chaînes vide"), immediate. = TRUE)
    }
  }

  # Remplace les "." par des "_" (harmonisation avec le sql)
  longueurs_com <- longueurs_com %>% rename_with(~str_replace_all(.x, "\\.", "_"))
  
  # Applique le schéma des aménagements cyclables
  DBI::dbExecute(con, "DROP VIEW IF EXISTS data_for_sql")
  duckdb::duckdb_register(con, "data_for_sql", longueurs_com)
  dt_dep <- DBI::dbGetQuery(con, paste0("
    -- Requête sql du schéma des aménagements cyclables",
    requete_sql, "
    -- Filtre pour les routes potentiellement cyclables (on retire trunk)
    CASE
    		WHEN 
    			(highway IN ('primary', 'secondary', 'busway'))
    --		OR (highway IN ('tertiary', 'residential', 'living_street', 'unclassified', 'trunk', 'service')
      		OR (highway IN ('tertiary', 'residential', 'living_street', 'unclassified', 'service')
    			    AND (access IS NULL OR access != 'no')
    			    AND (motor_vehicle IS NULL OR motor_vehicle != 'no')
    			    AND (motorcar IS NULL OR motorcar != 'no')
    			    AND (surface IS NULL OR surface = 'asphalt')
    			    AND (smoothness IS NULL OR smoothness IN ('excellent', 'good'))
    			    AND (tracktype IS NULL OR tracktype = 'grade1') 
    			    AND (bicycle IS NULL OR bicycle != 'designated')
    			    AND (designation IS NULL OR designation != 'greenway')
    			)
    		THEN 'true'
    		ELSE 'false'
    END filtre_pc,
    -- Sens de circulation pour les routes potentiellement cyclables
    CASE
    	WHEN oneway IN ('yes', 'reversible', '-1', 'true', '1', 'reverse') THEN 'UNIDIRECTIONNEL'
    	ELSE 'BIDIRECTIONNEL'
    END sens_pc, 
    highway, bicycle, designation, codgeo, libgeo, longueur,
    FROM data_for_sql
  "))
  
  # Simplification
  dt_dep <- dt_dep %>% group_by(pick(-"longueur")) %>% 
    summarise(longueur = sum(longueur, na.rm = TRUE), .groups = "drop") %>%
    mutate(dep = !!code_dep)
  
  dt <- bind_rows(dt, dt_dep)
  last_dep <- code_dep
}

message("Enregistrement du fichier ", glue(output_file))
aws.s3::s3write_using(
  dt,
  FUN = arrow::write_parquet,
  object = glue(output_file),
  bucket = BUCKET,
  opts = list("region" = "")
)

DBI::dbDisconnect(con)
