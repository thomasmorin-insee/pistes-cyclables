library(tidyverse)
library(arrow)
library(duckdb)
library(DBI)

# Coffre
BUCKET <- "zg6dup"

# Date de l'extraction
annee <- "2025"

# Dossier cible
dir_output_ac <- paste0("schema-ac-com-par-depts-", annee)

# liste des tags utiles 
file_tags <- "./pistes-cyclables/_tags-osm.R"
liste_tags <- eval(eval(parse(text = paste(readLines(file_tags), collapse = "\n"))))

# Requête sql
file_sql <- "./pistes-cyclables/_schema-amenagements-cyclables.sql"
requete_sql <- readChar(file_sql, file.info(file_sql)$size)

liste_dep <- c(
  "01",
  "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", 
  "13",
  "14", "15", "16", "17", "18", "19", 
  "2A", "2B", "21", "22", "23", "24", "25", "26", "27", "28", "29",
  "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
  "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
  "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
  "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
  "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
  "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
  "90", "91", "92", "93", "94", "95", "971", "972", "973", "974", "976"
)

# Boucle sur les départements
for(code_dep in liste_dep) {

  message("Chargement des voiries en ", annee, ", département : ", code_dep)
  longueurs_com <- aws.s3::s3read_using(
    FUN = arrow::read_parquet,
    object = paste0("api-ohsome-com-par-depts-", annee, "/lines-com-dep",code_dep ,".parquet"),
    bucket = BUCKET,
    opts = list("region" = "")
  )
  
  # setdiff(liste_tags, colnames(longueurs_com))
  # intersect(liste_tags, colnames(longueurs_com))
  
  # Ajoute les variables manquantes pour le sql
  for(tag in liste_tags) {
    if(!tag %in% colnames(longueurs_com)) {
      longueurs_com <- longueurs_com %>% mutate({{tag}} := "")
    }
  }
  
  # Remplace les "." par des "_" (harmonisation avec le sql)
  longueurs_com <- longueurs_com %>% rename_with(~str_replace_all(.x, "\\.", "_"))
  
  # Connexion duckdb
  # DBI::dbDisconnect(con, shutdown = TRUE)
  con <- DBI::dbConnect(duckdb::duckdb())
  
  # Applique le schéma des aménagements cyclables
  DBI::dbExecute(con, "DROP VIEW IF EXISTS data_for_sql")
  duckdb::duckdb_register(con, "data_for_sql", longueurs_com)
  dt <- DBI::dbGetQuery(con, paste0("
    -- Requête sql du schéma des aménagements cyclables",
    requete_sql, "
    -- Filtre pour les routes potentiellement cyclables
    CASE
    		WHEN 
    			(highway IN ('trunk', 'living_street', 'primary', 'secondary', 'tertiary', 'residential', 'busway'))
    			OR (highway IN ('unclassified', 'service') AND (
    			    smoothness IN ('excellent', 'good') 
    			    OR tracktype = 'grade1' 
    			    OR surface IN  ('asphalt', 'paving_stones', 'chipseal', 'concrete')
    			))
    		THEN 'yes'
    		ELSE 'false'
    END filtre_pc,
    -- Sens de circulation pour les routes potentiellement cyclables
    CASE
    	WHEN oneway IN ('yes', 'reversible', '-1', 'true', '1', 'reverse') THEN 'UNIDIRECTIONNEL'
    	ELSE 'BIDIRECTIONNEL'
    END sens_pc, 
    codgeo, libgeo, longueur,
    FROM data_for_sql
  "))
  
  # Simplification
  dt <- dt %>% group_by(pick(-"longueur")) %>% 
    summarise(longueur = sum(longueur, na.rm = TRUE), .groups = "drop")
  
  message("Enregistrement du fichier ", dir_output_ac, "/dept-", code_dep, ".parquet")
  aws.s3::s3write_using(
    dt,
    FUN = arrow::write_parquet,
    object = paste0(dir_output_ac, "/dept-", code_dep, ".parquet"),
    bucket = BUCKET,
    opts = list("region" = "")
  )
}
