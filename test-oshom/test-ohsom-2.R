
library(tidyverse)
library(sf)
library(aws.s3)
library(arrow)
library(httr)
library(glue)

# Coffre
BUCKET <- "zg6dup"

# Date de l'extraction
annee <- "2023"
date <- glue("{annee}-01-01")

# Input / output
input_api <- "at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet"
output_api <- "at36vc/api-ohsome-com-par-depts-{annee}/lg-com-dep{code_dep}.parquet"
# output_api <- "api-ohsome-com-par-depts-{annee}/lines-com-dep{code_dep}.parquet" # Ancien dossier pour 2025

# liste des tags utiles 
liste_tags <- eval(parse(text = paste(readLines("_tags-osm.R"), collapse = "\n")))


code_dep <- "70"

message("Polygone des communes, fichier ", glue(input_api))
poly_com <- aws.s3::s3read_using(
  FUN = arrow::read_parquet,
  object = glue(input_api),
  bucket = BUCKET,
  opts = list("region" = "")
)

# Format sf
sf_poly_coms <- poly_com %>% 
  rename(geometry = geometry_wkt) %>%
  st_as_sf(wkt = "geometry", crs = st_crs(4326))

# Liste des communes 
liste_codgeo <- sort(poly_com$codgeo)
if(length(liste_codgeo) != length(poly_com$codgeo)) {
  warning("Communes en doublon !")
}

# Initialise le compteur et la liste 
compteur_traitement <- 0
liste_resultats <- list()


i <- seq_along(liste_codgeo)[1]


# Informations de la commune
codgeo <- liste_codgeo[i]
poly_com_i <- sf_poly_coms %>% filter(codgeo == !!codgeo)
libgeo <- poly_com_i$libgeo
message("Traitement ", i, "/", length(liste_codgeo), " : ",  codgeo, " ", libgeo)


# Parmaètres
input_sf <- poly_com_i
time <- date

# Requête sur une géométrie
query_sf <-ohsome_elements_geometry(boundary = input_sf, filter = "highway=*") |>
  ohsome::set_time(time) 
output_sf <- query_sf |> ohsome_post()
#> Error in ohsome_post(query_sf) : Forbidden (HTTP 403).

# Requête sur une statistique
query_cont <- ohsome_elements_count(boundary = input_sf, filter = "highway=*") |>
  ohsome::set_time(time)
output_count <- ohsome_post(query_cont)
output_count
# timestamp value
# 1 2023-01-01    25
