library(tidyverse)
library(sf)
library(aws.s3)
library(arrow)
library(httr)
library(glue)
library(ohsome)

# Coffre
BUCKET <- "zg6dup"

# Date de l'extraction
annee <- "2025"
date <- glue("{annee}-01-01")

# Input / output
input_api <- "at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet"
output_api <- "at36vc/api-ohsome-com-par-depts-{annee}/lg-com-dep{code_dep}.parquet"
# output_api <- "api-ohsome-com-par-depts-{annee}/lines-com-dep{code_dep}.parquet" # Ancien dossier pour 2025

# liste des tags utiles 
liste_tags <- eval(parse(text = paste(readLines("_tags-osm.R"), collapse = "\n")))


code_dep <- "13"

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
# Traitement 1/134 : 13001 Aix-en-Provence

# Parmaètres
input_sf <- poly_com_i
time <- date

# Requête sur une statistique : ça marche sur Onyxia
query_cont <- ohsome_elements_count(boundary = input_sf, filter = "highway=*") |>
  ohsome::set_time(time)
output_count <- ohsome_post(query_cont)
output_count
# timestamp value
# 1 2025-01-01 22207

# Requête simple sur une longueur : ça marche sur Onyxia
query_length <-
  ohsome::ohsome_elements_length(
    boundary = input_sf,
    filter = "highway=* and geometry:line",
    time = glue("{annee}-01-01")
  ) %>%
  ohsome::set_grouping("tag") %>%
  ohsome::set_groupByKey("highway")
output_length <-  ohsome_post(query_length)
output_length
# timestamp highway.residential highway.service highway.track highway.footway highway.unclassified highway.path
# 1 2025-01-01            305139.2        522060.1      313884.2        136647.5             242405.1     208811.8
# highway.tertiary highway.secondary highway.primary highway.living_street highway.trunk highway.cycleway highway.steps
# 1         108059.2          98763.53        37302.44              16914.77      27404.43         22994.05       3157.28
# highway.road highway.pedestrian highway.motorway highway.construction highway.motorway_link highway.primary_link
# 1        26.01           13678.19         39305.91               231.83              15809.69               461.26
# highway.trunk_link highway.secondary_link highway.tertiary_link highway.platform highway.corridor highway.busway
# 1           11339.01                 982.05                145.63           286.27            57.44        2412.57


# Requête sur une géométrie : ça ne marche pas sur On
query_sf <-ohsome_elements_geometry(boundary = input_sf, filter = "highway=*") |>
  ohsome::set_time(time) 
output_sf <- query_sf |> ohsome_post()
#> Error in ohsome_post(query_sf) : Forbidden (HTTP 403).

################################################################################
# Objet SF
class(input_sf)

input_sf
input_sf$geometry

# Géométrie simplifiée :
input_sf_simple <- input_sf |>
  sf::st_transform(2154) |>
  sf::st_simplify(dTolerance = 1000) |>  # tolérance en mètres, à ajuster
  sf::st_transform(4326)

wkt_simple <- sf::st_as_text(input_sf_simple$geometry, digits = 6)
cat(wkt_simple)
# POLYGON ((5.36533 43.6248, 5.45892 43.618, 5.46382 43.5769, 5.49855 43.5617, 5.4795 43.5346, 5.5063 43.5322, 5.47739 43.5245, 5.45232 43.4758, 5.38748 43.4619, 5.3275 43.4889, 5.32934 43.4612, 5.30105 43.4461, 5.26967 43.4986, 5.29414 43.5195, 5.33951 43.5192, 5.3463 43.5362, 5.37772 43.5448, 5.36748 43.584, 5.3345 43.593, 5.36533 43.6248))

sf::st_is_valid(input_sf_simple)  # TRUE
mapview::mapview(input_sf_simple)  # pour visualiser rapidement le résultat
mapview::mapview(input_sf)  # pour visualiser rapidement le résultat

# Pour recréer le polygone :
wkt_simple <- "POLYGON ((5.36533 43.6248, 5.45892 43.618, 5.46382 43.5769, 5.49855 43.5617, 5.4795 43.5346, 5.5063 43.5322, 5.47739 43.5245, 5.45232 43.4758, 5.38748 43.4619, 5.3275 43.4889, 5.32934 43.4612, 5.30105 43.4461, 5.26967 43.4986, 5.29414 43.5195, 5.33951 43.5192, 5.3463 43.5362, 5.37772 43.5448, 5.36748 43.584, 5.3345 43.593, 5.36533 43.6248))"

input_sf_simple <- sf::st_sf(
  id = 1,
  geometry = sf::st_sfc(sf::st_as_sfc(wkt_simple), crs = 4326)
)

