################################################################################
# 
# Test de l'API ohsome sur Aix-en-Pce
#
################################################################################

library(tidyverse)
library(sf)
library(glue)
library(ohsome)

# Polygone de la commune en WGS84 (simplifié)
wkt <- "POLYGON ((5.36533 43.6248, 5.45892 43.618, 5.46382 43.5769, 5.49855 43.5617, 5.4795 43.5346, 5.5063 43.5322, 5.47739 43.5245, 5.45232 43.4758, 5.38748 43.4619, 5.3275 43.4889, 5.32934 43.4612, 5.30105 43.4461, 5.26967 43.4986, 5.29414 43.5195, 5.33951 43.5192, 5.3463 43.5362, 5.37772 43.5448, 5.36748 43.584, 5.3345 43.593, 5.36533 43.6248))"
input_sf <- st_sf(id = 1,  geometry = st_sfc(st_as_sfc(wkt), crs = 4326))

# Date de l'extraction
time <- "2025-01-01"

# Requête sur une statistique : ça marche sur Onyxia
query_cont <-
  ohsome_elements_count(
    boundary = input_sf, 
    filter = "highway=*"
  ) %>%
  ohsome::set_time(time)
ohsome_post(query_cont)
#> timestamp value
#> 1 2025-01-01 22454

# Requête simple sur une longueur : ça marche sur Onyxia
query_length <-
  ohsome_elements_length(
    boundary = input_sf,
    filter = "highway=* and geometry:line",
    time = time
  ) %>%
  ohsome::set_grouping("tag") %>%
  ohsome::set_groupByKey("highway")
ohsome_post(query_length)
#> timestamp highway.residential highway.service highway.track highway.footway highway.unclassified highway.path
#> 1 2025-01-01            309136.8        521384.8      322244.6        137642.7             242996.1     223525.7
#> highway.tertiary highway.secondary highway.primary highway.living_street highway.trunk highway.cycleway highway.steps
#> 1         110938.1          102138.6        37646.42              16914.77      28378.75         23141.25       3138.79
#> highway.road highway.pedestrian highway.motorway highway.construction highway.motorway_link highway.primary_link
#> 1        26.01           13705.66         40460.14               231.83              16933.44               461.26
#> highway.trunk_link highway.secondary_link highway.tertiary_link highway.platform highway.corridor highway.raceway
#> 1           12080.49                 982.05                145.63           291.07            57.44          526.19
#> highway.busway
#> 1        2412.57


# Requête sur une géométrie : ça ne marche pas sur Onyxia
query_sf <-ohsome_elements_geometry(boundary = input_sf, filter = "highway=*") |>
  ohsome::set_time(time) 
output_sf <- query_sf |> ohsome_post()
#> Error in ohsome_post(query_sf) : Forbidden (HTTP 403).
