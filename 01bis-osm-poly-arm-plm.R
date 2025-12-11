################################################################################
#
# Remplace les polygones des communes par ceux des arrondissements pour PLM
# 
# Entrée : at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet
# Sortie : at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet
#
################################################################################

install.packages("osmdata")
library(tidyverse)
library(osmdata)
library(sf)
library(aws.s3)
library(arrow)

# Coffre
BUCKET <- "zg6dup"

input_polygones <- "at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet"

plm_name <- list("Paris", "Marseille", "Lyon")
plm_arm <- list(
  "Paris" = c("75101", "75102", "75103", "75104", "75105",
    "75106", "75107", "75108", "75109", "75110",
    "75111", "75112", "75113", "75114", "75115",
    "75116", "75117", "75118", "75119", "75120"),
  "Marseille"= c("13201", "13202", "13203", "13204", "13205",
    "13206", "13207", "13208", "13209", "13210",
    "13211", "13212", "13213", "13214", "13215", "13216"),
  "Lyon" = c("69381", "69382", "69383", "69384", "69385",
    "69386", "69387", "69388", "69389")
)
plm_code <- list("Paris" = "75056", "Marseille"= "13055", "Lyon" = "69123")
plm_dep <- list("Paris" = "75", "Marseille"= "13", "Lyon" = "69")

plm <- "Paris"
plm <- "Marseille"
plm <- "Lyon"

# Définir la zone de recherche (Paris)
bbox_plm <- getbb(glue("{plm}, France"))

# Requête pour extraire les polygones des arrondissements de Paris
osmdata_plm <- opq(bbox = bbox_plm) %>%
  add_osm_feature(key = "boundary",
                  value = "administrative") %>%
  add_osm_feature(key = "admin_level",
                  value = "9") %>%
  osmdata_sf()

# Polygone de la commune
poly_plm <- osmdata_plm$osm_multipolygons %>%
  select(osm_id, codgeo = `ref:INSEE`, libgeo = name) %>% 
  filter(codgeo %in% plm_arm[[plm]])

poly_plm %>% select(libgeo) %>% plot()

# En data frame
df_poly_plm <- poly_plm %>%
  mutate(geometry_wkt = st_as_text(geometry)) %>%
  st_drop_geometry()

# Polygone des communes du département
code_dep <- plm_dep[[plm]]
poly_com <- aws.s3::s3read_using(
  FUN = arrow::read_parquet,
  object = glue(input_polygones),
  bucket = BUCKET,
  opts = list("region" = "")
)

# Remplace la commune par les arrondissements
code_com_old <- plm_code[[plm]]
poly_com <- poly_com %>%
  bind_rows(df_poly_plm) %>%
  filter(codgeo != !!code_com_old)

# Format sf
# sf_poly_coms <- poly_com %>%
#   rename(geometry = geometry_wkt) %>%
#   st_as_sf(wkt = "geometry", crs = st_crs(4326))
# sf_poly_coms %>% select(libgeo) %>% plot()

# Enregistrement
message(glue(input_polygones))
aws.s3::s3write_using(
  poly_com,
  FUN = arrow::write_parquet,
  object = glue(input_polygones),
  bucket = BUCKET,
  opts = list("region" = "")
)


