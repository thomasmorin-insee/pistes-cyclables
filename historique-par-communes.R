# Library
library(osmdata)
library(sf)
library(dplyr)
library(aws.s3)
library(arrow)
library(httr)
library(geojsonsf)

# Tags utilisés pour reconstituer les pistes cyclables
liste_tags <- eval(eval(parse(text = paste(readLines("_tags-osm.R"), collapse = "\n"))))

# Départements et nom OSM
liste_depts <- eval(eval(parse(text = paste(readLines("_depts_osm.R"), collapse = "\n"))))

# Initialisation
code_commune <- "13001"
date <- "2025-11-10"
code_dep <- substr(code_commune,1, 2)

# Fichier à enregistrer
BUCKET <- "zg6dup"
FILE <- paste0("osm-historique-commune/longueur-",code_commune ,"-", date, ".parquet")

# Récupère les objets "commune" dans le département "Nom-du-dépt, France"
communes_osm <- opq(paste0(liste_dep_osm[code_dep], ", France")) %>%
# communes_osm <- opq(paste0("Aix-en-Provence", ", France")) %>%
  add_osm_feature(key = "boundary", value = "administrative") %>%
  add_osm_feature(key = "admin_level", value = "8") %>%
  osmdata_sf()

# On ne garde que les communes dans le département
communes_sf <- communes_osm$osm_multipolygons %>% 
  filter(substr(`ref:INSEE`,1,2) == !!code_dep)

# Polygone de la commune
poly_commune <- communes_sf %>% filter(`ref:INSEE` == code_commune)
# plot(poly_commune)

# Convertir le polygone en GeoJSON (format attendu par l'API ohsome)
poly_geojson <- geojsonsf::sf_geojson(poly_commune %>% select(geometry))

poly_geojson
#> {"type":"MultiPolygon","coordinates":[[[[5.365328,43.624773],[5.3703193,43.6253839],...

# Convertir poly_commune en FeatureCollection GeoJSON
poly_commune$id <- 1  # ou un identifiant unique si vous avez plusieurs features
fc_geojson <- geojsonsf::sf_geojson(select(poly_commune, id), simplify = FALSE)


# Construire le corps de la requête
r <- POST(
  url = "https://api.ohsome.org/v1/elements/geometry",
  body = list(
    bpolys = fc_geojson,
    filter = "highway=*",
    time = "2017-01-01",
    properties = "tags",
    showMetadata = FALSE
  ),
  encode = "multipart"
)

response_text <- content(r, "text")
sf_object <- st_read(response_text, quiet = TRUE)

setdiff(liste_tags, colnames(sf_object))
intersect(liste_tags, colnames(sf_object))
