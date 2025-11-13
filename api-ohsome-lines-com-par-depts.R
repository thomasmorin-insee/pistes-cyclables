library(tidyverse)
library(sf)
library(aws.s3)
library(arrow)
library(httr)

# Date de l'extraction
date <- "2025-01-01"

# Code du département
code_dep <- "13"

# Coffre
BUCKET <- "zg6dup"

# liste des tags utiles 
liste_tags <- eval(eval(parse(text = paste(readLines("./pistes-cyclables/_tags-osm.R"), collapse = "\n"))))

# Convertir poly_commune en FeatureCollection GeoJSON
poly_com_i$id <- 1  # ou un identifiant unique si vous avez plusieurs features
fc_geojson <- geojsonsf::sf_geojson(select(poly_com_i, id), simplify = FALSE)

# Requête dans l'api ohsome à partir d'un polygone de commune
requete_ohsome <- function(objet_sf) {
  
  # Convertir poly_commune en FeatureCollection GeoJSON
  objet_sf$id <- 1  # ou un identifiant unique si vous avez plusieurs features
  fc_geojson <- geojsonsf::sf_geojson(select(objet_sf, id), simplify = FALSE)
  
  # Requête
  r <- httr::POST(
    url = "https://api.ohsome.org/v1/elements/geometry",
    body = list(
      bpolys = fc_geojson,
      filter = "highway=*",
      time = date,
      properties = "tags",
      showMetadata = FALSE
    ),
    encode = "multipart"
  )
  
  # Dans un objet sf
  response_text <- httr::content(r, "text")
  sf_object <- st_read(response_text, quiet = TRUE)
  return(sf_object)
}

# Polygone des communes dans open street map
poly_com <- aws.s3::s3read_using(
  FUN = arrow::read_parquet,
  object = paste0("osm-polygones-com-geo2025/poly-com-dep",code_dep ,"-crs4356-geo2025.parquet"),
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

# Boucle sur les communes (peut être relancée si erreur)
for (i in seq_along(liste_codgeo)) {
  
  # Relance
  if(i <= compteur_traitement) next
  
  # Informations de la commune
  codgeo <- liste_codgeo[i]
  poly_com_i <- sf_poly_coms %>% filter(codgeo == !!codgeo)
  libgeo <- poly_com_i$libgeo
  message("Traitement ", i, "/", length(liste_codgeo), " : ",  codgeo, " ", libgeo)

  # Requête : corrige le polygone si besoin
  sf_object <- tryCatch(
    expr = { requete_ohsome(poly_com_i)}, 
    error = function(e) {
      message("... Nétoyage avec st_make_valid()...")
      poly_com_i <- st_make_valid(poly_com_i)
      res <- tryCatch(    
        expr = { requete_ohsome(poly_com_i)}, 
        error = function(e) {
          # Nétoyage avec st_buffer
          message("... Nétoyage avec st_buffer()...")
          poly_com_i <- st_buffer(poly_com_i, 0)
          return(requete_ohsome(poly_com_i))
        })
      return(res)
    })
  
  # Lambert 93
  sf_object <- sf_object %>% st_transform(crs=2154)
  
  # Calcul des longueurs par type
  sf_object$longueur <- st_length(sf_object)
  
  # Longueur selon le type de voirie
  longueur_voirie <- sf_object %>% 
    
    # Variables utiles
    select(any_of(c(liste_tags, "longueur"))) %>%
    st_drop_geometry() %>%
    
    # Longueur selon la description de la voirie
    group_by(pick(-"longueur")) %>% 
    summarise(longueur = sum(longueur, na.rm = TRUE), .groups = "drop") %>%
    
    # Information de la commune
    mutate(codgeo = !!codgeo, libgeo = !!libgeo, .before = 1)
  
  
  # Résultat
  liste_resultats[[codgeo]] <- longueur_voirie
  compteur_traitement <- i
}
