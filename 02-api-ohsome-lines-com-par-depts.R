################################################################################
#   
# Longueur des voies des communes par département d'après l'API ohsome
# 
# Entrée : at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet
# Sortie : at36vc/api-ohsome-com-par-depts-{annee}/lg-com-dep{code_dep}.parquet
#
################################################################################

library(tidyverse)
library(sf)
library(aws.s3)
library(arrow)
library(httr)
library(glue)

# Coffre
BUCKET <- "zg6dup"

# Date de l'extraction
annee <- "2020"
date <- glue("{annee}-01-01")

# Input / output
input_api <- "at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet"
output_api <- "at36vc/api-ohsome-com-par-depts-{annee}/lg-com-dep{code_dep}.parquet"
# output_api <- "api-ohsome-com-par-depts-{annee}/lines-com-dep{code_dep}.parquet" # Ancien dossier pour 2025

# liste des tags utiles 
liste_tags <- eval(parse(text = paste(readLines("_tags-osm.R"), collapse = "\n")))

# Requête dans l'api ohsome à partir d'un polygone de commune
# Renvoie un objet sf
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

# Liste des départements
liste_dep <- c(
  # "01", "02", "03", "04", "05", "06", "07", "08", "09",
  # "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
  # "2A", "2B", "21", "22", "23", "24", "25", "26", "27", "28", "29",
  # "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
  # "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
  # "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
  # "60", "61", "62", "63", "64", "65", "66",
  # "67", "68",
  "69",
  "70", "71", "72", "73", "74",
  "75"
  , "76", "77", "78", "79",
  "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
  "90", "91", "92", "93", "94", "95",
  "971"
  , "972", "973", "974", "976"
)

for(code_dep in liste_dep) {
  
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
  
  # Boucle sur les communes (peut être relancée si erreur)
  for (i in seq_along(liste_codgeo)) {
    
    # Relance
    if(i <= compteur_traitement) next
    
    # Informations de la commune
    codgeo <- liste_codgeo[i]
    poly_com_i <- sf_poly_coms %>% filter(codgeo == !!codgeo)
    libgeo <- poly_com_i$libgeo
    message("Traitement ", i, "/", length(liste_codgeo), " : ",  codgeo, " ", libgeo)
    
    # plot(poly_com_i$libgeo)
    
    # Requête : corrige le polygone si besoin
    sf_object <- tryCatch(
      expr = { 
        requete_ohsome(poly_com_i) 
      }, 
      error = function(e) {
        message("... Nétoyage avec st_make_valid()...")
        poly_com_i <- st_make_valid(poly_com_i)
        tryCatch(    
          expr = { 
            requete_ohsome(poly_com_i)
          }, 
          error = function(e) {
            tryCatch(
              expr = {
                message("... Nétoyage avec st_buffer()...")
                poly_com_i <- st_buffer(poly_com_i, 0)
                requete_ohsome(poly_com_i)
              },
              error = function(e){
                message("... Créer une enveloppe convexe...")
                poly_points <- st_cast(poly_com_i, "POINT")
                convex_hull <- st_convex_hull(st_combine(poly_points))
                convex_hull_sf <- st_sf(geometry = convex_hull)
                convex_hull_sf$osm_id <- poly_com_i$osm_id
                convex_hull_sf$codgeo <- poly_com_i$codgeo
                convex_hull_sf$libgeo <- poly_com_i$libgeo
                requete_ohsome(convex_hull_sf)
              })
          })
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
  
  # Correction des formats
  for(codgeo in liste_codgeo) {
    data <- liste_resultats[[codgeo]]
    data <- data %>% mutate(across(any_of(liste_tags), as.character))
    liste_resultats[[codgeo]] <- data
  }
  
  # Résultat final
  data_resultats <- bind_rows(liste_resultats)
  
  lg <- sum(data_resultats$longueur, na.rm = TRUE)/1000
  message(code_dep, " - ", annee, " : ", round(lg,-3), " km")
  message("Enregistrement du fichier ", glue(output_api))
  aws.s3::s3write_using(
    data_resultats,
    FUN = arrow::write_parquet,
    object = glue(output_api),
    bucket = BUCKET,
    opts = list("region" = "")
  )
}

