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
annee <- "2019"
date <- glue("{annee}-01-01")

# Input / output
input_api <- "at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet"
output_api <- "at36vc/api-ohsome-com-par-depts-{annee}/lg-com-dep{code_dep}.parquet"
# output_api <- "api-ohsome-com-par-depts-{annee}/lines-com-dep{code_dep}.parquet" # Ancien dossier pour 2025

# liste des tags utiles 
liste_tags <- eval(parse(text = paste(readLines("./pistes-cyclables/_tags-osm.R"), collapse = "\n")))

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
liste_dep <- c("13", "69")
plm_arm <- list(
  "13"= c("13201", "13202", "13203", "13204", "13205",
    "13206", "13207", "13208", "13209", "13210",
    "13211", "13212", "13213", "13214", "13215", "13216"),
  "69" = c("69381", "69382", "69383", "69384", "69385",
    "69386", "69387", "69388", "69389")
)
plm_code <- list("13"= "13055", "69" = "69123")

for(code_dep in liste_dep) {
  
  message(annee, " - ", date, " - Polygone des communes, fichier ", glue(input_api))
  poly_com <- aws.s3::s3read_using(
    FUN = arrow::read_parquet,
    object = glue(input_api),
    bucket = BUCKET,
    opts = list("region" = "")
  )
  
  # On ne garde que les arrondissements
  liste_arm <- plm_arm[[code_dep]]
  poly_com <- poly_com %>%
    filter(codgeo %in% !!liste_arm)
  
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
  
  # Ancien résultat
  message("Chargement du fichier ", glue(output_api))
  old_resultat <- aws.s3::s3read_using(
    FUN = arrow::read_parquet,
    object = glue(output_api),
    bucket = BUCKET,
    opts = list("region" = "")
  )
  
  # Remplace la commune par les arrondissements
  code_com_old <- plm_code[[code_dep]]
  new_resultat <- old_resultat %>%
    bind_rows(data_resultats) %>%
    filter(codgeo != !!code_com_old)

lg_old <- sum(old_resultat$longueur, na.rm = TRUE)
lg_new <- sum(new_resultat$longueur, na.rm = TRUE)
ecart <- 100 * (lg_new - lg_old) / lg_old

message(annee, " - ", date, " - écart ", ecart, " %")

message("Enregistrement du fichier ", glue(output_api))
  aws.s3::s3write_using(
    new_resultat,
    FUN = arrow::write_parquet,
    object = glue(output_api),
    bucket = BUCKET,
    opts = list("region" = "")
  )
}

