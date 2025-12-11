################################################################################
#
# Polygones des communes par département d'après open street map
# 
# Sortie : at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet
#
################################################################################

install.packages("osmdata")
library(tidyverse)
library(osmdata)
library(sf)
library(aws.s3)
library(arrow)
# library(arrow)
library(glue)

# Coffre
BUCKET <- "zg6dup"

output_polygone <- "at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet"

# Départements OSM
liste_dep_osm <- c(
  # "971" = "Guadeloupe", 
  # "972" = "Martinique", 
  # "973" = "Guyane",
  "974" = "La Réunion", 
  "976" = "Mayotte"
)

compteur_traitement <- 0

# Boucle pour osmdata_sf
osmdata_sf_retry <- function(dept, max_retries = 3) {
  for (i in 1:max_retries) {
    tryCatch(
      {
        data <- opq(dept) %>%
          add_osm_feature(key = "boundary", value = "administrative") %>%
          add_osm_feature(key = "admin_level", value = "8") %>%
          osmdata_sf()
        return(data)
      },
      error = function(e) {
        if (grepl("504", e$message) && i < max_retries) {
          Sys.sleep(5 * i)  # Attendre avant de réessayer
          message("Réessai ", i, "/", max_retries)
        } else {
          stop("Échec après ", max_retries, " tentatives : ", e$message)
        }
      }
    )
  }
}

# Boucle sur les départements (peut être relancée si erreur)
for (i in seq_along(liste_dep_osm)) {
  
  # Passe les communes déjà traitées
  if(i <= compteur_traitement) next;
  
  code_dep <- names(liste_dep_osm)[i]
  nom_dom <- liste_dep_osm[i]
  message(i, " : ", code_dep, " ", liste_dep_osm[code_dep])
  
  # Code du département
  # code_dep <- "13"
  
  # Récupère les objets "commune" dans le département "Nom-du-dépt, France"
  communes_osm <- osmdata_sf_retry(paste0(liste_dep_osm[code_dep], ", France"), 8)
  
  # Définir la zone de recherche (ex: "Guadeloupe, France")
  bbox_dom <- getbb(paste(nom_dom, ", France"))
  
  # Requête pour extraire les polygones des communes (admin_level = 7)
  communes_dom <- opq(bbox = bbox_dom) %>%
    add_osm_feature(key = "boundary",
                    value = "administrative") %>%
    add_osm_feature(key = "admin_level",
                    value = "8") %>%
    osmdata_sf()
  
  # Polygone de la commune
  poly_communes <- communes_dom$osm_multipolygons %>% 
    filter(substr(`ref:INSEE`,1,3) == !!code_dep) %>%
    select(osm_id, codgeo = `ref:INSEE`, libgeo = name)
  
  # st_crs(poly_communes) # 4326
  poly_communes %>%  select(libgeo) %>% plot()
  
  df_poly_commune <- poly_communes %>%
    mutate(geometry_wkt = st_as_text(geometry)) %>%
    st_drop_geometry()
  
  # Enregistrement
  message("Enregistrement de ", glue(output_polygone))
  aws.s3::s3write_using(
    df_poly_commune,
    FUN = arrow::write_parquet,
    object = glue(output_polygone),
    bucket = BUCKET,
    opts = list("region" = "")
  )
  
  compteur_traitement <- i
}
