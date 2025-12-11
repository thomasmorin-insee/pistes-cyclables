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
# library()

# Coffre
BUCKET <- "zg6dup"

output_polygone <- "at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet"

# Départements OSM
liste_dep_osm <- c(
  "01" = "Ain", "02" = "Aisne", "03" = "Allier", "04" = "Alpes-de-Haute-Provence", "05" = "Hautes-Alpes", "06" = "Alpes-Maritimes", "07" = "Ardèche", "08" = "Ardennes",
  "09" = "Ariège", "10" = "Aube", "11" = "Aude", "12" = "Aveyron", "13" = "Bouches-du-Rhône", "14" = "Calvados", "15" = "Cantal", "16" = "Charente", "17" = "Charente-Maritime",
  "18" = "Cher", "19" = "Corrèze", "2A" = "Corse-du-Sud", "2B" = "Haute-Corse", "21" = "Côte-d'Or", "22" = "Côtes-d'Armor", "23" = "Creuse", "24" = "Dordogne",
  "25" = "Doubs", "26" = "Drôme", "27" = "Eure", "28" = "Eure-et-Loir", "29" = "Finistère", "30" = "Gard", "31" = "Haute-Garonne", "32" = "Gers", "33" = "Gironde", "34" = "Hérault",
  "35" = "Ille-et-Vilaine", "36" = "Indre", "37" = "Indre-et-Loire", "38" = "Isère", "39" = "Jura", "40" = "Landes", "41" = "Loir-et-Cher", "42" = "Loire",
  "43" = "Haute-Loire", "44" = "Loire-Atlantique", "45" = "Loiret", "46" = "Lot", "47" = "Lot-et-Garonne", "48" = "Lozère", "49" = "Maine-et-Loire", "50" = "Manche",
  "51" = "Marne", "52" = "Haute-Marne", "53" = "Mayenne", "54" = "Meurthe-et-Moselle", "55" = "Meuse", "56" = "Morbihan", "57" = "Moselle", "58" = "Nièvre", "59" = "Nord",
  "60" = "Oise", "61" = "Orne", "62" = "Pas-de-Calais", "63" = "Puy-de-Dôme", "64" = "Pyrénées-Atlantiques", "65" = "Hautes-Pyrénées", "66" = "Pyrénées-Orientales",
  "67" = "Bas-Rhin", "68" = "Haut-Rhin", "69" = "Rhône", "70" = "Haute-Saône", "71" = "Saône-et-Loire", "72" = "Sarthe", "73" = "Savoie", "74" = "Haute-Savoie",
  "75" = "Paris", "76" = "Seine-Maritime", "77" = "Seine-et-Marne", "78" = "Yvelines", "79" = "Deux-Sèvres", "80" = "Somme", "81" = "Tarn", "82" = "Tarn-et-Garonne",
  "83" = "Var", "84" = "Vaucluse", "85" = "Vendée", "86" = "Vienne", "87" = "Haute-Vienne", "88" = "Vosges", "89" = "Yonne", "90" = "Territoire de Belfort", "91" = "Essonne",
  "92" = "Hauts-de-Seine", "93" = "Seine-Saint-Denis", "94" = "Val-de-Marne", "95" = "Val-d'Oise"
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
for (i in seq_along(liste_code_dep)) {
  
  # Passe les communes déjà traitées
  if(i <= compteur_traitement) next;
  
  code_dep <- liste_dep_osm[i]
  message(i, " : ", code_dep, " ", liste_dep_osm[code_dep])

  # Code du département
  # code_dep <- "13"
  
  # Récupère les objets "commune" dans le département "Nom-du-dépt, France"
  communes_osm <- osmdata_sf_retry(paste0(liste_dep_osm[code_dep], ", France"), 8)
  
  # Polygone de la commune
  poly_communes <- communes_osm$osm_multipolygons %>% 
    filter(substr(`ref:INSEE`,1,2) == !!code_dep) %>%
    select(osm_id, codgeo = `ref:INSEE`, libgeo = name)
  
  # st_crs(poly_communes) # 4326
  # poly_communes %>% filter(codgeo == "13001") %>% select(libgeo) %>% plot()
  
  df_poly_commune <- poly_communes %>%
    mutate(geometry_wkt = st_as_text(geometry)) %>%
    st_drop_geometry()
  
  # Enregistrement
  aws.s3::s3write_using(
    df_poly_commune,
    FUN = arrow::write_parquet,
    object = glue(output_polygone),
    bucket = BUCKET,
    opts = list("region" = "")
  )
  
  compteur_traitement <- i
}


## Vérification
# 
# df2 <- aws.s3::s3read_using(
#   FUN = arrow::read_parquet,
#   object = paste0("osm-polygones-com-geo2025/poly-com-dep",code_dep ,"-crs4356-geo2025.parquet"),
#   bucket = BUCKET,
#   opts = list("region" = "")
# )
# 
# poly_communes2 <-
#   df2 %>% rename(geometry = geometry_wkt) %>%
#   st_as_sf(wkt = "geometry", crs = st_crs(4326))
#
# poly_communes2 %>% filter(codgeo == "13001") %>% select(libgeo) %>% plot()
#
# poly_communes2 %>% filter(codgeo == "13001") %>%
#   bind_rows(poly_communes %>% filter(codgeo == "13113")) %>%
#   select(libgeo) %>%
#   plot()

