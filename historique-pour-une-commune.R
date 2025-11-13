# Récupérer les longueurs des voies par commune et les informations utiles
# pour reconstituer les pistes et les bandes cyclables
install.packages("osmdata")
library(tidyverse)
library(osmdata)
library(sf)
library(aws.s3)
library(arrow)
# library(arrow)
# library()

# Dossiers des pgms
dir <- "./pistes-cyclables/"

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
  "92" = "Hauts-de-Seine", "93" = "Seine-Saint-Denis", "94" = "Val-de-Marne", "95" = "Val-d'Oise",
  "971" = "Guadeloupe", "972" = "Martinique", "973" = "Guyane",
  "974" = "La Réunion", "976" = "Mayotte"
)

# Code du département
code_dep <- "13"
  
# Récupère les objets "commune" dans le département "Nom-du-dépt, France"
communes_osm <- paste0(liste_dep_osm[code_dep], ", France") %>%
  opq() %>%
  add_osm_feature(key = "boundary", value = "administrative") %>%
  add_osm_feature(key = "admin_level", value = "8") %>%
  osmdata_sf()

# Code de la commune
code_commune <- "13001"

# Polygone de la commune
poly_commune <- communes_osm$osm_multipolygons %>% filter(`ref:INSEE` == code_commune)

# Nom de la commune
nom_commune <- poly_commune %>% pull(name) %>% unique()
if(length(nom_commune) > 1) {
  warning("Plusieurs noms de commune ???")
}

# Convertir poly_commune en FeatureCollection GeoJSON
poly_commune$id <- 1  # ou un identifiant unique si vous avez plusieurs features
fc_geojson <- geojsonsf::sf_geojson(select(poly_commune, id), simplify = FALSE)

# Date de l'extraction
date <- "2025-01-01"
date <- "2017-01-01"
date <- "2023-01-01"

# Construire le corps de la requête
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

# liste des tags utiles 
liste_tags <- eval(eval(parse(text = paste(readLines("./pistes-cyclables/_tags-osm.R"), collapse = "\n"))))

setdiff(liste_tags, colnames(sf_object))
intersect(liste_tags, colnames(sf_object))

# Lambert 93
lignes_commune <- sf_object %>% st_transform(crs=2154)

# Calcul des longueurs par type
lignes_commune$longueur <- st_length(lignes_commune)

# Longueur selon le type de voirie
longueur_voirie <- lignes_commune %>% 
  
  # Variables utiles
  select(any_of(c(liste_tags, "longueur"))) %>%
  st_drop_geometry() %>%
  
  # Longueur selon la description de la voirie
  group_by(pick(-"longueur")) %>% 
  summarise(longueur = sum(longueur, na.rm = TRUE), .groups = "drop") %>%
  
  # Information de la commune
  mutate(code = !!code_commune, commune = !!nom_commune, .before = 1)

# Fichier à enregistrer
BUCKET <- "zg6dup"
FILE <- paste0("osm-historique-commune/longueur-",code_commune ,"-", date, ".parquet")

# Enregistrement
aws.s3::s3write_using(
  longueur_voirie,
  FUN = arrow::write_parquet,
  object = FILE,
  bucket = BUCKET,
  opts = list("region" = "")
)

# Requête sql
data <- longueur_voirie
file_sql <- paste0(dir,"/_schema-amenagements-cyclables.sql")
requete_sql <- readChar(file_sql, file.info(file_sql)$size)

# Connexion
# DBI::dbDisconnect(con, shutdown = TRUE)
con <- DBI::dbConnect(duckdb::duckdb())

# Ajoute les variables manquantes
for(tag in liste_tags) {
  if(!tag %in% colnames(data)) {
    data <- data %>% mutate({{tag}} := "")
  }
}

# Remplace les "." par des "_"
data <- data %>% rename_with(~str_replace_all(.x, "\\.", "_"))

# Applique le shéma des aménagements cyclables
DBI::dbExecute(con, "DROP VIEW IF EXISTS data_for_sql")
duckdb::duckdb_register(con, "data_for_sql", data)
tbl <- DBI::dbGetQuery(con, paste0(requete_sql,"
  longueur, code, commune,
  FROM data_for_sql
"))

FILE_AC <- paste0("schema-ac-historique-commune/lg-ac-",code_commune ,"-", date, ".parquet")

# Enregistrement
aws.s3::s3write_using(
  tbl,
  FUN = arrow::write_parquet,
  object = FILE_AC,
  bucket = BUCKET,
  opts = list("region" = "")
)

# Aménagement de droite :
tbl %>% filter(commune == "Aix-en-Provence", filtre_ac == "yes") %>%
  group_by(ame_d) %>%
  summarise(longueur = sum(longueur), .groups = "drop")

# Aménagement de gauche :
tbl %>% filter(commune == "Aix-en-Provence", filtre_ac == "yes") %>%
  group_by(ame_g) %>%
  summarise(longueur = sum(longueur), .groups = "drop")

################################################################################
# Résultats :

## 01/01/2025
# # A tibble: 8 × 2
# ame_d                                         longueur
# <chr>                                            <dbl>
# 1 AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE  15862. 
# 2 AUCUN                                           8188. 
# 3 AUTRE                                          32988. 
# 4 BANDE CYCLABLE                                 34461. 
# 5 COULOIR BUS+VELO                                6445. 
# 6 PISTE CYCLABLE                                 27395. 
# 7 RAMPE                                             13.0
# 8 VOIE VERTE                                      9163. 

# A tibble: 8 × 2
ame_d                                         longueur
<chr>                                            <dbl>
  1 AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE  17869. 
2 AUCUN                                           9716. 
3 AUTRE                                          32627. 
4 BANDE CYCLABLE                                 32594. 
5 COULOIR BUS+VELO                                6376. 
6 PISTE CYCLABLE                                 23389. 
7 RAMPE                                             13.0
8 VOIE VERTE                                      3843. 

## 13/11/2025 osmdata / geodatamine
# A tibble: 8 x 4
# ame_d                                         longueur longueur_cible  diff
# <chr>                                            <dbl>          <dbl> <dbl>
#   1 AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE  14841.         16079.   1238
# 2 AUCUN                                           6987.          7137.    150
# 3 AUTRE                                          15643.         14800.   -843
# 4 BANDE CYCLABLE                                 35178.         35053.   -125
# 5 COULOIR BUS+VELO                                6544.          6545.      0
# 6 PISTE CYCLABLE                                 28584.         28860.    275
# 7 RAMPE                                             13.0           13.0     0
# 8 VOIE VERTE                                      9016.          9007.     -9

## 01/01/2025
# ame_g                                longueur
# <chr>                                   <dbl>
#   1 AUCUN                                 84133. 
# 2 AUTRE                                 15164. 
# 3 BANDE CYCLABLE                        19895. 
# 4 COULOIR BUS+VELO                       2808. 
# 5 DOUBLE SENS CYCLABLE BANDE              325. 
# 6 DOUBLE SENS CYCLABLE NON MATERIALISE   9412. 
# 7 DOUBLE SENS CYCLABLE PISTE               97.9
# 8 PISTE CYCLABLE                         2680

## 13/11/2025 osmdata / geodatamine
# ame_g                                longueur longueur_cible  diff
# <chr>                                   <dbl>          <dbl> <dbl>
#   1 AUCUN                                 71655.         72353.    698
# 2 AUTRE                                 16568.         15944.   -623
# 3 BANDE CYCLABLE                        19834.         20233.    399
# 4 COULOIR BUS+VELO                       2808.          2808.      0
# 5 DOUBLE SENS CYCLABLE BANDE              325.           325.      0
# 6 DOUBLE SENS CYCLABLE NON MATERIALISE   2504.          2261.   -243
# 7 DOUBLE SENS CYCLABLE PISTE               97.9           97.9     0
# 8 PISTE CYCLABLE                         3016.          3472.    455

## 01/012017
# A tibble: 5 × 2
# ame_d                                         longueur
# <chr>                                            <dbl>
# 1 AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE    7734.
# 2 AUTRE                                         1552196.
# 3 BANDE CYCLABLE                                  15267.
# 4 PISTE CYCLABLE                                   7004.
# 5 VOIE VERTE                                        851.

# A tibble: 3 × 2
# ame_g          longueur
# <chr>             <dbl>
# 1 AUTRE          1574507.
# 2 BANDE CYCLABLE    7914.
# 3 PISTE CYCLABLE     633.

