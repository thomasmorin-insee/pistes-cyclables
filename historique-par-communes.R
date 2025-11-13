# Library
library(osmdata)
library(sf)
library(dplyr)
library(aws.s3)
library(arrow)
library(httr)
library(geojsonsf)

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

# Tags utilisés pour le schéma des aménagaments cyclables
liste_tags <- c(
  # 1. Caractéristiques générales
  "highway", "construction", "junction", "tracktype", "service", "footway", "path", "steps", "living_street", "pedestrian",
  "residential", "unclassified", "primary", "secondary", "tertiary",
  # 2. Aménagements cyclables
  "cycleway", "cycleway.left", "cycleway.right", "cycleway.both", "cycleway.width", "cycleway.est_width",
  "cycleway.left.width", "cycleway.right.width", "cycleway.both.width", "cycleway.left.est_width", "cycleway.right.est_width", "cycleway.both.est_width",
  "cycleway.left.oneway", "cycleway.right.oneway", "cycleway.left.surface", "cycleway.right.surface", "cycleway.surface",
  "cycleway.left.smoothness", "cycleway.right.smoothness", "cycleway.smoothness",  "cycleway.left.segregated", "cycleway.right.segregated",
  "cycleway.both.segregated", "cycleway.segregated",  "ramp.bicycle", "oneway.bicycle",
  # 3. Aménagements piétons
  "sidewalk.bicycle", "sidewalk.left.segregated", "sidewalk.segregated",  "sidewalk.left", "sidewalk.right",
  # 4. Circulation et accès
  "access", "motor_vehicle", "motorcar", "psv", "bus", "oneway", "lanes",
  # 5. Revêtement et qualité
  "surface", "surface.left", "surface.right",  "smoothness", "smoothness.left", "smoothness.right",
  # 6. Signalisation et réglementation
  "traffic_sign", "designation", "maxspeed", "zone.maxspeed",  "source.maxspeed", "cyclestreet",
  # 7. Métadonnées
  "ref", "description", "note", "fixme", "source", "source.geometry", "start_date", "osm_timestamp",
  # 8. Relations d’itinéraires
  "route", "route_icn_ref", "route_ncn_ref", "route_rcn_ref", "route_lcn_ref",
  # 9. Ajout / correction IA :
  "bicycle", "segregated"
)



# Récupère les objets "commune" dans le département "Nom-du-dépt, France"
communes_osm <- paste0(liste_dep_osm[code_dep], ", France") %>%
  opq() %>%
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
