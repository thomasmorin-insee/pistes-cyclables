# Récupérer les longueurs des voies par commune et les informations utiles
# pour reconstituer les pistes et les bandes cyclables

library(tidyverse)
library(osmdata)
library(sf)
library(aws.s3)
library(arrow)

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

# Fait 01 - 44, 67, 68

# Amélioration de la méthode à partir de 26
liste_code_dep <- names(liste_dep_osm)[19:27]
liste_code_dep <- "18"
for(code_dep in liste_code_dep) {
  # for(code_dep in c("26","27")) {
  
  gc()
  
  message(code_dep, " ", liste_dep_osm[code_dep])
  
  # Fichier à enregistrer
  BUCKET <- "zg6dup"
  FILE <- paste0("pistes_cyclables/longueur_voirie_",code_dep ,".parquet")
  
  # Récupère les objets "commune" dans le département "Nom-du-dépt, France"
  communes_osm <- paste0(liste_dep_osm[code_dep], ", France") %>%
    opq() %>%
    add_osm_feature(key = "boundary", value = "administrative") %>%
    add_osm_feature(key = "admin_level", value = "8") %>%
    osmdata_sf()
  
  # On ne garde que les communes dans le département
  communes_sf <- communes_osm$osm_multipolygons %>% 
    filter(substr(`ref:INSEE`,1,2) == !!code_dep)
  
  # Visualiser :
  # communes_sf %>% st_union() %>% plot()
  
  # Codes et noms de commune
  liste_id <- communes_sf$osm_id
  liste_communes <- communes_sf$name
  liste_codes_insee <- communes_sf$`ref:INSEE`
  
  # Initialise le compteur et la liste 
  compteur_traitement <- 0
  liste_resultats <- list()
  
  # Boucle sur les communes (peut être relancée si erreur)
  for (i in seq_along(liste_communes)) {
    
    # Passe les communes déjà traitées
    if(i <= compteur_traitement) next
    
    # Informations de la commune / du polygone
    id <- liste_id[i]
    nom_commune <- liste_communes[i]
    code_commune <- liste_codes_insee[i]
    poly_commune <- communes_sf[i, ]
    
    # Communes buggée (à réessayer avec nouvelle version)
    # if(code_commune == "2B159") next
    
    message("Traitement ", i, "/", length(liste_communes), " : ",  code_commune, " ", nom_commune)
    
    # Requête OSM sur la bbox de la commune
    req <- opq(bbox = st_bbox(poly_commune), timeout = 1000) %>%
      add_osm_feature(key = "highway")
    
    # On essaie deux fois pour réduire le risque d'erreur 
    req <- tryCatch(
      expr = {req %>% osmdata_sf() }, 
      error = function(e) {
        message("... Deuxième tentative...")
        tryCatch(
          expr = {req %>% osmdata_sf() }, 
          error = function(e) {
            message("... Troisième tentative...")
            req %>% osmdata_sf() 
          }
        )
      }
    )
    
    lignes <- req$osm_lines
    
    # Intersection : en cas d'erreur on corrige avec st_make_valid + st_buffer
    lignes_commune <- tryCatch({
      st_intersection(lignes, poly_commune)
    }, error = function(e) {
      message("... Nétoyage avec st_make_valid()...")
      poly_commune <- st_make_valid(poly_commune)
      lignes_commune <- tryCatch({
        st_intersection(lignes, poly_commune)
      }, error = function(e) {
        # Nétoyage avec st_buffer
        message("... Nétoyage avec st_buffer()...")
        poly_commune <- st_buffer(poly_commune, 0)
        return(st_intersection(lignes, poly_commune))
      })
      return(lignes_commune)
    })
    
    
    # Lambert 93
    lignes_commune <- lignes_commune %>% st_transform(crs=2154)
    
    # Calcul des longueurs par type
    lignes_commune$longueur <- st_length(lignes_commune)
    
    # Longueur selon le type de voirie
    longueur_voirie <- lignes_commune %>% 
      
      # Variables utiles
      select(any_of(c("highway","oneway","bicycle", "cycleway" , "maxspeed", "longueur"))) %>%
      st_drop_geometry() %>%
      
      # Longueur selon la description de la voirie
      group_by(pick(-"longueur")) %>% 
      summarise(longueur = sum(longueur, na.rm = TRUE), .groups = "drop") %>%
      
      # Information de la commune
      mutate(osm_id = !!id, code = !!code_commune, commune = !!nom_commune, .before = 1)
    
    # Résultat
    liste_resultats[[id]] <- longueur_voirie
    compteur_traitement <- i
  }
  
  # Résultat final
  data_resultats <- bind_rows(liste_resultats)
  
  # Enregistrement
  aws.s3::s3write_using(
    data_resultats,
    FUN = arrow::write_parquet,
    object = FILE,
    bucket = BUCKET,
    opts = list("region" = "")
  )
}
