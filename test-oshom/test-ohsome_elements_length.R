library(tidyverse)
library(sf)
library(aws.s3)
library(arrow)
library(httr)
library(glue)
library(ohsome)

# Coffre
BUCKET <- "zg6dup"

# Etude de cas
annee <- "2022"
code_dep <- "75"
codgeo <- "75101"

code_dep <- "13"
codgeo <- "13001"

# Résultat attendu
expected <- aws.s3::s3read_using(
  FUN = arrow::read_parquet,
  object = glue("at36vc/api-ohsome-com-par-depts-{annee}/lg-com-dep{code_dep}.parquet"),
  bucket = BUCKET,
  opts = list("region" = "")
) 
expected <- expected %>% filter(codgeo == codgeo) %>% mutate(longueur = as.numeric(longueur))

message("Polygone des communes, fichier ", glue(input_api))
poly_com <- aws.s3::s3read_using(
  FUN = arrow::read_parquet,
  object = glue("at36vc/osmdata-polygones-geo2025/poly-com-dep{code_dep}-crs4356-geo2025.parquet"),
  bucket = BUCKET,
  opts = list("region" = "")
)
# Format sf
sf_poly_coms <- poly_com %>% 
  rename(geometry = geometry_wkt) %>%
  st_as_sf(wkt = "geometry", crs = st_crs(4326))

liste_codgeo <- sort(poly_com$codgeo)

# Informations de la commune
# codgeo <- liste_codgeo[i]
input_sf <- sf_poly_coms %>% filter(codgeo == !!codgeo)

attributs <- c("highway", "bicycle", "un-attribut-qui-nexiste-pas")

longueur_par_attribut <- function(attribut, input_sf, time) {
  df <- input_sf %>%
    ohsome::ohsome_elements_length(
      filter = "highway=* and geometry:line",
      time = glue("{annee}-01-01")
    ) |>
    ohsome::set_grouping("tag") %>%
    ohsome::set_groupByKey(attribut) %>%
    ohsome::ohsome_post()
  
  df %>%
    rename_with(
      .fn = ~ gsub(pattern = glue("^{attribut}\\."), replacement = "", x = .x),
      .cols = everything()
    ) %>%
    mutate(attribut = attribut, .before = 1) 
}

# output_lg <- map_dfr(attributs, ~ longueur_par_attribut(.x, input_sf, time))
attribut <- "highway"
attribut <- "test-attribut-qui-nexiste-pas"
target <- longueur_par_attribut(attribut, input_sf, time) %>% pivot_longer(cols = where(is.numeric))

# Comparaison
comparaison <- merge(
  target %>% select(!!attribut := name, longueur = value),
  expected %>% group_by(pick(any_of(attribut))) %>% summarise(longueur = sum(longueur, na.rm = TRUE)),
  by = attribut ) 

comparaison %>%
  arrange(desc(longueur.x)) %>%
  mutate(ratio = longueur.x / longueur.y * 100) %>%
  arrange(desc(longueur.x))
# highway longueur.x longueur.y ratio
# 1 service 404565.67 5256750.240 7.696117
# 2 unclassified 310219.44 4046355.390 7.666638
# 3 track 280326.67 10987791.064 2.551256
# 4 residential 251605.98 4749560.259 5.297458
# 5 footway 132914.78 1182042.716 11.244499
# 6 path 120363.40 4078458.985 2.951198
# 7 tertiary 104874.47 1946766.081 5.387112
# 8 secondary 90856.69 1388421.714 6.543883
# 9 primary 45024.68 844563.744 5.331117 

comparaison %>% 
  summarise(longueur.x = sum(longueur.x), longueur.y = sum(longueur.y)) %>%
  mutate(ratio = longueur.x / longueur.y * 100)

# longueur.x longueur.y ratio
# 1 1891555 36067545 5.24448


