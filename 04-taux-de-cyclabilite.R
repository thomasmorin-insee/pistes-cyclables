library(tidyverse)
library(glue)

# Coffre
BUCKET <- "zg6dup"

annee <- "2025"
code_dep <- "13"

data <- aws.s3::s3read_using(
  FUN = arrow::read_parquet,
  object = glue("schema-ac-com-par-depts-{annee}/dept-{code_dep}.parquet"),
  bucket = BUCKET,
  opts = list("region" = "")
)

data %>% distinct(filtre_ac)


data %>% distinct(filtre_pc)

# Longueur des pistes dans l'Atlas Vélo et territoire
data %>% group_by(filtre_ac, filtre_pc) %>%
  summarise(longueur = sum(longueur)) %>%
  head()


# Simplification
data <- data %>% mutate(across(c(ame_d, ame_g), function(.x){
  .x <- case_when(
    filtre_ac == "false" ~ "AUCUN",
    
    
    
  )
  
})

crit_atlas_velo_territoire <- c(
  "PISTE CYCLABLE" = "Piste cyclable",
  "VOIE VERTE" = "Voie verte"
  "AUTRE" = "Aucun",
  "AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE" = "Aucun",
  "DOUBLE SENS CYCLABLE PISTE" = "Piste cyclable",
  "GOULOTTE" = "Autre",
  "AUCUN" = "Aucun",
  "BANDE CYCLABLE" = "Bande cyclable",
  "DOUBLE SENS CYCLABLE NON MATERIALISE"  = "Aucun",
  "COULOIR BUS+VELO" = "Couloir bus+vélo",
  "DOUBLE SENS CYCLABLE BANDE" = "Bande cyclable",
  "CHAUSSEE A VOIE CENTRALE BANALISEE" = "CVCB",
  "VELO RUE" = "Vélo Rue",
  "ACCOTEMENT REVETU HORS CVCB"= "Autre"
  )