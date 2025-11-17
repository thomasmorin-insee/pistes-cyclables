library(tidyverse)
library(glue)

# Coffre
BUCKET <- "zg6dup"

input_ac <- "schema-ac-com-par-depts-01-06-2024/schema-ac-dept-{code_dep}.parquet"
input_ac <- "schema-ac-com-par-depts-2025/dept-{code_dep}.parquet"
code_dep <- "01"

# Chargement des données
data <- aws.s3::s3read_using(
  FUN = arrow::read_parquet,
  object = glue(input_ac),
  bucket = BUCKET,
  opts = list("region" = "")
)

data %>% group_by(filtre_ac, filtre_pc) %>%
  summarise(sum(longueur)/1000) %>% 
  knitr::kable(format.args = list(big.mark = " "), digits = 0)
# |filtre_ac |filtre_pc | sum(longueur)/1000|
# |:---------|:---------|------------------:|
# |false     |false     |             19 176|
# |false     |yes       |             13 152|
# |true      |false     |              1 318|
# |true      |yes       |                522|


# Simplification
crit_atlas_velo_territoire <- c(
  "PISTE CYCLABLE" = "Piste cyclable",
  "DOUBLE SENS CYCLABLE PISTE" = "Piste cyclable",
  "VOIE VERTE" = "Voie verte",
  "BANDE CYCLABLE" = "Bande cyclable",
  "DOUBLE SENS CYCLABLE BANDE" = "Double sens cyclable",
  "CHAUSSEE A VOIE CENTRALE BANALISEE" = "CVCB",
  "COULOIR BUS+VELO" = "Couloir bus+vélo",
  "VELO RUE" = "Velorue",
  "GOULOTTE" = "Autre",
  "ACCOTEMENT REVETU HORS CVCB"= "Autre",
  "AUTRE" = "Aucun",
  "AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE" = "Aucun",
  "AUCUN" = "Aucun",
  "DOUBLE SENS CYCLABLE NON MATERIALISE"  = "Aucun"
)
# Voies de gauche et de droite empilées
data_l <- bind_rows(
  data %>% select(ame = ame_d, sens = sens_d, statut = statut_d, revet = revet_d,
                  access_ame, filtre_ac, longueur) %>% 
    mutate(voie = "d"),
  data %>% select(ame = ame_g, sens = sens_g, statut = statut_g, revet = revet_g,
                  access_ame, filtre_ac, longueur) %>% 
    mutate(voie = "g")) %>% 
  filter(filtre_ac == "true") %>%
  mutate(ame_rev = factor(ame, 
                          levels = names(crit_atlas_velo_territoire),
                          labels = unname(crit_atlas_velo_territoire))) %>%
  mutate(pond = ifelse(sens == "BIDIRECTIONNEL", 2, 1)) %>%
  mutate(cyclable = !ame_rev %in% c("Aucun", NA))

# Résultats
res <- data_l %>% 
  filter(cyclable) %>%
  group_by(ame_rev) %>%
  summarise(longueur = sum(pond * longueur)/1000) %>% 
  arrange(ame_rev)

res %>% knitr::kable(format.args = list(big.mark = " "), digits = 0)
# |ame                  | longueur|
# |:--------------------|--------:|
# |Piste cyclable       |    1 139|
# |Voie verte           |      689|
# |Bande cyclable       |      330|
# |Double sens cyclable |       22|
# |CVCB                 |       45|
# |Couloir bus+vélo     |       41|
# |Velorue              |        7|

res %>% summarise(sum(longueur)) %>% knitr::kable(format.args = list(big.mark = " "), digits = 0)
# | sum(longueur)|
# |-------------:|
# |         2 273|

# Pondération
data_l %>% filter(cyclable) %>% group_by(pond) %>% 
  summarise(sum(longueur))%>% knitr::kable(format.args = list(big.mark = " "), digits = 0)
# | pond| sum(longueur)|
# |----:|-------------:|
# |    1|       538 459|
# |    2|       867 169|

# Statut
data_l %>% filter(cyclable) %>% 
  group_by(statut) %>% 
  summarise(sum(longueur*pond)) %>% knitr::kable(format.args = list(big.mark = " "), digits = 0)
# |statut     | sum(longueur * pond)|
# |:----------|--------------------:|
# |EN SERVICE |            2 272 583|
# |EN TRAVAUX |                  214|

# revêtement (aucune info)
data_l %>% filter(cyclable) %>% 
  group_by(revet) %>% 
  summarise(sum(longueur*pond))%>% knitr::kable(format.args = list(big.mark = " "), digits = 0)

data_l %>% filter(cyclable) %>% 
  group_by(access_ame) %>% 
  summarise(sum(longueur*pond)/1000)%>% knitr::kable(format.args = list(big.mark = " "), digits = 0)

data_l %>% filter(cyclable, access_ame == "VTC") %>% 
  group_by(ame_rev) %>%
  summarise(sum(longueur*pond)/1000)%>% knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |ame_rev        | sum(longueur * pond)/1000|
# |:--------------|-------------------------:|
# |Piste cyclable |                      0.14|
# |Voie verte     |                      0.06|
  
