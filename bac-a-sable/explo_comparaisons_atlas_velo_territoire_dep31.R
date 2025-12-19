library(tidyverse)
library(glue)

# Coffre
BUCKET <- "zg6dup"

# input_ac <- "schema-ac-com-par-depts-2025/dept-{code_dep}.parquet"
input_ac <- "schema-ac-com-par-depts-01-06-2024/schema-ac-dept-{code_dep}-bis.parquet"
code_dep <- "31"

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

# Communes sans voies potentiellement cyclables ?
data %>% distinct(codgeo) %>% nrow() # 586
data %>% filter(filtre_pc == "true") %>% distinct(codgeo) %>% nrow() # 585

setdiff(data %>% distinct(codgeo), data %>% filter(filtre_pc == "true") %>% distinct(codgeo))
# 31068 

# Fichier "brut" oshome
longueurs_com1 <- aws.s3::s3read_using(
  FUN = arrow::read_parquet,
  object = glue("schema-ac-com-par-depts-01-06-2024/lines-com-dep{code_dep}.parquet"),
  bucket = BUCKET,
  opts = list("region" = "")
)
temp1 <- longueurs_com1 %>% filter(codgeo == "31068")

# Version plus récente : les routes (surface asphalt ont été renseignées)
longueurs_com2 <- aws.s3::s3read_using(
  FUN = arrow::read_parquet,
  object = glue("api-ohsome-com-par-depts-2025/lines-com-dep{code_dep}.parquet"),
  bucket = BUCKET,
  opts = list("region" = "")
)
temp2 <- longueurs_com2 %>% filter(codgeo == "31068")

# Simplification
crit_1 <- c(
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
crit_2 <- c(
  "PISTE CYCLABLE" = "Piste cyclable",
  "VOIE VERTE" = "Voie verte",
  "BANDE CYCLABLE" = "Bande cyclable",
  "DOUBLE SENS CYCLABLE BANDE" = "Double sens cyclable",
  "DOUBLE SENS CYCLABLE PISTE" = "Double sens cyclable",
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
  mutate(ame_rev = factor(ame, names(crit_1),unname(crit_1))) %>%
  mutate(ame_rev2 = factor(ame, names(crit_2),unname(crit_2))) %>%
  mutate(pond = ifelse(sens == "BIDIRECTIONNEL", 2, 1)) %>%
  mutate(cyclable = !ame_rev %in% c("Aucun", NA))

################################################################################
# Longueur des voies cyclables

# Résultats
res <- data_l %>% 
  filter(cyclable) %>%
  group_by(ame_rev) %>%
  summarise(longueur = sum(pond * longueur)/1000) %>% 
  arrange(ame_rev)

res2 <- data_l %>% 
  filter(cyclable) %>%
  group_by(ame_rev2) %>%
  summarise(longueur = sum(pond * longueur)/1000)

# https://www.velo-territoires.org/observatoires/donnees-velo/atlas-regionaux/
cible <- data.frame(
  ame_rev = c("Piste cyclable", "Voie verte", "Bande cyclable", "Double sens cyclable", 
          "CVCB", "Couloir bus+vélo", "Velorue"),
  longueur_cible = c(1158, 528, 312, 195, 51, 41, 8)
)

res %>% left_join(res2, by = c("ame_rev"="ame_rev2")) %>% 
  left_join(cible, by = "ame_rev") %>%
  knitr::kable(format.args = list(big.mark = " "), digits = 0)
# |ame_rev              | longueur.x| longueur.y| longueur_cible|
# |:--------------------|----------:|----------:|--------------:|
# |Piste cyclable       |      1 139|      1 136|          1 158|
# |Voie verte           |        689|        689|            528|
# |Bande cyclable       |        330|        330|            312|
# |Double sens cyclable |         22|         26|            195|
# |CVCB                 |         45|         45|             51|
# |Couloir bus+vélo     |         41|         41|             41|
# |Velorue              |          7|          7|              8|


res %>% left_join(cible, by = "ame_rev") %>%
  summarise(across(-1, ~sum(.x, na.rm=TRUE))) %>% 
  bind_cols(data.frame(total_velo_territoire = 2293)) %>%
  knitr::kable(format.args = list(big.mark = " "), digits = 0)
# | longueur| longueur_cible| total_velo_territoire|
# |--------:|--------------:|---------------------:|
# |    2 273|          2 293|                 2 293|

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

################################################################################
# Doubles comptes ?

data2 <- data %>%
  filter(filtre_ac == "true") %>%
  mutate(ame_d = factor(ame_d, 
                          levels = names(crit_1),
                          labels = unname(crit_1))) %>%
  mutate(ame_g = factor(ame_g, 
                        levels = names(crit_1),
                        labels = unname(crit_1))) %>%
  mutate(pond_d = ifelse(sens_d == "BIDIRECTIONNEL", 2, 1)) %>%
  mutate(pond_g = ifelse(sens_g == "BIDIRECTIONNEL", 2, 1)) 

data2 %>% group_by(ame_g, pond_g)%>%
  summarise(sum(longueur)/1000)%>% knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |ame_g                | pond_g| sum(longueur)/1000|
# |:--------------------|------:|------------------:|
# |Piste cyclable       |      1|              11.02|
# |Piste cyclable       |      2|               0.94|
# |Bande cyclable       |      1|             127.79|
# |Bande cyclable       |      2|               3.23|
# |Double sens cyclable |      1|              21.98|
# |CVCB                 |      1|              22.55|
# |Couloir bus+vélo     |      1|               8.60|
# |Velorue              |      1|               1.77|
# |Aucun                |      1|           1 641.19|
# |Aucun                |      2|               0.19|

data2 %>% 
  filter(ame_g == "Piste cyclable", pond_g == "2") %>%
  group_by(ame_d, pond_d)%>%
  summarise(sum(longueur)/1000)%>% knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |ame_d | pond_d| sum(longueur)/1000|
# |:-----|------:|------------------:|
# |Aucun |      1|               3.23|

data2 %>% 
  filter(ame_g == "Bande cyclable", pond_g == "2") %>%
  group_by(ame_d, pond_d)%>%
  summarise(sum(longueur)/1000)%>% knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |ame_d | pond_d| sum(longueur)/1000|
# |:-----|------:|------------------:|
# |Aucun |      1|               0.94|

data2 %>% 
  filter(ame_g == "Double sens cyclable", pond_g == "1") %>%
  group_by(ame_d, pond_d)%>%
  summarise(sum(longueur)/1000)%>% knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |ame_d            | pond_d| sum(longueur)/1000|
# |:----------------|------:|------------------:|
# |Bande cyclable   |      1|               1.94|
# |Couloir bus+vélo |      1|               0.21|
# |Aucun            |      1|              19.83|

data2 %>% group_by(ame_d, pond_d)%>%
  summarise(sum(longueur)/1000)%>% knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |ame_d            | pond_d| sum(longueur)/1000|
# |:----------------|------:|------------------:|
# |Piste cyclable   |      1|              77.24|
# |Piste cyclable   |      2|             524.47|
# |Voie verte       |      1|              13.64|
# |Voie verte       |      2|             337.62|
# |Bande cyclable   |      1|             193.86|
# |Bande cyclable   |      2|               0.92|
# |CVCB             |      1|              22.55|
# |Couloir bus+vélo |      1|              32.39|
# |Velorue          |      1|               5.06|
# |Aucun            |      1|             270.70|
# |Aucun            |      2|             360.24|
# |NA               |      2|               0.59|

data2 %>% 
  filter(ame_d == "Piste cyclable", pond_d == "2") %>%
  group_by(ame_g, pond_g)%>%
  summarise(sum(longueur)/1000) %>% knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |ame_g          | pond_g| sum(longueur)/1000|
# |:--------------|------:|------------------:|
# |Piste cyclable |      1|               0.13|
# |Bande cyclable |      1|               0.13|
# |Aucun          |      1|             524.20|

data2 %>% 
  filter(ame_d == "Voie verte", pond_d == "2") %>%
  group_by(ame_g, pond_g)%>%
  summarise(sum(longueur)/1000)%>% knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |ame_g          | pond_g| sum(longueur)/1000|
# |:--------------|------:|------------------:|
# |Piste cyclable |      1|               0.15|
# |Bande cyclable |      1|               0.06|
# |Aucun          |      1|             337.41|

################################################################################
# Taux de cyclabilité

crit_s <- c(
    "PISTE CYCLABLE" = "piste",
    "DOUBLE SENS CYCLABLE PISTE" = "piste",
    "VOIE VERTE" = "voie_verte",
    "BANDE CYCLABLE" = "bande",
    "DOUBLE SENS CYCLABLE BANDE" = "bande",
    "CHAUSSEE A VOIE CENTRALE BANALISEE" = "autre",
    "COULOIR BUS+VELO" = "autre",
    "VELO RUE" = "autre",
    "GOULOTTE" = "autre",
    "ACCOTEMENT REVETU HORS CVCB"= "autre"
    # ,
    # "AUTRE" = "aucun",
    # "AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE" = "aucun",
    # "AUCUN" = "aucun",
    # "DOUBLE SENS CYCLABLE NON MATERIALISE"  = "aucun"
  )

# Voies de gauche et de droite empilées
data_long <- bind_rows(
      data %>% select(codgeo, libgeo, vc = ame_d, sens = sens_d, filtre_ac, filtre_pc, sens_pc, longueur), 
      data %>% select(codgeo, libgeo, vc = ame_g, sens = sens_g, filtre_ac, filtre_pc, sens_pc, longueur)
  ) %>% 
  mutate(vc = ifelse(filtre_ac == "true", vc, NA),
         vc = factor(vc, names(crit_s),unname(crit_s)),
         pond = ifelse(sens == "BIDIRECTIONNEL", 2, 1),
         pond_pc =  ifelse(sens_pc == "BIDIRECTIONNEL", 1, 0.5),
         cyclable = vc %in% c("piste", "voie_verte", "bande", "autre"),
         voie_pc = filtre_pc == "true") %>%
  # filter(cyclable | voie_pc) %>%
  group_by(codgeo, libgeo, voie_pc, cyclable, vc, pond, pond_pc) %>%
  summarise(longueur = sum(longueur, na.rm = TRUE), .groups = "drop")


data_long %>%
  filter(vc %in% c("piste", "voie_verte", "bande", "autre")) %>%
  summarise(longueur = sum(pond * longueur)/1000)
# 2273.

data_long %>%
  group_by(vc) %>%
  summarise(longueur = sum(pond * longueur)/1000) %>% 
  knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |vc         |  longueur|
# |:----------|---------:|
# |piste      |  1 139.07|
# |voie_verte |    688.88|
# |bande      |    351.92|
# |autre      |     92.93|
# |NA         | 67 290.95|

# Taux de cyclabilité
data_long %>% 
  summarise(lg_pc = sum(voie_pc * longueur),
            lg_pc_rl = sum(voie_pc * pond_pc * longueur), # Proxy Rémi Lardellier
            cyclable = sum(cyclable * pond * longueur)) %>%
  mutate(tx = 100 * cyclable / lg_pc, tx_lr = 100 * cyclable / lg_pc_rl) %>% 
  knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |      lg_pc|   lg_pc_rl|  cyclable|   tx| tx_lr|
# |----------:|----------:|---------:|----:|-----:|
# | 27 347 807| 25 808 601| 2 272 797| 8.31|  8.81|

# Pour Toulouse
data_long %>% 
  filter(codgeo == "31555") %>%
  summarise(lg_pc = sum(voie_pc * longueur),
            lg_pc_rl = sum(voie_pc * pond_pc * longueur), # Proxy Rémi Lardellier
            cyclable = sum(cyclable * pond * longueur)) %>%
  mutate(tx = 100 * cyclable / lg_pc, tx_lr = 100 * cyclable / lg_pc_rl) %>% 
  mutate(tx_atlas_velo_territoire = 25.0) %>%
  knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |     lg_pc|  lg_pc_rl|  cyclable|    tx| tx_lr| tx_atlas_velo_territoire|
# |---------:|---------:|---------:|-----:|-----:|------------------------:|
# | 2 875 716| 2 374 528| 739 990.9| 25.73| 31.16|                       25|

# Pour Toulouse
data_long %>% 
  filter(codgeo == "31557") %>%
  summarise(lg_pc = sum(voie_pc * longueur),
            lg_pc_rl = sum(voie_pc * pond_pc * longueur), # Proxy Rémi Lardellier
            cyclable = sum(cyclable * pond * longueur)) %>%
  mutate(tx = 100 * cyclable / lg_pc, tx_lr = 100 * cyclable / lg_pc_rl) %>% 
  mutate(tx_atlas_velo_territoire = 25.2) %>%
  knitr::kable(format.args = list(big.mark = " "), digits = 2)
# |     lg_pc|  lg_pc_rl|  cyclable|    tx| tx_lr| tx_atlas_velo_territoire|
# |---------:|---------:|---------:|-----:|-----:|------------------------:|
# | 379 580.4| 343 757.2| 102 258.6| 26.94| 29.75|                     25.2|

data_large <- data_long %>% 
  mutate(voie_pc = voie_pc * longueur, 
         voie_cyclable = cyclable * longueur * pond,
         voie_cyclable2 = voie_cyclable) %>%
  pivot_wider(names_from = vc, values_from = voie_cyclable2, values_fill = 0) %>%
  group_by(codgeo, libgeo) %>%
  summarise(across(.cols = c(piste, bande, voie_verte, autre, voie_cyclable, voie_pc), 
                   .fns = ~ sum(.x, na.rm = TRUE)),
            .groups = "drop") %>%
  mutate(tx_cyclable = 100 * voie_cyclable / voie_pc)

data_large %>% filter(codgeo %in% c("31555", "31557")) %>%
  knitr::kable(format.args = list(big.mark = " "), 
               digits = c(rep(0,8),2))
# |codgeo |libgeo        |   piste|   bande| voie_verte|  autre| voie_cyclable|   voie_pc| tx_cyclable|
# |:------|:-------------|-------:|-------:|----------:|------:|-------------:|---------:|-----------:|
# |31555  |Toulouse      | 321 463| 158 718|    204 835| 54 975|       739 991| 2 875 716|       25.73|
# |31557  |Tournefeuille |  80 283|   5 468|     14 452|  2 055|       102 259|   379 580|       26.94|

# Moyenne départementale : 8.31
data_large %>% summarise(tx_cyclable = weighted.mean(tx_cyclable, w = voie_pc))


  