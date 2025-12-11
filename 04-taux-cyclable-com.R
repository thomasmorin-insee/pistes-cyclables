################################################################################
#   
# Base communale : longueur des voies et taux de cyclabilité
# 
# Entrée : at36vc/schema-ac-com-par-depts-{annee}/dept-{code_dep}.parquet
# Sortie : at36vc/taux-cyclable-com/base-com-cyclable-{annee}.parquet
#
################################################################################

# Librairie
library(tidyverse)
library(glue)

# Coffre
BUCKET <- "zg6dup"

# Input / output
annee <- 2025
input_tc <- "at36vc/schema-ac-com-par-depts-{annee}/dept-{code_dep}.parquet"
output_tc <- "at36vc/taux-cyclable-com/base-com-cyclable-{annee}-geo2025.parquet"

liste_dep <- c(
  "01",  "02", "03", "04", "05", "06", "07", "08", "09", 
  "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
  "2A", "2B", "21", "22", "23", "24", "25", "26", "27", "28", "29",
  "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
  "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
  "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
  "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
  "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
  "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
  "90", "91", "92", "93", "94", "95", "971", "972", "973", "974", "976"
)

# Initialisation
data <- tibble(codgeo = character())

for(code_dep in liste_dep) {

  # Chargement des données
  data_dep <- tryCatch(
    expr = {
      dt <- aws.s3::s3read_using(
        FUN = arrow::read_parquet,
        object = glue(input_tc),
        bucket = BUCKET,
        opts = list("region" = "")
      )
      print(glue("Fichier : ", input_tc))
      dt
    },
    error = function(e) { stop("dernier département (2022) : ", last_dep) }
  )
  
  # Critères simplifiés
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
    # ,"AUTRE" = "aucun",
    # "AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE" = "aucun",
    # "AUCUN" = "aucun",
    # "DOUBLE SENS CYCLABLE NON MATERIALISE"  = "aucun"
  )
  
  # Voies de gauche et de droite empilées
  data_long <- bind_rows(
    data_dep %>% select(codgeo, libgeo, vc = ame_d, sens = sens_d, filtre_ac, filtre_pc, longueur), 
    data_dep %>% select(codgeo, libgeo, vc = ame_g, sens = sens_g, filtre_ac, filtre_pc, longueur)
  ) %>% 
    mutate(vc = ifelse(filtre_ac == "true", vc, NA),
           vc = factor(vc, names(crit_s), unname(crit_s)),
           pond = ifelse(sens == "BIDIRECTIONNEL", 2, 1),
           cyclable = vc %in% c("piste", "voie_verte", "bande", "autre"),
           voie_pc = filtre_pc == "true") %>%
    group_by(codgeo, libgeo, voie_pc, cyclable, vc, pond) %>%
    summarise(longueur = sum(longueur, na.rm = TRUE), .groups = "drop") 
  
  # Une ligne par commune
  data_large <- data_long %>% 
    mutate(piste = ifelse(vc == "piste", longueur * pond, 0),
           bande = ifelse(vc == "bande", longueur * pond, 0),
           voie_verte = ifelse(vc == "voie_verte", longueur * pond, 0),
           autre_cyclable  = ifelse(vc == "autre", longueur * pond, 0),
           voie_cyclable = cyclable * longueur * pond,
           route_pc = voie_pc * longueur) %>%
    group_by(codgeo, libgeo) %>%
    summarise(across(.cols = c(piste, bande, voie_verte, autre_cyclable, voie_cyclable, route_pc), 
                     .fns = ~ sum(.x, na.rm = TRUE)),
              .groups = "drop") %>%
    mutate(tx_cyclable = 100 * voie_cyclable / route_pc) %>%
    mutate(dep = !!code_dep, .after = libgeo)
  
  data <- bind_rows(data_large, data)
  last_dep <- code_dep
}

# Formatage
data <- data %>% arrange(codgeo) 

message("Enregistrement du fichier ", glue(output_tc))

aws.s3::s3write_using(
  data,
  FUN = arrow::write_parquet,
  object = glue(output_tc),
  bucket = BUCKET,
  opts = list("region" = "")
)

# Grandes villes
data %>%
  select(-dep) %>%
  filter(codgeo %in% c("31555", "34172", "33063", 
                       "59350", "67482", "44109", "35238")) %>%
  arrange(desc(tx_cyclable)) %>%
  knitr::kable(format.args = list(big.mark = " "), digits = c(rep(0,8), 1))
# |codgeo |libgeo      |   piste|   bande| voie_verte| autre_cyclable| voie_cyclable|  route_pc| tx_cyclable|
# |:------|:-----------|-------:|-------:|----------:|--------------:|-------------:|---------:|-----------:|
# |67482  |Strasbourg  | 256 279|  42 408|    125 897|         10 254|       434 838| 1 147 343|        37.9|
# |31555  |Toulouse    | 274 447| 151 089|    297 906|         58 160|       781 603| 2 303 502|        33.9|
# |44109  |Nantes      | 109 309| 170 217|    115 539|         38 222|       433 287| 1 313 304|        33.0|
# |35238  |Rennes      | 112 211| 112 050|     44 951|         23 705|       292 917|   989 115|        29.6|
# |33063  |Bordeaux    |  85 037| 102 365|     11 027|         48 772|       247 200| 1 129 956|        21.9|
# |59350  |Lille       |  62 269|  60 654|     36 373|         21 494|       180 789|   849 434|        21.3|
# |34172  |Montpellier | 150 690|  39 530|     37 341|         15 641|       243 202| 1 352 985|        18.0|


# Total par départements
base_dep <- data %>% 
  group_by(dep) %>%
  summarise(across(.cols = c(piste, bande, voie_verte, autre_cyclable, voie_cyclable, route_pc), 
                   .fns = ~ sum(.x, na.rm = TRUE)),
            .groups = "drop") %>%
  mutate(tx_cyclable = 100 * voie_cyclable / route_pc)  %>%
  mutate(across(c(piste, bande, voie_verte, voie_cyclable, route_pc), ~.x/1000))

# Pour se comparer à https://www.velo-territoires.org/observatoires/donnees-velo/atlas-regionaux/
code_occitanie <- c("09", "11", "12", "30", "31", "32","34", "46", "48", "65", "66", "81", "82")
noms_occitanie <- c(
  "Ariège",  "Aude",  "Aveyron",  "Gard",  "Haute-Garonne", "Gers",  "Hérault",  
  "Lot",  "Lozère",  "Hautes-Pyrénées", "Pyrénées-Orientales",  "Tarn",  "Tarn-et-Garonne"
)
base_dep %>%
  filter(dep %in% code_occitanie) %>%
  mutate(LIB = factor(dep, code_occitanie, noms_occitanie), .after = dep) %>%
  arrange(desc(voie_cyclable)) %>%
  knitr::kable(format.args = list(big.mark = " "), digits = c(rep(0,8), 1))
# |dep |LIB                 | piste| bande| voie_verte|  autre| voie_cyclable| voie_pc| tx_cyclable|
# |:---|:-------------------|-----:|-----:|----------:|------:|-------------:|-------:|-----------:|
# |31  |Haute-Garonne       | 1 051|   354|        950| 95 209|         2 450|  33 118|         7.4|
# |34  |Hérault             | 1 041|   157|        501| 25 900|         1 725|  27 676|         6.2|
# |66  |Pyrénées-Orientales |   386|   115|        322|  8 963|           832|  14 392|         5.8|
# |30  |Gard                |   420|   108|        223| 34 016|           784|  25 690|         3.1|
# |11  |Aude                |   140|    47|        170| 10 524|           368|  18 327|         2.0|
# |09  |Ariège              |    87|    21|        118|    513|           226|  11 078|         2.0|
# |65  |Hautes-Pyrénées     |    57|    53|        106|    105|           215|  12 460|         1.7|
# |32  |Gers                |    96|     7|         34|      0|           137|  22 281|         0.6|
# |12  |Aveyron             |    43|    37|         34|     33|           114|  32 688|         0.3|
# |46  |Lot                 |    47|    21|         15|    678|            83|  23 215|         0.4|
# |48  |Lozère              |    12|     4|         11|      0|            27|  12 139|         0.2|

# Départements avec arrondissements non traités
base_dep %>%
  filter(dep %in% c("13","69", "75")) %>%
  arrange(desc(voie_cyclable)) %>%
  knitr::kable(format.args = list(big.mark = " "), digits = c(rep(0,7), 1))
# |dep | piste| bande| voie_verte| autre_cyclable| voie_cyclable| route_pc| tx_cyclable|
# |:---|-----:|-----:|----------:|--------------:|-------------:|--------:|-----------:|
# |69  |   648|   941|        438|        208 483|         2 235|   26 830|         8.3|
# |13  |   545|   381|        386|         32 003|         1 344|   25 553|         5.3|
# |75  |   438|    95|         37|        171 244|           742|    2 556|        29.0|

# DOM
base_dep %>%
  filter(dep %in% c( "971", "972", "973", "974", "976")) %>%
  arrange(desc(voie_cyclable)) %>%
  knitr::kable(format.args = list(big.mark = " "), digits = c(rep(0,7), 1))
