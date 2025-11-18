################################################################################
#   
# Base communale : longueur des voies et taux de cyclabilité
# 
# Entrée : at36vc/schema-ac-com-par-depts-{annee}/dept-{code_dep}.parquet
# Sortie : at36vc/taux-cyclable-com-{annee}/dept-{code_dep}.parquet
#
################################################################################
library(tidyverse)
library(glue)

# Coffre
BUCKET <- "zg6dup"

# Input / output
annee <- 2025
input_tc <- "at36vc/schema-ac-com-par-depts-{annee}/dept-{code_dep}.parquet"
output_tc <- "at36vc/taux-cyclable-com-{annee}/dept-{code_dep}.parquet"

liste_dep <- c(
  "01", 
  "02",
  "03", "04", "05", "06", "07", "08", "09", 
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

for(code_dep in liste_dep) {

  # Chargement des données
  data <- aws.s3::s3read_using(
    FUN = arrow::read_parquet,
    object = glue(input_tc),
    bucket = BUCKET,
    opts = list("region" = "")
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
    data %>% select(codgeo, libgeo, vc = ame_d, sens = sens_d, filtre_ac, filtre_pc, longueur), 
    data %>% select(codgeo, libgeo, vc = ame_g, sens = sens_g, filtre_ac, filtre_pc, longueur)
  ) %>% 
    mutate(vc = ifelse(filtre_ac == "true", vc, NA),
           vc = factor(vc, names(crit_s),unname(crit_s)),
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
           autre  = ifelse(vc == "autre", longueur * pond, 0),
           voie_cyclable = cyclable * longueur * pond,
           voie_pc = voie_pc * longueur) %>%
    group_by(codgeo, libgeo) %>%
    summarise(across(.cols = c(piste, bande, voie_verte, autre, voie_cyclable, voie_pc), 
                     .fns = ~ sum(.x, na.rm = TRUE)),
              .groups = "drop") %>%
    mutate(tx_cyclable = 100 * voie_cyclable / voie_pc)
  
  message("Enregistrement du fichier ", glue(output_tc))
  
  aws.s3::s3write_using(
    data_large,
    FUN = arrow::write_parquet,
    object = glue(output_tc),
    bucket = BUCKET,
    opts = list("region" = "")
  )
}

# Total pour un département
data_large %>%
  summarise(across(.cols = c(piste, bande, voie_verte, autre, voie_cyclable, voie_pc), 
                   .fns = ~ sum(.x, na.rm = TRUE)),
            .groups = "drop") %>%
  mutate(tx_cyclable = 100 * voie_cyclable / voie_pc) %>%
  knitr::kable(format.args = list(big.mark = " "), 
               digits = c(rep(0,6),2))
# |     piste|   bande| voie_verte|  autre| voie_cyclable|    voie_pc| tx_cyclable|
# |---------:|-------:|----------:|------:|-------------:|----------:|-----------:|
# | 1 051 391| 353 901|    949 733| 95 209|     2 450 234| 27 969 226|        8.76|