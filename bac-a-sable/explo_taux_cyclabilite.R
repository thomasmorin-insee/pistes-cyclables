library(tidyverse)
library(knitr)

BUCKET <- "zg6dup"

liste_dep <- c(
  "01", "02", "03", "04", "05", "06", "07", "08", "09", 
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
data <- tibble()

annee <- 2025
last_dep <- "00"
for(code_dep in liste_dep) {
  message(code_dep)
  data_dep <- tryCatch(
    expr = {
      df <- aws.s3::s3read_using(
        FUN = arrow::read_parquet,
        object = glue("at36vc/schema-ac-com-par-depts-{annee}/dept-{code_dep}.parquet"),
        bucket = BUCKET,
        opts = list("region" = "")
      )
    },
    error = function(e) { stop("dernier département (2022) : ", last_dep) }
  )
  last_dep <- code_dep
  data_dep <- data_dep %>% 
    mutate(dep = !!code_dep, annee = 2022) 
              
  data <- bind_rows(data_dep, data)
}

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
  data %>% select(dep, highway, vc = ame_d, sens = sens_d, filtre_ac, filtre_pc, longueur), 
  data %>% select(dep, highway, vc = ame_g, sens = sens_g, filtre_ac, filtre_pc,longueur)
) %>% 
  mutate(vc = ifelse(filtre_ac == "true", vc, NA),
         vc = factor(vc, names(crit_s),unname(crit_s)),
         pond = ifelse(sens == "BIDIRECTIONNEL", 2, 1),
         cyclable = vc %in% c("piste", "voie_verte", "bande", "autre"),
         voie_pc = filtre_pc == "true") %>%
  group_by(dep, highway, voie_pc, cyclable, vc, pond) %>%
  summarise(longueur = sum(longueur, na.rm = TRUE)/1000, .groups = "drop")  %>% 
  mutate(piste = ifelse(vc == "piste", longueur * pond, 0),
         bande = ifelse(vc == "bande", longueur * pond, 0),
         voie_verte = ifelse(vc == "voie_verte", longueur * pond, 0),
         autre  = ifelse(vc == "autre", longueur * pond, 0),
         voie_cyclable = cyclable * longueur * pond,
         voie_pc = voie_pc * longueur)

# Une ligne par dép
base_dep <- data_long %>%
  mutate(voie_pc2 = ifelse(highway %in% c("service", "trunk"), 0, voie_pc)) %>%
  group_by(dep) %>%
  summarise(across(.cols = c(piste, bande, voie_verte, autre, voie_cyclable, voie_pc, voie_pc2), 
                   .fns = ~ sum(.x, na.rm = TRUE)),
            .groups = "drop") %>%
  mutate(tx_1 = 100 * voie_cyclable / voie_pc,
         tx_2 = 100 * voie_cyclable / voie_pc2)

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
  knitr::kable(format.args = list(big.mark = " "), digits = c(rep(0,9),1, 1))
# |dep |LIB           | piste| bande| voie_verte| autre| voie_cyclable| voie_pc| voie_pc2| tx_1| tx_2|
# |:---|:-------------|-----:|-----:|----------:|-----:|-------------:|-------:|--------:|----:|----:|
# |31  |Haute-Garonne | 1 051|   354|        950|    95|         2 450|  43 467|   33 118|  5.6|  7.4|
# |34  |Hérault       | 1 041|   157|        501|    26|         1 725|  34 730|   27 676|  5.0|  6.2|
# |30  |Gard          |   420|   108|        223|    34|           784|  31 056|   25 690|  2.5|  3.1|
# |11  |Aude          |   140|    47|        170|    11|           368|  23 807|   18 327|  1.5|  2.0|
# |09  |Ariège        |    87|    21|        118|     1|           226|  14 216|   11 078|  1.6|  2.0|
# |32  |Gers          |    96|     7|         34|     0|           137|  33 229|   22 281|  0.4|  0.6|
# |12  |Aveyron       |    43|    37|         34|     0|           114|  38 480|   32 688|  0.3|  0.3|

# Une ligne par highway
base_highway <- data_long %>%
  group_by(highway) %>%
  summarise(across(.cols = c(piste, bande, voie_verte, autre, voie_cyclable, voie_pc), 
                   .fns = ~ sum(.x, na.rm = TRUE)),
            .groups = "drop") 

base_highway %>%
  filter(voie_cyclable > 0) %>%
  mutate(pct_cyclable = 100 * voie_cyclable / sum(voie_cyclable)) %>%
  arrange(desc(voie_cyclable)) %>%
  head(10) %>%
  knitr::kable(format.args = list(big.mark = " "), digits = c(rep(0,8),1))
# |highway       |  piste| bande| voie_verte| autre| voie_cyclable| voie_pc| pct_cyclable|
# |:-------------|------:|-----:|----------:|-----:|-------------:|-------:|------------:|
# |cycleway      | 14 749|     5|        606|     1|        15 360|       0|           45|
# |path          |      3|     1|      9 939|     0|         9 943|       0|           29|
# |secondary     |    119| 1 875|          0|   157|         2 151|   2 139|            6|
# |primary       |     76| 1 415|          0|    83|         1 574|   1 567|            5|
# |tertiary      |    125| 1 272|          0|   160|         1 557|   1 521|            5|
# |footway       |      9|     1|      1 515|     0|         1 525|       0|            4|
# |residential   |     87|   976|          0|   111|         1 174|   1 120|            3|
# |unclassified  |     54|   390|          1|   103|           547|     514|            2|
# |service       |      2|    30|          0|    57|            89|      37|            0|
# |living_street |      2|    18|          0|     5|            24|      19|            0|

base_highway  %>%
  filter(voie_pc > 0)  %>%
  mutate(tx_cyclable = 100 * voie_cyclable / voie_pc) %>%
  arrange(desc(voie_cyclable)) %>%
  knitr::kable(format.args = list(big.mark = " "), digits = c(rep(0,7),2))
# |highway       | piste| bande| voie_verte| autre| voie_cyclable| voie_pc| tx_cyclable|
# |:-------------|-----:|-----:|----------:|-----:|-------------:|-------:|-----------:|
# |secondary     |   119| 1 875|          0|   157|         2 151| 116 051|         1.9|
# |primary       |    76| 1 415|          0|    83|         1 574|  54 188|         2.9|
# |tertiary      |   125| 1 272|          0|   160|         1 557| 237 358|         0.7|
# |residential   |    87|   976|          0|   111|         1 174| 206 361|         0.6|
# |unclassified  |    54|   390|          1|   103|           547| 429 976|         0.1|
# |service       |     2|    30|          0|    57|            89| 205 936|         0.0|
# |living_street |     2|    18|          0|     5|            24|   3 179|         0.8|
# |busway        |     0|     1|          0|    21|            21|     142|        15.0|
# |trunk         |     0|     1|          0|     0|             1|  10 991|         0.0|

base_highway  %>% filter(voie_pc > 0)  %>% pull(highway)

base_highway  %>%
  filter(str_detect(highway, "_link"))  %>%
  mutate(tx_cyclable = 100 * voie_cyclable / voie_pc) %>%
  arrange(desc(voie_cyclable)) %>%
  knitr::kable(format.args = list(big.mark = " "), digits = c(rep(0,8),1))
# |highway        | piste| bande| voie_verte| autre| voie_cyclable| voie_pc| tx_cyclable|
# |:--------------|-----:|-----:|----------:|-----:|-------------:|-------:|-----------:|
# |primary_link   |     1|     7|          0|     0|             8|       0|         Inf|
# |secondary_link |     1|     4|          0|     0|             6|       0|         Inf|
# |tertiary_link  |     0|     2|          0|     0|             3|       0|         Inf|
# |trunk_link     |     0|     1|          0|     0|             1|       0|         Inf|
# |motorway_link  |     0|     0|          0|     0|             0|       0|         Inf|

# Tags Rémi Lardellier pour filter osm
highway_rl <- c("bridleway","bus_guideway","cycleway","living_street", "pedestrian",
                "primary","primary_link","residential","road","secondary",
                "secondary_link", "tertiary","tertiary_link","unclassified", 
                "footway","path","track")

# Tags Arthur pour les routes potentiellement cyclables
highway_a <- c("trunk", "living_street", "primary", "secondary", "tertiary", 
               "residential", "busway", "unclassified", "service")
setdiff(highway_rl,highway_a )
setdiff(highway_a,highway_rl )
# "trunk"   "busway"  "service"

