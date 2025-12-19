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

# liste des tags utiles 
file_tags <- "./pistes-cyclables/_tags-osm.R"
liste_tags <- eval(parse(text = paste(readLines(file_tags), collapse = "\n")))

# Initialisation
data <- tibble()
for (tag in liste_tags) {
  data[[tag]] <- character()  # ou numeric(), etc.
}

last_dep <- "00"
for(code_dep in liste_dep) {
  message(code_dep)
  data_dep_2022 <- tryCatch(
    expr = {
      df <- aws.s3::s3read_using(
        FUN = arrow::read_parquet,
        object = glue("at36vc/api-ohsome-com-par-depts-2022/lg-com-dep{code_dep}.parquet"),
        bucket = BUCKET,
        opts = list("region" = "")
      )
    },
    error = function(e) { stop("dernier département (2022) : ", last_dep) }
  )
  data_dep_2025 <- tryCatch(
    expr = {
      aws.s3::s3read_using(
        FUN = arrow::read_parquet,
        object = glue("at36vc/api-ohsome-com-par-depts-2025/lg-com-dep{code_dep}.parquet"),
        bucket = BUCKET,
        opts = list("region" = "")
      )
    },
    error = function(e) { stop("dernier département (2025) : ", last_dep) }
  )
  last_dep <- code_dep
  data_dep <- bind_rows(
    data_dep_2022 %>% mutate(dep = !!code_dep, annee = 2022),
    data_dep_2025 %>% mutate(dep = !!code_dep, annee = 2025)
  )
  data_dep <- data_dep %>%
    group_by(pick(any_of(c("annee", "dep", liste_tags)))) %>%
    summarise(longueur = sum(longueur, na.rm = TRUE), .groups = "drop")
              
  data <- bind_rows(data_dep, data)
}

explo_var <- function(data, ..., .pct_par = NULL) {
  res <- data %>% 
    mutate(UN = 1) %>%
    group_by(annee, ...) %>%
    summarise(longueur = sum(longueur, na.rm = TRUE) / 1000, .groups = "drop") %>%
    pivot_wider(names_from = annee, values_from = longueur, names_prefix = "lg_") %>%
    mutate(EVO = (lg_2025-lg_2022)/lg_2022 * 100) %>%
    arrange(desc(lg_2025)) %>%
    group_by(pick(any_of(c("UN", .pct_par)))) %>%
    mutate(pct_2022 = 100 * lg_2022 / sum(lg_2022, na.rm = TRUE), 
           pct_2025 = 100 * lg_2025 / sum(lg_2025, na.rm = TRUE)) %>%
    ungroup()
  
} 


# Longueur et évolution selon highway
data %>% explo_var(highway) %>% head(8) %>% 
  kable(digits = c(0, -3, -3, 1, 1, 1),  format.args = list(big.mark = " ")) 
# |highway      | lg_2022| lg_2025|  EVO| pct_2022| pct_2025|
# |:------------|-------:|-------:|----:|--------:|--------:|
# |track        | 165 000| 184 000| 11.8|     33.4|     33.9|
# |unclassified |  85 000|  83 000| -1.9|     17.2|     15.3|
# |path         |  54 000|  69 000| 28.9|     10.9|     12.8|
# |tertiary     |  53 000|  53 000|  0.1|     10.8|      9.8|
# |service      |  35 000|  47 000| 34.7|      7.1|      8.7|
# |residential  |  42 000|  43 000|  1.8|      8.6|      7.9|
# |secondary    |  26 000|  26 000| -0.2|      5.4|      4.9|
# |primary      |  12 000|  12 000|  2.8|      2.4|      2.2|

# Tags Rémi Lardellier pour filter osm
highway_rl <- c("bridleway","bus_guideway","cycleway","living_street", "pedestrian",
                "primary","primary_link","residential","road","secondary",
                "secondary_link", "tertiary","tertiary_link","unclassified", 
                "footway","path","track")

# Tags Arthur pour les routes potentiellement cyclables
highway_a <- c("trunk", "living_street", "primary", "secondary", "tertiary", 
                "residential", "busway", "unclassified", "service")

# Pas d'enjeu : les tags importants sont pour les vélos ou les piétons
res %>% filter(highway %in% setdiff(highway_rl, highway_a)) %>%
  kable(digits = c(0, -3, -3, 1, 1, 1),  format.args = list(big.mark = " ")) 
# |highway        | lg_2022| lg_2025|   EVO| pct_2022| pct_2025|
# |:--------------|-------:|-------:|-----:|--------:|--------:|
# |track          | 165 000| 184 000|  11.8|     33.4|     33.9|
# |path           |  54 000|  69 000|  28.9|     10.9|     12.8|
# |footway        |   8 000|  11 000|  34.4|      1.7|      2.0|
# |cycleway       |   2 000|   2 000|  21.6|      0.4|      0.5|
# |pedestrian     |   1 000|       0|  -7.7|      0.1|      0.1|
# |bridleway      |       0|       0|   0.1|      0.1|      0.1|
# |primary_link   |       0|       0|   3.1|      0.0|      0.0|
# |road           |       0|       0| -16.9|      0.0|      0.0|
# |secondary_link |       0|       0|  -8.4|      0.0|      0.0|
# |tertiary_link  |       0|       0| -10.1|      0.0|      0.0|
# |bus_guideway   |       0|      NA|    NA|      0.0|       NA|

# Hausse des routes "unclassified" ou "service" avec la surface "asphalt"
data %>% 
  filter(highway %in% c("unclassified", "service")) %>%
  explo_var(highway, surface, .pct_par = "highway") %>%
  arrange(desc(lg_2025)) %>%
  head(12) %>% 
  arrange(highway) %>% 
  kable(digits = c(0, 0, -2, -2, 1, 1, 1),  format.args = list(big.mark = " ")) 
# |highway      |surface   | lg_2022| lg_2025|   EVO| pct_2022| pct_2025|
# |:------------|:---------|-------:|-------:|-----:|--------:|--------:|
# |service      |NA        |  31 400|  39 700|  26.6|     89.7|     84.3|
# |service      |asphalt   |   2 400|   5 500| 125.5|      7.0|     11.7|
# |service      |gravel    |     300|     500|  49.9|      0.9|      1.0|
# |service      |unpaved   |     200|     400|  62.6|      0.7|      0.8|
# |service      |compacted |     100|     300| 117.0|      0.4|      0.7|
# |service      |ground    |     200|     200|  39.1|      0.5|      0.5|
# |unclassified |NA        |  77 100|  69 700|  -9.6|     91.0|     83.9|
# |unclassified |asphalt   |   6 500|  11 900|  82.6|      7.7|     14.3|
# |unclassified |paved     |     500|     500|   1.0|      0.6|      0.6|
# |unclassified |gravel    |     200|     300|  71.2|      0.2|      0.4|
# |unclassified |unpaved   |     100|     200|  38.3|      0.2|      0.2|
# |unclassified |compacted |       0|     200| 306.3|      0.1|      0.2|

# Baisse de smouthness excellent ou good avec asphalt non renseigné (permet de compenser)
data %>% 
  filter(highway %in% c("unclassified", "service")) %>%
  mutate(smouth = smoothness %in% c("excellent", "good")) %>%
  filter(is.na(surface)) %>%
  explo_var(smouth) %>% 
  kable(digits = c(0, -2,  1, 1, 1),  format.args = list(big.mark = " ")) 
# smouth | lg_2022|   lg_2025|   EVO| pct_2022| pct_2025|
# |:------|-------:|---------:|-----:|--------:|--------:|
# |FALSE  | 108 500| 109 436.7|   0.9|      100|      100|
# |TRUE   |       0|      20.1| -24.0|        0|        0|

# Hausse des routes "unclassified" ou "service" avec la surface "asphalt"
data %>% 
  filter(highway %in% c("unclassified", "service")) %>%
  explo_var(highway, tracktype, .pct_par = "highway") %>%
  head(12) %>% 
  arrange(highway) %>% 
  kable(digits = c(0, 0, -2, -2, 1, 1, 1),  format.args = list(big.mark = " ")) 
# |highway      |tracktype | lg_2022| lg_2025|   EVO| pct_2022| pct_2025|
# |:------------|:---------|-------:|-------:|-----:|--------:|--------:|
# |service      |NA        |  34 600|  46 600|  34.7|     98.8|     98.8|
# |service      |grade1    |     200|     300|  26.8|      0.7|      0.6|
# |service      |grade2    |     100|     200|  50.5|      0.4|      0.4|
# |service      |grade3    |       0|     100|  86.8|      0.1|      0.1|
# |service      |grade4    |       0|       0| -11.3|      0.0|      0.0|
# |service      |grade5    |       0|       0|  -3.8|      0.0|      0.0|
# |unclassified |NA        |  84 100|  82 400|  -2.1|     99.3|     99.1|
# |unclassified |grade1    |     400|     400|   1.3|      0.4|      0.4|
# |unclassified |grade2    |     200|     300|  83.1|      0.2|      0.4|
# |unclassified |grade3    |       0|     100|  18.1|      0.1|      0.1|
# |unclassified |grade4    |       0|       0|  56.6|      0.0|      0.0|
# |unclassified |grade5    |       0|       0| 111.4|      0.0|      0.0|

# Accès
data %>%
  filter(highway %in% c('tertiary', 'unclassified', 'service')) %>% 
  explo_var(access) %>%
  arrange(desc(lg_2025)) %>% head() %>%
  kable(digits = c(0, -2, -2, 1, 1, 1, 1),  format.args = list(big.mark = " ")) 
# |access      | lg_2022| lg_2025|   EVO| pct_2022| pct_2025|
# |:-----------|-------:|-------:|-----:|--------:|--------:|
# |NA          | 166 600| 174 100|   4.5|     96.4|     94.9|
# |private     |   3 800|   6 600|  71.6|      2.2|      3.6|
# |no          |     800|     800|  11.0|      0.4|      0.5|
# |yes         |     600|     600| -11.5|      0.4|      0.3|
# |destination |     400|     500|  17.8|      0.3|      0.3|
# |permissive  |     200|     300|  47.1|      0.1|      0.2|

data %>%
  filter(highway %in% c('tertiary', 'unclassified', 'service')) %>% 
  explo_var(motor_vehicle) %>%
  arrange(desc(lg_2025)) %>% head() %>%
  kable(digits = c(0, -2, -2, 1, 1, 1, 1),  format.args = list(big.mark = " ")) 
#  motor_vehicle | lg_2022| lg_2025|   EVO| pct_2022| pct_2025|
# |:-------------|-------:|-------:|-----:|--------:|--------:|
# |NA            | 172 000| 182 400|   6.0|     99.6|     99.4|
# |yes           |     200|     300|  13.7|      0.1|      0.1|
# |no            |     200|     200|  19.0|      0.1|      0.1|
# |private       |     100|     200|  64.2|      0.1|      0.1|
# |destination   |     100|     100| 134.7|      0.0|      0.1|
# |designated    |     100|     100|  40.3|      0.0|      0.0|

data %>%
  filter(highway %in% c('tertiary', 'unclassified', 'service')) %>% 
  explo_var(motorcar) %>%
  arrange(desc(lg_2025)) %>% head() %>%
  kable(digits = c(0, 0, 0, 1, 1, 1, 1),  format.args = list(big.mark = " ")) 
# |motorcar     | lg_2022| lg_2025|  EVO| pct_2022| pct_2025|
# |:------------|-------:|-------:|----:|--------:|--------:|
# |NA           | 172 700| 183 300|  6.2|      100|      100|
# |yes          |       0|       0| -7.4|        0|        0|
# |no           |       0|       0| 26.9|        0|        0|
# |agricultural |       0|       0|  2.4|        0|        0|
# |private      |       0|       0| 67.1|        0|        0|
# |destination  |       0|       0| -2.0|        0|        0|

# DBI::dbDisconnect(con, shutdown = TRUE)
con <- DBI::dbConnect(duckdb::duckdb())

# Applique le schéma des aménagements cyclables
DBI::dbExecute(con, "DROP VIEW IF EXISTS data_for_sql")
duckdb::duckdb_register(con, "data_for_sql", data)
dt <- DBI::dbGetQuery(con, paste0("
  SELECT
    annee, dep, highway,
    -- Filtre Arthur
    CASE
    		WHEN 
    			(highway IN ('trunk', 'living_street', 'primary', 'secondary', 'residential', 'busway'))
    			OR (highway IN ('tertiary', 'unclassified', 'service') AND (
    			    smoothness IN ('excellent', 'good') 
    			    OR tracktype = 'grade1' 
    			    OR surface IN  ('asphalt', 'paving_stones', 'chipseal', 'concrete')
    			))
    		THEN 1
    		ELSE 0
    END filtre_pc_0,    
    -- Filtre inspiré d'Arthur
    CASE
    		WHEN 
    			(highway IN ('trunk', 'living_street', 'primary', 'secondary', 'tertiary', 'residential', 'busway'))
    			OR (highway IN ('unclassified', 'service') AND (
    			    smoothness IN ('excellent', 'good') 
    			    OR tracktype = 'grade1' 
    			    OR surface IN  ('asphalt', 'paving_stones', 'chipseal', 'concrete')
    			))
    		THEN 1
    		ELSE 0
    END filtre_pc_1,
    -- Nouveau filtre
    CASE
    		WHEN 
    			(highway IN ('primary', 'secondary', 'busway'))
    			OR (highway IN ('living_street', 'trunk',  'residential', 'tertiary', 'unclassified', 'service')
    			    AND (access IS NULL OR access != 'no')
    			    AND (motor_vehicle IS NULL OR motor_vehicle != 'no')
    			    AND (motorcar IS NULL OR motorcar != 'no')
    			    AND (surface IS NULL OR surface = 'asphalt')
    			    AND (smoothness IS NULL OR smoothness IN ('excellent', 'good'))
    			    AND (tracktype IS NULL OR tracktype = 'grade1') 
    			    AND (bicycle IS NULL OR bicycle != 'designated')
    			    AND (designation IS NULL OR designation != 'greenway')
    			)
    		THEN 1
    		ELSE 0
    END filtre_pc_2,
    longueur,
  FROM data_for_sql
  "))

dt %>% 
  group_by(annee) %>%
  summarise(longueur_0 = sum(filtre_pc_0 * longueur) / 1000, 
            longueur_1 = sum(filtre_pc_1 * longueur) / 1000, 
            longueur_2 = sum(filtre_pc_2 * longueur) / 1000, 
            .groups = "drop")  %>%
  pivot_longer(cols = contains("longueur"), names_to = "filtre") %>%
  pivot_wider(names_from = annee, values_from = value, names_prefix = "lg_")  %>%
  mutate(EVO = (lg_2025-lg_2022)/lg_2022 * 100)  %>% 
  kable(digits = c(0, -2, -2, 1, 1),  format.args = list(big.mark = " ")) 

# |filtre     | lg_2022| lg_2025|  EVO|
# |:----------|-------:|-------:|----:|
# |longueur_0 | 100 300| 118 900| 18.5|
# |longueur_1 | 145 400| 155 100|  6.6|
# |longueur_2 | 250 900| 260 600|  3.9|
