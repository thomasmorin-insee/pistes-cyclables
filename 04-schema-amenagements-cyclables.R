################################################################################
#   
# Schéma des aménagements cyclables
# 
# Entrée :  at36vc/longueur-com/longueur-com-{annee}.parquet
#
################################################################################

library(tidyverse)
library(arrow)
library(duckdb)
library(DBI)
library(glue)
library(knitr)

# Coffre
BUCKET <- "zg6dup"

# Input/output
annee <- 2025
input_file <- "at36vc/longueur-com/longueur-com-{annee}.parquet"

# Longueur des voies cyclables
longueurs_com <- aws.s3::s3read_using(
  FUN = arrow::read_parquet,
  object = glue(input_file),
  bucket = BUCKET,
  opts = list("region" = "")
)

# Création d'une vue pour appliquer le schéma sql
# Connexion duckdb
# DBI::dbDisconnect(con, shutdown = TRUE)
con <- DBI::dbConnect(duckdb::duckdb())
DBI::dbExecute(con, "DROP VIEW IF EXISTS table_sql")
duckdb::duckdb_register(con, "table_sql", longueurs_com)

# Requête sql des aménagements cyclables sur la base des longueur par commune
file_sql <- "sql/ame_insee.sql"
requete_sql <- readChar(file_sql, file.info(file_sql)$size)

# Applique le schéma
tbl_parquet <- tbl(con, sql(requete_sql))
colnames(tbl_parquet)

# Agrège par commune
dt_0 <- tbl_parquet %>%
  group_by(codgeo, filtre_ac, filtre_pc1, ame_d, ame_g, sens_d, sens_g) %>%
  summarise(longueur = sum(longueur, na.rm = TRUE), .groups ="drop") %>%
  collect()

# Aménagement droite France entière (sans tenir compte des doubles sens)
dt_0 %>% 
  filter(filtre_ac, ame_d != "AUCUN") %>%
  group_by(ame_d) %>%
  summarise(km = sum(longueur, na.rm= TRUE) * .001, .groups ="drop") %>%
  arrange(desc(km)) %>%
  kable(digits = 0)  
# |ame_d                                         |    km|
# |:---------------------------------------------|-----:|
# |PISTE CYCLABLE                                | 20256|
# |AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE | 15448|
# |VOIE VERTE                                    | 13600|
# |BANDE CYCLABLE                                |  9556|
# |AUTRE                                         |  6628|
# |COULOIR BUS+VELO                              |   793|
# |CHAUSSEE A VOIE CENTRALE BANALISEE            |   600|
# |VELO RUE                                      |    65|
# |RAMPE                                         |    21|

# Aménagement gauche France entière (sans tenir compte des doubles sens)
dt_0 %>% 
  filter(filtre_ac, ame_g != "AUCUN") %>%
  group_by(ame_g) %>%
  summarise(km = sum(longueur, na.rm= TRUE) * .001, .groups ="drop") %>%
  arrange(desc(km)) %>%
  kable(digits = 0)  
# |ame_g                                         |   km|
# |:---------------------------------------------|----:|
# |BANDE CYCLABLE                                | 6628|
# |AUTRE                                         | 4681|
# |DOUBLE SENS CYCLABLE NON MATERIALISE          | 1590|
# |DOUBLE SENS CYCLABLE BANDE                    |  729|
# |CHAUSSEE A VOIE CENTRALE BANALISEE            |  600|
# |PISTE CYCLABLE                                |  465|
# |COULOIR BUS+VELO                              |  293|
# |AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE |   99|
# |DOUBLE SENS CYCLABLE PISTE                    |   65|
# |VELO RUE                                      |   24|
  
# Fusionne ame_d et ame_g
dt <-   dt_0 %>%
  select(codgeo, filtre_ac, filtre_pc1, sens = sens_d, ame = ame_d, longueur) %>%
  bind_rows(
    dt_wide %>%
      select(codgeo, filtre_ac, filtre_pc1, sens = sens_g, ame = ame_g, longueur)
  ) %>%
  
  # Pondération tenant compte du double sens
  mutate(pond = case_when(sens == "BIDIRECTIONNEL" ~ 2, TRUE ~ 1))  %>% 
  
  # Filtre pour les aménagements cyclables
  mutate(ame = ifelse(filtre_ac, ame, "AUCUN")) %>%
  
  # On ne retient que les routes potentiellement cycalbles ou aménagées 
  filter(filtre_pc1 | ame != "AUCUN") %>%
  
  group_by(codgeo, filtre_pc1, ame) %>%
  
  summarise(longueur = sum(pond * longueur, na.rm = TRUE), .groups ="drop")

# Exemple de Strabourg
dt %>% filter(codgeo=="67482") %>%
  group_by(ame) %>%
  summarise(km = sum(longueur, na.rm= TRUE) * .001, .groups ="drop") %>%
  arrange(desc(km)) %>%
  kable(digits = 0)
# |ame                                           |   km|
# |:---------------------------------------------|----:|
# |AUCUN                                         | 1004|
# |PISTE CYCLABLE                                |  267|
# |AUTRE                                         |  105|
# |VOIE VERTE                                    |   90|
# |AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE |   53|
# |BANDE CYCLABLE                                |   39|
# |DOUBLE SENS CYCLABLE BANDE                    |   10|
# |DOUBLE SENS CYCLABLE NON MATERIALISE          |    8|
# |COULOIR BUS+VELO                              |    7|
# |VELO RUE                                      |    3|
# |DOUBLE SENS CYCLABLE PISTE                    |    1|
# |RAMPE                                         |    0|

# Longueur des routes potentiellement cyclables
dt_pc <- 
  left_join(
    x = dt %>%
    filter(filtre_pc1) %>%
    group_by(codgeo) %>%
    summarise(ROUTE_PC = sum(longueur, na.rm= TRUE) * .001, .groups ="drop"),
    y = dt %>%
      filter(ame != "AUCUN") %>%
      group_by(codgeo) %>%
      summarise(CYCLABLE = sum(longueur, na.rm= TRUE) * .001, .groups ="drop"),
    by = "codgeo"
  ) %>%
  mutate(TX_CYCLABLE = 100 * CYCLABLE / ROUTE_PC)

# Exemple de Strabourg
dt_pc %>% filter(codgeo=="67482") %>%
  kable(digits = 0)
# |codgeo | ROUTE_PC| CYCLABLE| TX_CYCLABLE|
# |:------|--------:|--------:|-----------:|
# |67482  |     1149|      584|          51|

# Longueurs cyclables en colonne par commune
dt_wide_cyclable <- dt %>%
  filter(ame != "AUCUN") %>%
  group_by(codgeo, ame) %>%
  summarise(km = sum(longueur, na.rm= TRUE) * .001, .groups ="drop") %>%
  pivot_wider(id_cols = codgeo, names_from = ame, values_from = km, values_fill = 0)

# Exemple de strasbourg
dt_wide_cyclable %>% filter(codgeo=="67482") 


res <- left_join(dt_wide_cyclable)