# Déplacer les output
if(FALSE) {
  old <- "api-ohsome-com-par-depts-{annee}/lines-com-dep{code_dep}.parquet"
  new <-  "at36vc/api-ohsome-com-par-depts-{annee}/lg-com-dep{code_dep}.parquet"
  for(code_dep in liste_dep) {
    data <- aws.s3::s3read_using(
      FUN = arrow::read_parquet,
      object = glue(old),
      bucket = BUCKET,
      opts = list("region" = "")
    )
    aws.s3::s3write_using(
      data,
      FUN = arrow::write_parquet,
      object = glue(new),
      bucket = BUCKET,
      opts = list("region" = "")
    )
    print(glue(new))
  }
}
