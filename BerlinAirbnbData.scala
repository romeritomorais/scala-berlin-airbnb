object datasets {
    val reviews_summary = "reviews_summary.csv"
};

// load dataset reviews_summary
val ReviewsSummaryDF = spark.read.option("inferSchema", true)
                                 .option("header", "true")
                                 .csv(datasets.reviews_summary)

// remove linhas vazias
val RemoverNullDF = ReviewsSummaryDF.na.drop()

// exibe o squena do dataframe
RemoverNullDF.printSchema()

// exibe o numero de linhas do dataframe
println(RemoverNullDF.count()+" linhas")

// cria novo dataframe renomeando colunas e definindo o tipo de dados do campo data
val NewDF = RemoverNullDF.select("listing_id", "date", "reviewer_name", "comments")
                                .withColumn("date", col("date")
                                .cast("date"))
                                .withColumnRenamed("date", "data")

// exibe o squena do dataframe
NewDF.printSchema()

NewDF.show(5)

// cria tabela temporária
NewDF.createOrReplaceTempView("table")

// cria novo dataframe a partir de consulta sql removendo campos vazios
val RemoveDuplicatesDF = spark.sql("""
SELECT
    year(data) AS year,
    month(data) AS month,
    day(data) AS day,
    reviewer_name AS name,
    comments
FROM table
ORDER BY year
""").na.drop()

RemoveDuplicatesDF.show(5)

RemoveDuplicatesDF.printSchema()

// salva do dados no formato parquet particionando por year
RemoveDuplicatesDF.write.partitionBy("year")
                  .format("parquet")
                  .mode("overwrite")
                  .save("partition_year.parquet")

// carrega os dados de arquivos parquet
val partition_year = "/home/romerito/Dropbox/tecnology/bigdata_analytics/datasets/partition_year.parquet"
val Load_ParquetDF = spark.read.load(partition_year)

// cria tabela temporária
Load_ParquetDF.createOrReplaceTempView("parquet")

// cria novo dataframe com dados agrupados do numero de linhas por year
val CountLinesPartitionDF = spark.sql(
    """
    SELECT
    year,
    count(*) AS lines_partition
    FROM parquet
    GROUP BY year
    """
    ).na.drop()

CountLinesPartitionDF.show()

// salva dados no formato parquet
RemoveDuplicatesDF.write.partitionBy("year")
                  .format("parquet")
                  .mode("overwrite")
                  .save("CountLinesPartition.parquet")


