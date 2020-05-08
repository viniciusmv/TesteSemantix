from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, udf, lit, sum, desc, asc, sum
from pyspark.sql.types import StringType, DateType, DoubleType
from datetime import datetime

# Atribui sessao do Spark
spark = SparkSession.builder.appName("teste_semantix").enableHiveSupport().getOrCreate()
sc = spark.sparkContext

# Leitura dos arquivos com as requisições HTTP NASA
acess_log_jul95_df = spark.read.text("access_log_Jul95")
acess_log_aug95_df = spark.read.text("access_log_Aug95")
acess_log_df = acess_log_jul95_df.unionAll(acess_log_aug95_df).cache()

#   1. Número de hosts únicos.
num_hosts_unicos = acess_log_df.select(split(col("value"), ' ')[0].alias("host")).distinct().count()
print('1. Número de hosts únicos.')
print('Número de hosts únicos: ' + str(num_hosts_unicos))

print(' ')
print('*******************************************************')
print(' ')

#   2. O total de erros 404.
def get_col_error(line):
    try:
        column_erro = line.split(' ')[-2]
        return column_erro
    except:
        pass

get_col_error_udf = udf(get_col_error, StringType())

acess_log_erro_404_df = acess_log_df.withColumn("erro", get_col_error_udf(col("value"))).filter(col("erro") == '404').cache()
erros_404 = acess_log_erro_404_df.count()
print('2. O total de erros 404.')
print('O total de erros 404: ' + str(erros_404))

print(' ')
print('*******************************************************')
print(' ')

#   3. Os 5 URLs que mais causaram erro 404.
#df = acess_log_erro_404_df.select(concat(split(col("value"), ' ')[0], split(split(col("value"), '\"')[1], ' ')[1]).alias("url"), lit(1).alias("qtd")) \
urls_erro_404 = acess_log_erro_404_df.select(split(split(col("value"), '\"')[1], ' ')[1].alias("url"), lit(1).alias("qtd")) \
    .groupBy(col("url")).agg(sum(col("qtd")).alias("sum_qtd")).orderBy(desc("sum_qtd")).take(5)

print('Os 5 URLs que mais causaram erro 404.')
for url in urls_erro_404:
    print('url = ' + str(url[0]) + ', qtd = ' + str(url[1]))

print(' ')
print('*******************************************************')
print(' ')

#   4. Quantidade de erros 404 por dia.
convert_date = udf(lambda str_date: datetime.strptime(str_date, '%d/%b/%Y').date(), DateType())
dia_erro_404 = acess_log_erro_404_df.select(split(split(col("value"), '\[')[1], ':')[0].alias("dia"), lit(1).alias("qtd")) \
    .withColumn("dia", convert_date(col("dia"))) \
    .groupBy(col("dia")).agg(sum(col("qtd")).alias("sum_qtd")).orderBy(asc("dia")).collect()

print('4. Quantidade de erros 404 por dia.')
for dia in dia_erro_404:
    print("{}: {}".format(datetime.strftime(dia[0], '%d/%b/%Y'), dia[1]))

print(' ')
print('*******************************************************')
print(' ')

#   5. O total de bytes retornados.
def get_col_bytes(line):
    try:
        column_byte = line.split(' ')[-1]
        if column_byte == '-':
            column_byte = 0
        return column_byte
    except:
        pass
    return 0
get_col_bytes_udf = udf(get_col_error, StringType())
total_bytes = acess_log_df.select(get_col_bytes_udf(col("value")).cast(DoubleType()).alias("bytes")).agg(sum(col("bytes"))).collect()[0][0]
print('5. O total de bytes retornados.')
print('O total de bytes retornados: ' + str(total_bytes))