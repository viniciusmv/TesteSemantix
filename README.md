# TesteSemantix

# Qual o objetivo do comando cache em Spark?
Este comando tem como objetivo armazenar em memória resultados ações realizadas em um RDD. A utilização deste comando traz vantagens quando são necessárias repetidas ações sobre o mesmo conjunto de dados, pois desta forma evita-se que o acesso direto ao disco para cada uma das ações e consequentemente melhorando a performance.


# O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
O Spark normalmente é mais rápido que o MapReduce devido a possibilidade de uso de memória. No MapReduce, para cada execução os dados acessados e gravados em discos, já o Spark permite que os dados sejam persistidos em memórias, desta forma minimiza o tempo de gravar e disco e leitura novamente pela próxima execução.


# Qual é a função do SparkContext?
O Spark Context é o objeto que conecta o Spark ao programa que está sendo desenvolvido. Ele pode ser acessado como uma variável em um programa que para utilizar os seus recursos.


# Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
O Resilient Distributed Datasets é o objeto principal do modelo de programação do Spark, pois é através deste objeto que serão executados os processamentos dos dados. Ele pode ser criado/construído a partir dados armazenados em sistema de arquivo tradicional ou no HDFS (HadoopDistributed File System) e em alguns Banco de Dados NoSQL, como Cassandra e HBase. Os RDD permitem execução de operações que representam transformações (como agrupamentos, filtros e mapeamentos entre os dados) ou ações (como contagens e persistências). 
Os RDDs são resiliente, pois permitem recuperar os dados persistidos em memória em caso de falha, armazenamento Distribuído em memória de diferentes nós através do cluster.


# GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
A vantagem do reduceByKey é devido ao mesmo já realizar o agrupamento da informação pela chave dentro de cada nó, desta forma minimizando o volume de dados trafegados na rede e também diminuindo uso de memória, o que consequentemente pode evitar operações de leitura e escrita em disco (que diminuiria a performance). Já no GroupByKey este agrupamento somente é realizado no final, fazendo com que todo grande volume de dados seja trafegado pela rede.


# Explique o que o código Scala abaixo faz.
val textFile = sc.textFile ( "hdfs://..." )

val counts = textFile.flatMap ( line => line . split ( " " ))
.map ( word => ( word , 1 ))
.reduceByKey ( _ + _ )

counts.saveAsTextFile ( "hdfs://..." )

1.	Na linha 1 é realizada a leitura de um arquivo texto (armazenado no HDFS) para uma variável imutável.
2.	Na linha 2 cada linha do arquivo texto é “splitada” por espaço (palavras) e depois todas as linhas transformadas é uma coleção.
3.	Na linha 3 cada elemento da coleção é transformado em um Map(chave, valor), onde a palavra é a chave e com valor unitário “1”.
4.	Na linha 4 é realizada uma operação agrupando as chaves idênticas e somando os valores.
5.	Na linha 5 é realizada a gravação do resultado (contagem de palavras do arquivo lido) em um arquivo texto no HFDS.
