val sc = spark.sparkContext

// Leitura dos arquivos com as requisições HTTP NASA
val httpRequestJuly = sc.textFile("access_log_Jul95")
val httpRequestAugust = sc.textFile("access_log_Aug95")

// 1. Número de hosts únicos.
  // Realiza o split, cria Map com hosts
val hostsJuly = httpRequestJuly.map(_.split(" ")(0)).map(host => (host,1)).reduceByKey(_+_)
val hostsAugust = httpRequestAugust.map(_.split(" ")(0)).map(host => (host,1)).reduceByKey(_+_)
val totalHosts = (hostsJuly ++ hostsAugust).reduceByKey(_+_)

//trazer apenas os campos com contador em 1, ou seja, hosts unicos
val uniqueHosts = totalHosts.filter(_._2 == 1)

//Contar o número de linhas
val numberUniqueHost = uniqueHosts.count()

println("Número de hosts únicos: " + numberUniqueHost)



// 2. O total de erros 404.
val julyErr404 = httpRequestJuly.map(_.split(" ")).filter(_.contains("404"))
val augustErr404 = httpRequestAugust.map(_.split(" ")).filter(_.contains("404"))

val sumErr404 = julyErr404 ++ augustErr404

println("O total de erros 404: " + sumErr404.count())



// 3. Os 5 URLs que mais causaram erro 404.
val julyUrlErr404 = httpRequestJuly.map(_.split(" ")).filter(_.contains("404")).map(url => (url(6),1)).reduceByKey(_+_)
val augustUrlErr404 = httpRequestAugust.map(_.split(" ")).filter(_.contains("404")).map(url => (url(6),1)).reduceByKey(_+_)

val totalUrlErr404 = (julyUrlErr404 ++ augustUrlErr404).reduceByKey(_+_)
val top5UrlErr404 = totalUrlErr404.sortBy(_._2, false).take(5).map(x => x._1)

println("Os 5 URLs que mais causaram erro 404: " + top5UrlErr404.mkString("; "))



// 4. Quantidade de erros 404 por dia.
val julyErr404Day = httpRequestJuly.map(_.split(" ")).filter(_.contains("404")).map(time => (time(3).substring(1,12),1)).reduceByKey(_+_)
val augustErr404Day = httpRequestAugust.map(_.split(" ")).filter(_.contains("404")).map(time => (time(3).substring(1,12),1)).reduceByKey(_+_)

val totalErr404Day = (julyErr404Day ++ augustErr404Day)

print("Quantidade de erros 404 por dia: ")
totalErr404Day.collect().map(x => println(x))



// 5. O total de bytes retornados.
val httpRequestTotal = httpRequestJuly ++ httpRequestAugust
val checkedLine = httpRequestTotal.map(_.split(" ")).filter{_.size == 10} //Retira linha com problema
val returnedBytes = checkedLine.map(byte => byte(9)).filter(_ != "-" )

val sumReturedBytes = returnedBytes.map(x => x.toLong).reduce(_ + _)
println("O total de bytes retornados: " + sumReturedBytes.toString)
