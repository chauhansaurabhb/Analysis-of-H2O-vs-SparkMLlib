starttime<-Sys.time()
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "D:/spark-2.0.0-bin-hadoop2.7")
}

library(SparkR)

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(enableHiveSupport = FALSE,master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))
# library(elastic)
# connect()
library(h2o)
h2o.init(nthreads = -1,max_mem_size = "6g")
# airlines.h2o = h2o.importFile(path ="D:/airlines_all.05p.csv", destination_frame = "airlines.h2o")
airlines.h2o = h2o.importFile(path ="D:/allyears2k.csv", destination_frame = "airlines.h2o")
summary(airlines.h2o)

airlines.split=h2o.splitFrame(data=airlines.h2o,ratios = .7)

airlines.train<-airlines.split[[1]]
airlines.test<-airlines.split[[2]]

Y = "IsArrDelayed"
X = c("ArrDelay","DepDelay","IsDepDelayed") 

airlinesh2o.glm<-h2o.glm(training_frame =airlines.train,x=X,y=Y,family = "binomial",alpha = 0.5)
summary(airlinesh2o.glm)

object.size(airlines.h2o)

airlinesh2o.predict<-h2o.predict(object = airlinesh2o.glm,newdata = airlines.test)
# show(airlinesh2o.predict)
Sys.time()-starttime

