starttime<-Sys.time()

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "D:/spark-2.0.0-bin-hadoop2.7")
}

library(SparkR)
library(caTools)
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(enableHiveSupport = FALSE,master = "local[*]", sparkConfig = list(spark.driver.memory = "6g"))


airlinesDF<- read.df("D:/airlines_all.05p.csv","csv",header = "true", inferSchema = "true", na.strings = "NA")
# showDF(airlinesDF)
# 
# require(caTools)
# set.seed(101) 
# 
# sample = sample.split(as.data.frame(airlinesDF), SplitRatio = .7)
# airlinesDF.train = subset(airlinesDF, sample == TRUE)
# airlinesDF.test = subset(airlinesDF, sample == FALSE)
# 
# object.size(iris)
# 
# iris.trainDF<-suppressWarnings(as.DataFrame(iris.train))
# iris.testDF<-suppressWarnings(as.DataFrame(iris.test))

# 
# Y = "IsArrDelayed"
# X = c("ArrDelay","DepDelay","IsDepDelayed")
# airlinesGLM <- spark.glm(airlinesDF,IsArrDelayed~ArrDelay+DepDelay+IsDepDelayed,family = "binomial")

airlinesGLM <- glm(IsArrDelayed~ArrDelay+DepDelay+IsDepDelayed,data=airlinesDF,family = "binomial")
summary(airlinesGLM)

airlinesGLM.predict<-predict(airlinesGLM,airlinesDF)
showDF(airlinesGLM.predict)
Sys.time()-starttime
