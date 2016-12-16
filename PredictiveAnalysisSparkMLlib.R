starttime<-Sys.time()

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "D:/spark-2.0.0-bin-hadoop2.7")
}

library(SparkR)
library(caTools)
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(enableHiveSupport = FALSE,master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))



require(caTools)
set.seed(101) 
sample = sample.split(iris, SplitRatio = .7)
iris.train = subset(iris, sample == TRUE)
iris.test = subset(iris, sample == FALSE)

object.size(iris)

iris.trainDF<-suppressWarnings(as.DataFrame(iris.train))
iris.testDF<-suppressWarnings(as.DataFrame(iris.test))

irisGLM <- spark.glm(iris.trainDF, Sepal_Length ~ Sepal_Width + Species, family = "gaussian")
summary(irisGLM)

iris.predict<-predict(irisGLM,iris.testDF)
# showDF(iris.predict)

# 
# Calculation of R^2 squared
iris.df.train<-as.data.frame(iris.testDF)
iris.df.predictions<-as.data.frame(iris.predict)
meanofSepal.Length=mean(iris.df.train$Sepal_Length)
numerator<-sum((as.data.frame(iris.df.predictions$prediction)-meanofSepal.Length)^2)
denominator<-sum((as.data.frame(iris.df.train$Sepal_Length)-meanofSepal.Length)^2)

rsquared<-numerator/denominator
rsquared

# Calculation of RMSE
RMSE<-sqrt(mean((iris.df.train$Sepal_Length-iris.df.predictions$prediction)^2))
RMSE


irisdf<-as.data.frame(iris.testDF)
irisdf.predict<-as.data.frame(iris.predict)
index<-c(1:nrow(iris.predict))


plot(irisdf$Sepal_Length,index,xlab = "Sepal.Length",type = "p",ylab = "Index",sub ="Predictive Analysis of Sepal.Length using SparkMLlib",col="blue",pch=16)
points(irisdf.predict$predict,index,type = "p",col="red",pch=18)
legend(4.3,58,legend=c("ActualValue","PredictedValue"),pch=c(16,18),cex=0.75,col=c("blue","red")) 

Sys.time()-starttime

# Calculation of R^2 squared
meanofSepal.Length=mean(irisdf$Sepal_Length)
numerator<-sum((irisdf.predict$predict-meanofSepal.Length)^2)
denominator<-sum((irisdf$Sepal_Length-meanofSepal.Length)^2)

rsquared<-numerator/denominator

# Calculation of RMSE
RMSE<-sqrt(mean((irisdf$Sepal_Length-irisdf.predict$predict)^2))
