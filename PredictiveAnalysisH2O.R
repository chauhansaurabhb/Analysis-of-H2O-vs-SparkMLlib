starttime<-Sys.time()
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "D:/spark-2.0.0-bin-hadoop2.7")
}

library(SparkR)
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(enableHiveSupport = FALSE,master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))

library(h2o)
h2o.init(nthreads = -1)

irish2o<-as.h2o(iris)

iris.split=h2o.splitFrame(data=irish2o,ratios = .7)

iris.train<-iris.split[[1]]
iris.test<-iris.split[[2]]

Y = "Sepal.Length"
X = c("Sepal.Width","Species") 

irish2o.glm<-h2o.glm(training_frame =iris.train,x=X,y=Y,family = "gaussian")
summary(irish2o.glm)



irish2o.predict<-h2o.predict(object = irish2o.glm,newdata = iris.test)
# Additional

# irish2o.predict<-h2o.predict(object = irish2o.glm,newdata = iris.train)
# # show(irish2o.predict)
# 
# # Calculation of R^2 squared
# meanofSepal.Length=mean(iris.train$Sepal.Length)
# numerator<-sum((irish2o.predict$predict-meanofSepal.Length)^2)
# denominator<-sum((iris.train$Sepal.Length-meanofSepal.Length)^2)
# 
# rsquared<-numerator/denominator
# rsquared
# 
# # Calculation of RMSE
# RMSE<-sqrt(mean((iris.train$Sepal.Length-irish2o.predict$predict)^2))
# RMSE

object.size(iris)

# Plot graph
index=c(1:nrow(irish2o.predict))
irisdf<-as.data.frame(iris.test)
irisdf.predict<-as.data.frame(irish2o.predict)
plot(irisdf$Sepal.Length,index,xlab = "Sepal.Length",type = "p",ylab = "Index",sub ="Predictive Analysis of Sepal.Length using H2O in Spark",col="blue",pch=16)
points(irisdf.predict$predict,index,type = "p",col="red",pch=18)
legend(4.3,38, legend=c("ActualValue","PredictedValue"),pch=c(16,18),cex=0.75,col=c("blue","red"))

Sys.time()-starttime

