#' 
#' oil.R
#' 2015-02-21 
#'

# load libraries, set WD ------
setwd("C:/ydisk/Projects/oil_fut")
library(Quandl)
Quandl.auth("RE4zjdYzZ8yWUtsYdXGt")
library(plyr)
library(dplyr)
library(lubridate)
library(data.table)
library(xts)
library(reshape2)
library(googleVis)
library(quantmod)
    

# generate codes(tickers) ------
monthCode<-c("F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z")

# CL - WTI
yearCL<-sort(rep(1983:2022,12)) 
tickerCL<-paste(monthCode,yearCL,sep="")[-c(1:5,457:461, 463:467, 469:473, 475:479)]
tickerCLexp<-tickerCL[c(1:381)] # expired contracts
tickerCLcur<-tickerCL[-c(1:381)] # non-expired contracts
codeCL<-paste("CME/CL", tickerCL, sep="")
codeCLexp<-paste("CME/CL", tickerCLexp, sep="")
codeCLcur<-paste("CME/CL", tickerCLcur, sep="")

# B - Brent 
yearB<-sort(rep(1993:2021,12))
tickerB<-paste(monthCode,yearB,sep="")[-12]
tickerBexp<-tickerB[c(1:266)]
tickerBcur<-tickerB[-c(1:266)]
codeB<-paste("ICE/B", tickerB, sep="")
codeBexp<-paste("ICE/B", tickerBexp, sep="")
codeBcur<-paste("ICE/B", tickerBcur, sep="")

### load and save expired contracts data -------
# call only one time

# CL
dataCLexp<-data.frame(Date="",Settle="", Code="")
dataCLexp<-dataCLexp[-1,]

for (i in 1:length(tickerCLexp)) {
    
    data<-Quandl(paste(codeCLexp[i],".6",sep=""))
    data$Code<-tickerCLexp[i]
    
    dataCLexp<-rbind(dataCLexp, data)
    
}

write.table(dataCLexp, "dataCLexp.csv", sep=";", row.names = FALSE, dec=",")

# B 
# load and save dataBexp 
dataBexp<-data.frame(Date="",Settle="", Code="")
dataBexp<-dataBexp[-1,]

for (i in 1:length(tickerBexp)) {
    
    data<-Quandl(paste(codeBexp[i],".4",sep=""))
    data$Code<-tickerBexp[i]
    
    dataBexp<-rbind(dataBexp, data)
    
}

write.table(dataBexp, "dataBexp.csv", sep=";", row.names = FALSE, dec=",")

# update non-expired contracts data -----

# CL
dataCLcur<-data.frame(Date="",Settle="", Code="")
dataCLcur<-dataCLcur[-1,]

for (i in 1:length(tickerCLcur)) {
    
    data<-Quandl(paste(codeCLcur[i],".6",sep=""))
    data$Code<-tickerCLcur[i]
    
    dataCLcur<-rbind(dataCLcur, data)
    
}

write.table(dataCLcur, "dataCLcur.csv", sep=";", row.names = FALSE, dec=",")

# B 
dataBcur<-data.frame(Date="",Settle="", Code="")
dataBcur<-dataBcur[-1,]

for (i in 1:length(tickerBcur)) {
    
    data<-Quandl(paste(codeBcur[i],".4",sep=""))
    data$Code<-tickerBcur[i]
    
    dataBcur<-rbind(dataBcur, data)
    
}

write.table(dataBcur, "dataBcur.csv", sep=";", row.names = FALSE, dec=",")

# load local files and tidy data ------

CLexp<-data.table(read.table("dataCLexp.csv",sep=";", header=TRUE, colClasses = "character"))
CLcur<-data.table(read.table("dataCLcur.csv",sep=";", header=TRUE, colClasses = "character"))
Bexp<-data.table(read.table("dataBexp.csv",sep=";", header=TRUE, colClasses = "character"))
Bcur<-data.table(read.table("dataBcur.csv",sep=";", header=TRUE, colClasses = "character"))

CLcurDate<-data.table(read.table("CLcurDate.csv",sep=";", header=TRUE, colClasses = "character"))
BcurDate<-data.table(read.table("BcurDate.csv",sep=";", header=TRUE, colClasses = "character"))

CLexp$Date<-as.Date(CLexp$Date)
CLexp$Settle<-as.numeric(gsub(",", ".",CLexp$Settle))

CLcur$Date<-as.Date(CLcur$Date)
CLcur$Settle<-as.numeric(gsub(",", ".",CLcur$Settle))

Bexp$Date<-as.Date(Bexp$Date)
Bexp$Settle<-as.numeric(gsub(",", ".",Bexp$Settle))

Bcur$Date<-as.Date(Bcur$Date)
Bcur$Settle<-as.numeric(gsub(",", ".",Bcur$Settle))

CLcurDate$ExpDate<-as.Date(CLcurDate$ExpDate)
BcurDate$ExpDate<-as.Date(BcurDate$ExpDate)

# make Expiration Date data ------
CLexpDate<-CLexp[,.(ExpDate=first(Date)), by=Code]
BexpDate<-Bexp[,.(ExpDate=first(Date)), by=Code]

CLdate<-rbind(CLexpDate, CLcurDate)
Bdate<-rbind(BexpDate, BcurDate)

# combine data -------

CL<-rbind(CLexp,CLcur)
B<-rbind(Bexp,Bcur)

CL<-join(CL,CLdate)
B<-join(B,Bdate)

CL$mCode<-CL$Code
CL$Code<-"CL"

B$mCode<-B$Code
B$Code<-"B"

data<-rbind(CL,B)
data$daysTillExp<-as.numeric(data$ExpDate-data$Date)

# fix errors in raw data
data$Settle[data$Code=="B" & data$mCode=="H1995" & data$Date=="1993-09-13"]<-17

# build spread ---------
sdata<-dcast(data,Date+mCode ~ Code, value.var="Settle")

sdata<-sdata[!is.na(sdata$B),]
sdata<-sdata[!is.na(sdata$CL),]

# dd<-dcast(data,Date+mCode ~ Code, value.var="ExpDate") # then min of Date by row
names(sdata)[2]<-"Code"
sdata<-join(sdata,Bdate)
sdata$daysTillExp<-as.numeric(sdata$ExpDate-sdata$Date)
sdata$spread<-sdata$B-sdata$CL
sdata$spreadP<-100*sdata$spread/(0.5*sdata$B+0.5*sdata$CL)

# load 1st generic contracts ---------
CL1<-Quandl("CHRIS/CME_CL1.6")
B1<-Quandl("CHRIS/ICE_B1.4")

# plot motion chart forward curve ---------

fc<-data[,.(mCode,Date,daysTillExp, Settle,Code)]
fc$mCode<-paste(fc$Code,fc$mCode,sep="")
fc1<-fc[Date>"2015-03-01"]
fc2<-fc[daysTillExp<366 & Date>"2014-09-30"]

fc2$daysTillExp<-fc2$daysTillExp*(-1)

fcurve1<- gvisMotionChart(fc1, "mCode", "Date", options = list(width = 1600, height = 900))
plot(fcurve1)

fcurve2<- gvisMotionChart(fc2, "mCode", "Date", options = list(width = 1000, height = 800))
plot(fcurve2)

# plot motion chart spread curve -------

sdata<-data.table(sdata)
sc<-sdata[,.(Code,Date,daysTillExp, spread)]
spc<-sdata[,.(Code,Date,daysTillExp, spreadP)]

sc1<-sc[Date>"2007-01-01"]
sc2<-sc[daysTillExp<366 & Date>"2015-01-01"]


scurve1<- gvisMotionChart(sc1, "Code", "Date", options = list(width = 1600, height = 900))
plot(scurve1)

scurve2<- gvisMotionChart(sc2, "Code", "Date", options = list(width = 1000, height = 800))
plot(scurve2)

spc1<-spc[daysTillExp<366 & Date>"2008-06-01"]

spcurve1<- gvisMotionChart(spc1, "Code", "Date", options = list(width = 1600, height = 900))
plot(spcurve1)




# temp --------
y<-data[Date=="2015-08-03" ]
z<-sdata[sdata$Date=="2015-08-03", ]


plot(y$daysTillExp, y$Settle,  xlab="days till expiration", ylab="price", xlim=c(0,2500), ylim=c(20,80), yaxt="n",
     col=ifelse(y$Code=="B", "red", "blue"), pch=20)
axis(2, at=x<-seq(50,80,by=10),  las=2)
abline(v=seq(0,2500, by=100), col="grey", lty=3)
abline(h=seq(50,80, by=10), col="grey", lty=3)
points(z$daysTillExp, 4*z$spread, col="brown",pch=20)
axis(2, at=x<-seq(20,40,by=4), labels=x*0.25, las=2)
abline(h=x<-seq(20,40,by=4), col="grey", lty=3)

plot(sdata[sdata$Code=="U2015" & sdata$Date>"2015-01-01", sdata$Date], 
     sdata[sdata$Code=="U2015"  & sdata$Date>"2015-01-01", sdata$spread], 
     type="b", pch=20, col="red")

plot(sdata[Code=="J2015" & Date>"2015-01-01", Date], 
     sdata[Code=="J2015"  & Date>"2015-01-01", spread], 
     type="b", pch=20, col="red")

lines(sdata[Code=="K2015" & Date>"2015-01-01", Date], 
     sdata[Code=="K2015"  & Date>"2015-01-01", spread], 
     type="b", pch=20, col="blue")
lines(sdata[Code=="M2015" & Date>"2015-01-01", Date], 
      sdata[Code=="M2015"  & Date>"2015-01-01", spread], 
      type="b", pch=20, col="orange")


a<-data[Date>"2015-03-16" ]
plot(a$daysTillExp, a$Settle,  xlab="days till expiration", ylab="price", xlim=c(0,400), ylim=c(45,70), 
     col=as.factor(a$Date), pch=16)

legend(300, 51, unique(a$Date[a$Date>"2015-02-22"]),  pch=16, col = as.factor(a$Date))

legend(300, 51, unique(a$Date[a$Date>"2015-02-22"]),  fill=as.factor(a$Date))



par(mfrow=c(2,1))
par(mar=c(3,3,3,3))
# plot(y$Date,y$spread, type="l")
# plot(y$Date,y$B, type="l", col="blue")
# lines(y$Date,y$CL, col="red")
# abline(h=seq(50,110, by=10), col="grey", lty=3)


x<-data[Date=="2009-02-12" & Code=="CL",]
plot(x$Settle, xaxt="n")
axis(1, at=1:length(x$mCode), labels=x$mCode)




# notes -------
#' - type xts?
#' + 1 table for oil label
#' + load and save expired contracts, udate only non-expired
#' - may be make an automated detection of expired contracts (compare with current date)
#' + add exp date and days till exp 
#' - add generic contract (1,2,3 month)
#' - build generic CL-B spread (1,2,3 month)
#' 
#' Quandl("CHRIS/CME_CL1", type="xts")
#' Quandl("CHRIS/ICE_B1")