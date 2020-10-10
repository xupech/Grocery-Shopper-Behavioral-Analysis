library(data.table)
library(dplyr)
library(ggplot2)
dta_at_hh <- fread("~/Desktop/BIG DATA FINAL PROJECT/dta_at_hh.csv")
head(dta_at_hh)

dta_at_prod_id <- fread("~/Desktop/BIG DATA FINAL PROJECT/dta_at_prod_id.csv")
head(dta_at_prod_id)

dta_at_TC_upc <- fread("~/Desktop/BIG DATA FINAL PROJECT/dta_at_TC_upc.csv")
head(dta_at_TC_upc)

dta_at_TC <- fread("~/Desktop/BIG DATA FINAL PROJECT/dta_at_TC.csv")
head(dta_at_TC)


#Is the number of shopping trips per month correlated with the average number of items purchased?

dta_at_TC$TC_DATE <- as.Date(dta_at_TC$TC_date)

dta_at_TC$months <- months(dta_at_TC$TC_DATE)

month_trip_times <- count(dta_at_TC, months)

new_table <- merge(x=dta_at_TC, y=dta_at_TC_upc, by = 'TC_id')
head(new_table)
new_table2<-merge(new_table,dta_at_prod_id,by='prod_id')
head(new_table2)
### total expenditure per month
month_prod_buy <- aggregate(new_table$total_price_paid_at_TC_prod_id, by=list(months = new_table$months), FUN=sum)

head(new_table2)
### CTL BR monthly expenditure
new_table3<- new_table2[new_table2$brand_at_prod_id=='CTL BR']
head(new_table3)
month_prod_buy2<-aggregate(new_table3$total_price_paid_at_TC_prod_id, by=list(months=new_table3$months),FUN=sum)
month_prod_buy2 ## this is monthly expenditure of CTL_BR products

### Use Monthly expenditure of CTL_BR products/ Total monthly expenditure of all products
### We get expenditure share(percentage) of CTL_BR products
month_prod_buy['percent']=month_prod_buy2$x/month_prod_buy$x*100
month_prod_buy
monthly_expenditure=month_prod_buy[,c(1,3)]
monthly_expenditure$percent=round(monthly_expenditure$percent,2)
monthly_expenditure$months=c(1:12)
monthly_expenditure
ggplot(monthly_expenditure, aes(x = monthly_expenditure$months, y = monthly_expenditure$percent))+
  geom_point(size=1.5)+geom_line(size = 0.4)+
  geom_text(aes(label=monthly_expenditure$percent), vjust=-0.2)
### graph shows that expenditure share of CTL_BR products fluctuates through time and doenst stay constant




