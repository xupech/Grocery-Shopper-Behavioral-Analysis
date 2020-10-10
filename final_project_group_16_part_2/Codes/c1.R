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
dta_at_TC$year <- year(dta_at_TC$TC_DATE)
dta_at_TC <- dta_at_TC[year == '2004']                          # Remove data in December 2003

month_trip_times <- count(dta_at_TC, months)

new_table <- merge(x=dta_at_TC, y=dta_at_TC_upc, by = 'TC_id')  # Combine two tables according to 'TC_id'

month_prod_buy <- aggregate(new_table$quantity_at_TC_prod_id, by=list(months = new_table$months), FUN=sum)

prod_and_trip <- merge(x=month_trip_times, y=month_prod_buy, by='months')

colnames(prod_and_trip) <- c('Month', 'Time_of_Trips', 'Products_Bought')

ggplot(prod_and_trip, aes(x = Time_of_Trips, y = Products_Bought, color = Month)) +
  geom_point() 






