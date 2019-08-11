package de.bogin.hexahdata.main

import groovy.sql.GroovyRowResult
import groovy.sql.Sql

class HexAHDataConvertToTimeSeries {

	static main(args) {
		
		def sql = Sql.newInstance("jdbc:mysql://localhost:3306/hexahdata", "root", "", "com.mysql.jdbc.Driver")
		
		List<Date> timeSeriesDates = sql.rows("select distinct date from time_series").date
		
		sql.eachRow("select distinct date from raw_data") { dateRow ->
			Date date = dateRow.date
			if (timeSeriesDates.contains(date)) {
				println "Date $date already found in table time_series!"
			} else {
			List<GroovyRowResult> result = sql.rows("select * from raw_data where date = $date")
				List itemNames = result.name.unique()
				itemNames.each { item ->
					List<GroovyRowResult> dateNameResult = result.findAll{it.name == item}
					dateNameResult.rarity.unique().each { rarity ->
						List currencies = dateNameResult.findAll{it.rarity == rarity}.collect{it.currency}.unique()
						currencies.each { currency ->
							List<GroovyRowResult> rawDataForEntry = dateNameResult.findAll{it.rarity == rarity && it.currency == currency}
							def prices = rawDataForEntry.collect{it.price}
							def minPrice = prices.min()
							def maxPrice = prices.max()
							def medianPrice = median(prices)
							sql.execute("insert into time_series (name, rarity, min_price, median_price, max_price, currency, trades, date) values ($item, $rarity, $minPrice, $medianPrice, $maxPrice, $currency, ${rawDataForEntry.size()}, $date)")
						}
					}
				}
				println "Converted data of $date to time series"
			}
		}
		println "Finished converting data!"
	}
	
	
	def static median(List values) {
		List sortedList = values.sort()
		int numberOfItems = values.size()
		int midNumber = (int)(numberOfItems/2)
		int median = numberOfItems %2 != 0 ? values[midNumber] : (values[midNumber] + values[midNumber-1])/2
	}
}
