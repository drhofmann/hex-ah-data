package de.bogin.hexahdata.main

import groovy.sql.GroovyRowResult
import groovy.sql.Sql

class HexAHDataCalculateStatistics {

	static main(args) {
		
		int shortTimeDays = 10
		int mediumTimeDays = 60
		Date today = new Date()
		Date shortTimeBeginDate = today - shortTimeDays
		Date mediumTimeBeginDate = today - mediumTimeDays
		double shortTermMean
		double mediumTermMean
		
		def sql = Sql.newInstance("jdbc:mysql://localhost:3306/hexahdata", "root", "", "com.mysql.jdbc.Driver")
		
		String maxDate = sql.firstRow("select date from raw_data order by date desc").date

		def itemNames = sql.rows("select distinct name from time_series").name
		
		itemNames.each{itemName ->
			List<GroovyRowResult> timeSeries = sql.rows("select * from time_series where name = $itemName order by date asc")
			timeSeries.rarity.unique().each { rarity ->
				List currencies = timeSeries.findAll{it.rarity == rarity}.collect{it.currency}.unique()
				currencies.each { currency ->
					shortTermMean = null
					mediumTermMean = null
					List<Integer> timeSeriesOfMedians = timeSeries.findAll{it.rarity == rarity && it.currency == currency}.median_price
					double overallMean = timeSeriesOfMedians.sum()/timeSeriesOfMedians.size()
					
					List shortTermSequence = timeSeries.findAll{it.rarity == rarity && it.currency == currency && it.date >= shortTimeBeginDate}.median_price
					if (shortTermSequence.size()>0) {shortTermMean = shortTermSequence.sum() / shortTermSequence.size()}
					
					List mediumTermSequence =timeSeries.findAll{it.rarity == rarity && it.currency == currency && it.date >= mediumTimeBeginDate}.median_price
					if (mediumTermSequence.size()>0) {mediumTermMean = mediumTermSequence.sum()/mediumTermSequence.size()}
					sql.execute("insert into statistics (name, rarity, currency, overall_mean, medium_term_mean, short_term_mean, date) values ($itemName, $rarity, $currency, $overallMean, $mediumTermMean, $shortTermMean, $maxDate)")
				}
			}
		}
		println "Finished calculating statistics!"
	}
}
