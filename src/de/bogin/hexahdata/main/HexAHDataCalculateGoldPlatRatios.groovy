package de.bogin.hexahdata.main

import groovy.sql.GroovyRowResult
import groovy.sql.Sql

class HexAHDataCalculateGoldPlatRatios {

	
	static main(args) {
		double ratio
		
		def sql = Sql.newInstance("jdbc:mysql://localhost:3306/hexahdata", "root", "", "com.mysql.jdbc.Driver")
		
		List<Date> goldPlatRatioDates = sql.rows("select distinct date from gold_plat_ratio").date
		
		sql.eachRow("select distinct date from raw_data") { dateRow ->
			Date date = dateRow.date
			if (goldPlatRatioDates.contains(date)) {
				println "Date $date already found in table gold_plat_ratio!"
			} else {
			List<GroovyRowResult> result = sql.rows("select * from raw_data where date = $date")
				List itemNames = result.name.unique()
				itemNames.each { item ->
					List<GroovyRowResult> dateNameResult = result.findAll{it.name == item}
					dateNameResult.rarity.unique().each { rarity ->
							ratio = null
							List<GroovyRowResult> goldDataForEntry = dateNameResult.findAll{it.rarity == rarity && it.currency == 'GOLD'}.price
							List<GroovyRowResult> platDataForEntry = dateNameResult.findAll{it.rarity == rarity && it.currency == 'PLATINUM'}.price
							if (goldDataForEntry.size>0 && platDataForEntry.size()>0) {
								ratio = median(goldDataForEntry) / median(platDataForEntry)
								sql.execute("insert into gold_plat_ratio (name, rarity, median_ratio, date) values ($item, $rarity, $ratio, $date)")
							}
					}
				}
				ratio = null
				List<GroovyRowResult> ratios = sql.rows("select * from gold_plat_ratio where date = $date").median_ratio
				ratio = median (ratios)
				sql.execute("insert into gold_plat_ratio (name, rarity, median_ratio, date) values ('CURRENCY', '0', $ratio, $date)")
				println "Converted data of $date to gold plat ratio series"
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
