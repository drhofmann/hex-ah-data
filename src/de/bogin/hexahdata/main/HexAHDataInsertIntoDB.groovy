package de.bogin.hexahdata.main

import groovy.sql.Sql
import com.mysql.jdbc.Driver

class HexAHDataInsertIntoDB {

	static main(args) {

		def sql = Sql.newInstance("jdbc:mysql://localhost:3306/hexahdata", "root", "", "com.mysql.jdbc.Driver")

		String basePath = "X:/Dropbox/HexServerProject/AHData"
		String inputDirName = "new"
		String processedDirName = "processed"

		File inputDir = new File("$basePath/$inputDirName")
		inputDir.eachFileMatch ~/.*\.csv/, { file ->
			String date = file.name.find(/(\d{4})-(\d{2})-(\d{2})/)
			if (sql.rows("select * from raw_data where date = $date").size() > 0) {
				println "Date $date already found in table raw_data!"
				file.renameTo("$basePath/$processedDirName/$file.name")
			} else {
				File exactFile = new File("$inputDir/$file.name")
				println "Inserting raw AH data for $date."
				file.splitEachLine(",") {fields ->
					fields = fields*.trim()
					sql.execute("insert into raw_data (name, rarity, currency, price, date) values (${fields[0]}, ${fields[1]}, ${fields[2]}, ${fields[3]}, ${fields[4]})")
				}
				file.renameTo("$basePath/$processedDirName/$file.name")
			}
		}
		println "Finished inserting stuff!"
	}
}
