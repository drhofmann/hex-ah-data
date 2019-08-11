package de.bogin.hexahdata.main

class HexAHDataDownloader {

	static main(args) {
		
		//download new index file
		String staticUrl = "http://dl.hex.gameforge.com/auctionhouse"
		String localPath = "X:/Dropbox/HexServerProject/AHData"
		new File ("$localPath/index.txt").delete()
		download(staticUrl,localPath, "index.txt")
		
		//download all missing files from index file
		File indexFile = new File("X:/Dropbox/HexServerProject/AHData/index.txt")
		
		indexFile.eachLine {entry ->
			if (entry =~(/Data-(\d{4})-(\d{2})-(\d{2})/)){
				if (new File("$localPath/processed/$entry").exists()) {
					println "File $entry has already been processed."
				} else {
					download(staticUrl, "$localPath/new", entry)
				}
			} else if (entry =~(/Data-Card-(\d{4})-(\d{2})-(\d{2})/)) {
				download(staticUrl, "$localPath/carddata", entry)
			} else if (entry =~(/Data-Item-(\d{4})-(\d{2})-(\d{2})/)) {
				download(staticUrl, "$localPath/itemdata", entry)
			}
		}

		println "Finished downloading stuff!"	
	}
	
	
	def static download (String remoteUrl, String path, String fileName){
		if (new File("$path/$fileName").exists()){
			println "File $path/$fileName already exists."
		} else {
			new File("$path/$fileName").withOutputStream { out ->
				new URL("$remoteUrl/$fileName").withInputStream { from -> out << from }
				println "Downloaded file $path/$fileName from $remoteUrl."
			}
		}
	}
}
