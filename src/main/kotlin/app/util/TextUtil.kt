package app.util

fun getCollectionFromPath(path: String) = path.substringAfterLast('/').replace('-', ':')


// adb/2020-06-23/accepted-data.UpdateMongoLock_acceptedDataService.0001.json.gz.enc