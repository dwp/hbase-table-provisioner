package app.util

fun topicNameTableMatcher(collectionName: String) = collectionName.split('.')

fun convertCollectionNameToHbaseName(collectionName: String) = collectionName.replace('.', ':')
