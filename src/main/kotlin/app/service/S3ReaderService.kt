package app.service

interface S3ReaderService {
    fun getCollectionSummaries() : MutableMap<String, Long>
}
