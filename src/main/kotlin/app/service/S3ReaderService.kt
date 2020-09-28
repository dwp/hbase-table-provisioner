package app.service

interface S3ReaderService {
    fun getCollections() : List<String>
}
