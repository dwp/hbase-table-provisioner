package app.service

interface HbaseTableCreatorService {
    suspend fun createHbaseTableFromProps(collectionName: String, splits: List<ByteArray>)
}
