package app.service

interface HbaseTableCreatorService {
    suspend fun createHbaseTableFromProps(collectionName: String, regionCapacity: Int, splits: List<ByteArray>)
}
