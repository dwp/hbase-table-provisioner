package app.service

interface HbaseTableCreatorService {
    fun createHbaseTableFromProps(collectionName: String, regionCapacity: Int, splits: List<ByteArray>)
}
