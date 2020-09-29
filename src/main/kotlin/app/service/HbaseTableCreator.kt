package app.service

interface HbaseTableCreator {
    fun createHbaseTableFromProps(collectionName: String, regionCapacity: Int)
}
