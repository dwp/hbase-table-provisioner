package app.service

interface HbaseTableCreator {
    fun createHbaseTableFromProps(collectionName: String, regionSize: Int)
}