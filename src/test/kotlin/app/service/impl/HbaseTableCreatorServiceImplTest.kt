package app.service.impl

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.compress.Compression
import org.junit.jupiter.api.Test

class HbaseTableCreatorServiceImplTest {

    @Test
    fun shouldCreateHbaseTableForCollectionsWhenRequestingGivenTheyDoNotExist() {

        val tableName = "collection:name"
        val columnFamily = "cf"
        val regionReplicationCount = 1
        val splits = listOf<ByteArray>()

        val regionCapacity = 1

        val expectedHbaseTableName = TableName.valueOf(tableName)

        val expectedHbaseTable = HTableDescriptor(expectedHbaseTableName).apply {
            addFamily(HColumnDescriptor(columnFamily)
                    .apply {
                        maxVersions = Int.MAX_VALUE
                        minVersions = 1
                        compressionType = Compression.Algorithm.GZ
                        compactionCompressionType = Compression.Algorithm.GZ
                    })
            regionReplication = regionReplicationCount
        }

        val adm  = mock<Admin> {
            on { listTableNames() } doReturn arrayOf()
            on { listNamespaceDescriptors() } doReturn arrayOf(NamespaceDescriptor.DEFAULT_NAMESPACE)
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        val service = HbaseTableCreatorServiceImpl(connection, columnFamily, regionReplicationCount)

        service.createHbaseTableFromProps(tableName, regionCapacity, splits)

        verify(adm, times(1)).listTableNames()
        verify(adm, times(1)).listNamespaceDescriptors()

        verify(adm, times(1)).createTable(expectedHbaseTable, splits.toTypedArray())
    }
}
