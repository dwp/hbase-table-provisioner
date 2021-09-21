package app.service.impl

import com.nhaarman.mockitokotlin2.*
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.io.compress.Compression
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import kotlin.time.ExperimentalTime

class HbaseTableCreatorServiceImplTest {

    @ExperimentalTime
    @Test
    fun shouldCreateHbaseTableForCollectionsWhenRequestingGivenTheyDoNotExist() = runBlocking {

        val tableName = "collection:name"
        val columnFamily = "cf"
        val regionReplicationCount = 1
        val splits = listOf<ByteArray>(ByteArray(2))

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
            on { isTableAvailable(expectedHbaseTableName) } doReturnConsecutively listOf(false, true)
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        val service = HbaseTableCreatorServiceImpl(connection, columnFamily, regionReplicationCount, 60)
        service.createHbaseTableFromProps(tableName, splits)

        verify(adm, times(1)).listTableNames()
        verify(adm, times(1)).listNamespaceDescriptors()
        val namespaceCaptor = argumentCaptor<NamespaceDescriptor>()
        verify(adm, times(1)).createNamespace(namespaceCaptor.capture())
        assertEquals("collection", namespaceCaptor.firstValue.name)
        verify(adm, times(1)).createTableAsync(expectedHbaseTable, splits.toTypedArray())
        verify(adm, times(2)).isTableAvailable(expectedHbaseTableName)
        verifyNoMoreInteractions(adm)
    }

}
