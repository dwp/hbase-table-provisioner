//package app.service.impl
//
//import com.nhaarman.mockitokotlin2.argumentCaptor
//import com.nhaarman.mockitokotlin2.doReturn
//import com.nhaarman.mockitokotlin2.mock
//import org.apache.hadoop.hbase.NamespaceDescriptor
//import org.apache.hadoop.hbase.client.Admin
//import org.apache.hadoop.hbase.client.Connection
//import org.apache.hadoop.hbase.client.Get
//import org.apache.hadoop.hbase.client.Table
//import org.junit.jupiter.api.Test
//
//class HbaseTableCreatorServiceImplTest {
//
//    @Test
//    fun shouldCreateHbaseTableForCollectionsWhenRequestingGivenTheyDoNotExist() {
//
//        val tableName = "collection:name"
//        val columnFamily = "cf"
//        val regionReplicationCount = 1
//
//        val regionCapacity = 1
//
//        val adm  = mock<Admin> {
//            on { listTableNames() } doReturn arrayOf()
//            on { listNamespaceDescriptors() } doReturn arrayOf(NamespaceDescriptor.DEFAULT_NAMESPACE)
//        }
//
//        val existsArray = (1..100).map {it % 2 == 0}.toBooleanArray()
//
//        val getCaptor = argumentCaptor<List<Get>>()
//        val table = mock<Table> {
////            on { listTableNames(getCaptor.capture()) } doReturn existsArray
//        }
//
//        val connection = mock<Connection> {
//            on { admin } doReturn adm
////            on { getTable(TableName.valueOf(tableName)) } doReturn table
//        }
//
//        val service = HbaseTableCreatorServiceImpl(connection, columnFamily, regionReplicationCount)
//
//        service.createHbaseTableFromProps(tableName, regionCapacity)
//
//    }
//}
