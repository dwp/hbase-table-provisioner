//package app.service.impl
//
//import app.domain.CollectionSummary
//import com.nhaarman.mockitokotlin2.*
//import org.junit.jupiter.api.Test
//
//class TableProvisionerServiceImplTest {
//
//    @Test
//    fun shouldProvisionHbaseTablesWhenRequestedGivenCollectionsExist() {
//
//        val collectionSummaries = mockCollectionSummaries()
//
//        val s3ReaderServiceMock = mock<S3ReaderServiceImpl> {
//            on {
//                getCollectionSummaries()
//            } doReturn collectionSummaries
//        }
//
//        val hbaseTableCreatorMock = mock<HbaseTableCreatorServiceImpl>()
//
//        val regionTargetSize = 1
//        val regionServerCount = 3
//
//        val service = TableProvisionerServiceImpl(s3ReaderServiceMock, hbaseTableCreatorMock, regionTargetSize, regionServerCount)
//
//        service.provisionHbaseTable()
//
//        verify(s3ReaderServiceMock, times(1)).getCollectionSummaries()
//        verify(hbaseTableCreatorMock, times(2)).createHbaseTableFromProps(any(), any())
//    }
//
//    @Test
//    fun shouldNotProvisionedHbaseTablesWhenRequestedGivenNoCollectionsExist() {
//
//        val s3ReaderServiceMock = mock<S3ReaderServiceImpl> {
//            on {
//                getCollectionSummaries()
//            } doReturn mutableListOf()
//        }
//
//        val hbaseTableCreatorMock = mock<HbaseTableCreatorServiceImpl>()
//
//        val regionTargetSize = 1
//        val regionServerCount = 3
//
//        val service = TableProvisionerServiceImpl(s3ReaderServiceMock, hbaseTableCreatorMock, regionTargetSize, regionServerCount)
//
//        service.provisionHbaseTable()
//
//        verify(s3ReaderServiceMock, times(1)).getCollectionSummaries()
//        verifyZeroInteractions(hbaseTableCreatorMock)
//    }
//
//    private fun mockCollectionSummaries(): MutableList<CollectionSummary> =
//            mutableListOf(CollectionSummary("collection_1", 100), CollectionSummary("collection_2", 100))
//}
