package app.service

interface TableProvisionerService {
    fun provisionHbaseTable(regionSize: Int, regionServers: Int) : Int
}
