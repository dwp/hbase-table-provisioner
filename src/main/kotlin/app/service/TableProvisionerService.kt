package app.service

interface TableProvisionerService {
    fun provisionHbaseTable(a: Int, b: Int) : Int
}