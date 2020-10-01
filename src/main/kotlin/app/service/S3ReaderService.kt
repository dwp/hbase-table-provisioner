package app.service

import app.domain.CollectionSummary

interface S3ReaderService {
    fun getCollectionSummaries() : MutableMap<String, Long>
}
