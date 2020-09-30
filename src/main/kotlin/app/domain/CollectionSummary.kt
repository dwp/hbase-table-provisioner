package app.domain

import com.amazonaws.services.s3.model.S3ObjectSummary

data class CollectionSummary(
        val collectionName: String,
        val size: Int
)

data class S3ObjectSummaryPair(val data: S3ObjectSummary?)

data class KeyPair(val dataKey: String?)