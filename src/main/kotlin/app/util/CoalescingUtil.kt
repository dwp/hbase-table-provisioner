package app.util

import uk.gov.dwp.dataworks.logging.DataworksLogger

class CoalescingUtil {

    companion object {
        private const val filenamePattern = """(?<database>[\w-]+)\.(?<collection>[\w-]+)"""
        val filenameRegex = Regex(filenamePattern, RegexOption.IGNORE_CASE)
        val COALESCED_COLLECTION = Regex("-(archived|eight|eighteen|eleven|fifteen|five|four|fourteen|nine|nineteen|one|seven|seventeen|six|sixteen|ten|thirteen|thirty|thirtyone|thirtytwo|three|twelve|twenty|twentyeight|twentyfive|twentyfour|twentynine|twentyone|twentyseven|twentysix|twentythree|twentytwo|two)$")
        private val coalescedNames = mapOf("agent_core:agentToDoArchive" to "agent_core:agentToDo")
        val logger = DataworksLogger.getLogger(CoalescingUtil::class.toString())
    }

    fun coalescedCollection(topicName: String): String {

        val matchResult = filenameRegex.find(topicName)
        val groups = matchResult!!.groups
        val database = groups[1]!!.value // can assert nun-null as it matched on the regex
        val uncoalescedCollection = groups[2]!!.value
        var collection = coalesced(uncoalescedCollection)

        var originalTableName = "$database:$collection".replace("-", "_")
        var tableName = coalescedArchive(originalTableName)

        if (originalTableName != tableName) {
            collection = tableName.replace(Regex("""^[^:]+:"""), "")
        }

        originalTableName = "$database:$collection".replace("-", "_")
        tableName = coalescedArchive(originalTableName)

        logger.info("Coalesced collection", "result" to tableName)

        return tableName
    }

    private fun coalesced(collection: String): String {
        val coalescedName = COALESCED_COLLECTION.replace(collection, "")
        if (collection != coalescedName) {
            logger.info("Using coalesced collection", "original_name" to collection, "coalesced_name" to coalescedName)
        }
        return coalescedName
    }

    private fun coalescedArchive(tableName: String) = if (coalescedNames[tableName] != null) coalescedNames[tableName]
            ?: "" else tableName
}
