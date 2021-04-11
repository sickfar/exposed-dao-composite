package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeIdTable
import org.jetbrains.exposed.sql.Query
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.statements.*
import org.jetbrains.exposed.sql.targetTables

class CompositeEntityLifecycleInterceptor : GlobalStatementInterceptor {

    override fun beforeExecution(transaction: Transaction, context: StatementContext) {
        when (val statement = context.statement) {
            is Query -> transaction.flushEntities(statement)

            is DeleteStatement -> {
                transaction.flushCache()
                transaction.compositeEntityCache.removeTablesReferrers(listOf(statement.table))
            }

            is InsertStatement<*> -> {
                transaction.flushCache()
                transaction.compositeEntityCache.removeTablesReferrers(listOf(statement.table))
            }

            is CompositeBatchUpdateStatement -> {}

            is UpdateStatement -> {
                transaction.flushCache()
                transaction.compositeEntityCache.removeTablesReferrers(statement.targetsSet.targetTables())
            }

            else -> {
                if(statement.type.group == StatementGroup.DDL)
                    transaction.flushCache()
            }
        }
    }

    override fun beforeCommit(transaction: Transaction) {
        val created = transaction.flushCache()
        transaction.alertSubscribers()
        val createdByHooks = transaction.flushCache()
        CompositeEntityCache.invalidateGlobalCaches(created + createdByHooks)
    }

    override fun beforeRollback(transaction: Transaction) {
        val entityCache = transaction.compositeEntityCache
        entityCache.clearReferrersCache()
        entityCache.data.clear()
        entityCache.inserts.clear()
    }

    private fun Transaction.flushEntities(query: Query) {
        // Flush data before executing query or results may be unpredictable
        val tables = query.targets.filterIsInstance(CompositeIdTable::class.java).toSet()
        compositeEntityCache.flush(tables)
    }
}
