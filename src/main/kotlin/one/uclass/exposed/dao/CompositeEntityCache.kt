package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeEntityID
import one.uclass.exposed.dao.id.composite.CompositeIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transactionScope
import java.util.*

val Transaction.compositeEntityCache : CompositeEntityCache by transactionScope { CompositeEntityCache(this) }

@Suppress("UNCHECKED_CAST")
class CompositeEntityCache(private val transaction: Transaction) {
    private var flushingEntities = false
    val data = LinkedHashMap<CompositeIdTable<*, *>, MutableMap<Pair<Any, Any>, CompositeEntity<*, *>>>()
    val inserts = LinkedHashMap<CompositeIdTable<*, *>, MutableList<CompositeEntity<*, *>>>()
    val referrers = HashMap<CompositeEntityID<*, *>, MutableMap<Column<*>, SizedIterable<*>>>()

    private fun getMap(f: CompositeEntityClass<*, *, *>) : MutableMap<Pair<Any, Any>, CompositeEntity<*, *>> = getMap(f.table)

    private fun getMap(table: CompositeIdTable<*, *>) : MutableMap<Pair<Any, Any>, CompositeEntity<*, *>> = data.getOrPut(table) {
        LinkedHashMap()
    }

    fun <ClassifierID: Any, ID: Any, R: CompositeEntity<ClassifierID, ID>> getOrPutReferrers(sourceId: CompositeEntityID<*, *>, key: Column<*>, refs: ()-> SizedIterable<R>): SizedIterable<R> =
            referrers.getOrPut(sourceId){ HashMap() }.getOrPut(key) { LazySizedCollection(refs()) } as SizedIterable<R>

    fun <ClassifierID:Comparable<ClassifierID>, ID:Comparable<ID>, T: CompositeEntity<ClassifierID, ID>> find(f: CompositeEntityClass<ClassifierID, ID, T>, id: CompositeEntityID<ClassifierID, ID>): T? = getMap(f)[id.pair] as T? ?: inserts[f.table]?.firstOrNull { it.id == id } as? T

    fun <ClassifierID:Comparable<ClassifierID>, ID:Comparable<ID>, T: CompositeEntity<ClassifierID, ID>> findAll(f: CompositeEntityClass<ClassifierID, ID, T>): Collection<T> = getMap(f).values as Collection<T>

    fun <ClassifierID:Comparable<ClassifierID>, ID:Comparable<ID>, T: CompositeEntity<ClassifierID, ID>> store(f: CompositeEntityClass<ClassifierID, ID, T>, o: T) {
        getMap(f)[o.id.pair] = o
    }

    fun store(o: CompositeEntity<*, *>) {
        getMap(o.klass.table)[o.id.pair] = o
    }

    fun <ClassifierID:Comparable<ClassifierID>, ID:Comparable<ID>, T: CompositeEntity<ClassifierID, ID>> remove(table: CompositeIdTable<ClassifierID, ID>, o: T) {
        getMap(table).remove(o.id.pair)
    }

    fun <ClassifierID:Comparable<ClassifierID>, ID:Comparable<ID>, T: CompositeEntity<ClassifierID, ID>> scheduleInsert(f: CompositeEntityClass<ClassifierID, ID, T>, o: T) {
        inserts.getOrPut(f.table) { arrayListOf() }.add(o as CompositeEntity<*, *>)
    }

    fun flush() {
        flush(inserts.keys + data.keys)
    }

    private fun updateEntities(idTable: CompositeIdTable<*, *>) {
        data[idTable]?.let { map ->
            if (map.isNotEmpty()) {
                val updatedEntities = HashSet<CompositeEntity<*, *>>()
                val batch = CompositeEntityBatchUpdate(map.values.first().klass)
                for ((_, entity) in map) {
                    if (entity.flush(batch)) {
                        check(entity.klass !is ImmutableEntityClass<*, *, *>) { "Update on immutable entity ${entity.javaClass.simpleName} ${entity.id}" }
                        updatedEntities.add(entity)
                    }
                }
                batch.execute(transaction)
                updatedEntities.forEach {
                    transaction.registerChange(it.klass, it.id, EntityChangeType.Updated)
                }
            }
        }
    }

    fun flush(tables: Iterable<CompositeIdTable<*, *>>) {
        if (flushingEntities) return
        try {
            flushingEntities = true
            val insertedTables = inserts.keys

            val updateBeforeInsert = SchemaUtils.sortTablesByReferences(insertedTables).filterIsInstance<CompositeIdTable<*, *>>()
            updateBeforeInsert.forEach(::updateEntities)

            SchemaUtils.sortTablesByReferences(tables).filterIsInstance<CompositeIdTable<*, *>>().forEach(::flushInserts)

            val updateTheRestTables = tables - updateBeforeInsert
            for (t in updateTheRestTables) {
                updateEntities(t)
            }

            if (insertedTables.isNotEmpty()) {
                removeTablesReferrers(insertedTables)
            }
        } finally {
            flushingEntities = false
        }
    }

    internal fun removeTablesReferrers(insertedTables: Collection<Table>) {

        val insertedTablesSet = insertedTables.toSet()
        val tablesToRemove: List<Table> = referrers.values.flatMapTo(HashSet()) { it.keys.map { it.table } }
            .filter { table -> table.columns.any { c -> c.referee?.table in insertedTablesSet } } + insertedTablesSet

        referrers.mapNotNull { (entityId, entityReferrers) ->
            entityReferrers.filterKeys { it.table in tablesToRemove }.keys.forEach { entityReferrers.remove(it) }
            entityId.takeIf { entityReferrers.isEmpty() }
        }.forEach {
            referrers.remove(it)
        }
    }

    internal fun flushInserts(table: CompositeIdTable<*, *>) {
        inserts.remove(table)?.let {
            it.forEach { entry ->
                entry.writeValues[entry.klass.table.classifierId as Column<Any?>] = entry.id.classifierId
            }
            var toFlush: List<CompositeEntity<*, *>> = it
            do {
                val partition = toFlush.partition {
                    it.writeValues.none {
                        val (key, value) = it
                        key.referee == table.id && value is CompositeEntityID<*, *> && value._idPart._value == null
                    }
                }
                toFlush = partition.first
                val ids = table.batchInsert(toFlush) { entry ->
                    for ((c, v) in entry.writeValues) {
                        this[c] = v
                    }
                }

                for ((entry, genValues) in toFlush.zip(ids)) {
                    entry.writeValues[entry.klass.table.classifierId as Column<Any?>] = entry.id.classifierId
                    if (entry.id._idPart._value == null) {
                        val id = genValues[table.id]
                        entry.id._idPart._value = id._value
                        entry.writeValues[entry.klass.table.id as Column<Any?>] = id
                    }
                    genValues.fieldIndex.keys.forEach { key ->
                        entry.writeValues[key as Column<Any?>] = genValues[key]
                    }

                    entry.storeWrittenValues()
                    store(entry)
                    transaction.registerChange(entry.klass, entry.id, EntityChangeType.Created)
                }
                toFlush = partition.second
            } while(toFlush.isNotEmpty())
        }
    }

    fun clearReferrersCache() {
        referrers.clear()
    }

    companion object {

        fun invalidateGlobalCaches(created: List<CompositeEntity<*, *>>) {
            created.asSequence().mapNotNull { it.klass as? ImmutableCachedEntityClass<*, *, *> }.distinct().forEach {
                it.expireCache()
            }
        }
    }
}

fun Transaction.flushCache(): List<CompositeEntity<*, *>> {
    with(compositeEntityCache) {
        val newEntities = inserts.flatMap { it.value }
        flush()
        return newEntities
    }
}
