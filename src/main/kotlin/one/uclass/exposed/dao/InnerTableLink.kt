package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeEntityID
import one.uclass.exposed.dao.id.composite.CompositeIDGenPart
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.TransactionManager
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

@Suppress("UNCHECKED_CAST")
class InnerTableLink<CID:Comparable<CID>, SID:Comparable<SID>, Source: CompositeEntity<CID, SID>, ID:Comparable<ID>, Target: CompositeEntity<CID, ID>>(
    val table: Table,
    val target: CompositeEntityClass<CID, ID, Target>,
    val sourceColumn: Column<CompositeIDGenPart<SID>>? = null,
    _targetColumn: Column<CompositeIDGenPart<ID>>? = null) : ReadWriteProperty<Source, SizedIterable<Target>> {
    init {
        _targetColumn?.let {
            requireNotNull(sourceColumn) { "Both source and target columns should be specified"}
            require(_targetColumn.referee?.table == target.table) {
                "Column $_targetColumn point to wrong table, expected ${target.table.tableName}"
            }
            require(_targetColumn.table == sourceColumn.table) {
                "Both source and target columns should be from the same table"
            }
        }
        sourceColumn?.let {
            requireNotNull(_targetColumn) { "Both source and target columns should be specified"}
        }
    }

    private val targetColumn = _targetColumn
            ?: table.columns.singleOrNull { it.referee == target.table.genId } as? Column<CompositeIDGenPart<ID>>
            ?: error("Table does not reference target")

    private fun getSourceRefColumn(o: Source): Column<CompositeIDGenPart<SID>> {
        return sourceColumn ?: table.columns.singleOrNull { it.referee == o.klass.table.genId } as? Column<CompositeIDGenPart<SID>>
        ?: error("Table does not reference source")
    }

    override operator fun getValue(o: Source, unused: KProperty<*>): SizedIterable<Target> {
        if (o.id._genId._value == null) return emptySized()
        val sourceRefColumn = getSourceRefColumn(o)
        val alreadyInJoin = (target.dependsOnTables as? Join)?.alreadyInJoin(table)?: false
        val entityTables = if (alreadyInJoin) target.dependsOnTables else target.dependsOnTables.join(table, JoinType.INNER, target.table.genId, targetColumn) { target.table.constId eq o.id.constId }

        val columns = (target.dependsOnColumns + (if (!alreadyInJoin) table.columns else emptyList())
            - sourceRefColumn).distinct() + sourceRefColumn

        val query = {target.wrapRows(entityTables.slice(columns).select{o.klass.table.constId eq o.id.constId and (sourceRefColumn eq o.id.genId)})}
        return TransactionManager.current().entityCache.getOrPutReferrers(o.id, sourceRefColumn, query)
    }

    override fun setValue(o: Source, unused: KProperty<*>, value: SizedIterable<Target>) {
        val sourceRefColumn = getSourceRefColumn(o)

        val tx = TransactionManager.current()
        val entityCache = tx.entityCache
        entityCache.flush()
        val oldValue = getValue(o, unused)
        val existingIds = oldValue.map { it.id }.toSet()
        entityCache.referrers[o.id]?.remove(sourceRefColumn)

        val targetIds = value.map { it.id }
        table.deleteWhere { (sourceRefColumn eq o.id.genId) and (targetColumn notInList targetIds.map { it._genId }) }
        table.batchInsert(targetIds.filter { !existingIds.contains(it) }, shouldReturnGeneratedValues = false) { targetId ->
            this[sourceRefColumn] = o.id._genId
            this[targetColumn] = targetId._genId
        }

        // current entity updated
        tx.registerChange(o.klass, o.id, EntityChangeType.Updated)

        // linked entities updated
        val targetClass = (value.firstOrNull() ?: oldValue.firstOrNull())?.klass
        if (targetClass != null) {
            existingIds.plus(targetIds).forEach {
                tx.registerChange(targetClass, it, EntityChangeType.Updated)
            }
        }
    }
}
