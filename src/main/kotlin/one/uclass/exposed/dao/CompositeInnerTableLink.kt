package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeEntityIdPart
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.TransactionManager
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

@Suppress("UNCHECKED_CAST")
class CompositeInnerTableLink<ClassifierID:Comparable<ClassifierID>, SID:Comparable<SID>, Source: CompositeEntity<ClassifierID, SID>, ID:Comparable<ID>, Target: CompositeEntity<ClassifierID, ID>>(
    val table: Table,
    val target: CompositeEntityClass<ClassifierID, ID, Target>,
    val sourceColumn: Column<CompositeEntityIdPart<SID>>? = null,
    _targetColumn: Column<CompositeEntityIdPart<ID>>? = null) : ReadWriteProperty<Source, SizedIterable<Target>> {
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
            ?: table.columns.singleOrNull { it.referee == target.table.id } as? Column<CompositeEntityIdPart<ID>>
            ?: error("Table does not reference target")

    private fun getSourceRefColumn(o: Source): Column<CompositeEntityIdPart<SID>> {
        return sourceColumn ?: table.columns.singleOrNull { it.referee == o.klass.table.id } as? Column<CompositeEntityIdPart<SID>>
        ?: error("Table does not reference source")
    }

    override operator fun getValue(o: Source, unused: KProperty<*>): SizedIterable<Target> {
        if (o.id._idPart._value == null) return emptySized()
        val sourceRefColumn = getSourceRefColumn(o)
        val alreadyInJoin = (target.dependsOnTables as? Join)?.alreadyInJoin(table)?: false
        val entityTables = if (alreadyInJoin) target.dependsOnTables else target.dependsOnTables.join(table, JoinType.INNER, target.table.id, targetColumn) { target.table.classifierId eq o.id.classifierId }

        val columns = (target.dependsOnColumns + (if (!alreadyInJoin) table.columns else emptyList())
            - sourceRefColumn).distinct() + sourceRefColumn

        val query = {target.wrapRows(entityTables.slice(columns).select{o.klass.table.classifierId eq o.id.classifierId and (sourceRefColumn eq o.id.id)})}
        return TransactionManager.current().compositeEntityCache.getOrPutReferrers(o.id, sourceRefColumn, query)
    }

    override fun setValue(o: Source, unused: KProperty<*>, value: SizedIterable<Target>) {
        val sourceRefColumn = getSourceRefColumn(o)

        val tx = TransactionManager.current()
        val entityCache = tx.compositeEntityCache
        entityCache.flush()
        val oldValue = getValue(o, unused)
        val existinIDs = oldValue.map { it.id }.toSet()
        entityCache.referrers[o.id]?.remove(sourceRefColumn)

        val targetIds = value.map { it.id }
        table.deleteWhere { (sourceRefColumn eq o.id.id) and (targetColumn notInList targetIds.map { it._idPart }) }
        table.batchInsert(targetIds.filter { !existinIDs.contains(it) }, shouldReturnGeneratedValues = false) { targetId ->
            this[sourceRefColumn] = o.id._idPart
            this[targetColumn] = targetId._idPart
        }

        // current entity updated
        tx.registerChange(o.klass, o.id, EntityChangeType.Updated)

        // linked entities updated
        val targetClass = (value.firstOrNull() ?: oldValue.firstOrNull())?.klass
        if (targetClass != null) {
            existinIDs.plus(targetIds).forEach {
                tx.registerChange(targetClass, it, EntityChangeType.Updated)
            }
        }
    }
}
