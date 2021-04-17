package one.uclass.exposed.dao.id.composite

import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.vendors.OracleDialect
import org.jetbrains.exposed.sql.vendors.SQLiteDialect
import org.jetbrains.exposed.sql.vendors.currentDialect
import java.util.*

class CompositeEntityIdPartColumnType<T : Comparable<T>>(val idColumn: Column<T>) : ColumnType() {

    init {
        require(idColumn.table is CompositeIdTable<*, *>) { "CompositeIDGenPart supported only for CompositeIdTable" }
    }

    override fun sqlType(): String = idColumn.columnType.sqlType()

    override fun notNullValueToDB(value: Any): Any = idColumn.columnType.notNullValueToDB(
        when (value) {
            is CompositeEntityID<*, *> -> value.id
            is CompositeEntityIdPart<*> -> value.value
            else -> value
        }
    )

    override fun nonNullValueToString(value: Any): String = idColumn.columnType.nonNullValueToString(
        when (value) {
            is CompositeEntityID<*, *> -> value.id
            is CompositeEntityIdPart<*> -> value.value
            else -> value
        }
    )

    @Suppress("UNCHECKED_CAST")
    override fun valueFromDB(value: Any): CompositeEntityIdPart<T> = CompositeEntityIDFunctionProvider.createEntityID(
        when (value) {
            is CompositeEntityID<*, *> -> value.id as T
            is CompositeEntityIdPart<*> -> value.value as T
            else -> idColumn.columnType.valueFromDB(value) as T
        },
        idColumn.table as CompositeIdTable<*, T>
    )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as CompositeEntityIdPartColumnType<*>

        if (idColumn != other.idColumn) return false

        return true
    }

    override fun hashCode(): Int = 31 * super.hashCode() + idColumn.hashCode()
}

@Suppress("UNCHECKED_CAST")
@JvmName("inListCompositeIds")
infix fun <T : Comparable<T>> Column<CompositeEntityIdPart<T>>.inListComposite(list: Iterable<T>): InListOrNotInListOp<CompositeEntityIdPart<T>> {
    fun <T> ExpressionWithColumnType<T>.inList(list: Iterable<T>): InListOrNotInListOp<T> =
        InListOrNotInListOp(this, list, isInList = true)

    val idTable = (columnType as CompositeEntityIdPartColumnType<T>).idColumn.table as CompositeIdTable<*, T>
    return inList(list.map { CompositeEntityIDFunctionProvider.createEntityID(it, idTable) })
}

@Suppress("UNCHECKED_CAST")
@JvmName("inListOptCompositeIds")
infix fun <T : Comparable<T>> Column<CompositeEntityIdPart<T>?>.inListOptComposite(list: Iterable<T>): InListOrNotInListOp<CompositeEntityIdPart<T>?> {
    fun <T> ExpressionWithColumnType<T>.inList(list: Iterable<T>): InListOrNotInListOp<T> =
        InListOrNotInListOp(this, list, isInList = true)

    val idTable = (columnType as CompositeEntityIdPartColumnType<T>).idColumn.table as CompositeIdTable<*, T>
    return inList(list.map { CompositeEntityIDFunctionProvider.createEntityID(it, idTable) })
}

interface CompositeEntityIDFactory {
    fun <T : Comparable<T>> createEntityID(id: T, table: CompositeIdTable<*, T>): CompositeEntityIdPart<T>
}

object CompositeEntityIDFunctionProvider {
    private val factory: CompositeEntityIDFactory =
        ServiceLoader.load(CompositeEntityIDFactory::class.java, CompositeEntityIDFactory::class.java.classLoader)
            .firstOrNull()
            ?: object : CompositeEntityIDFactory {
                override fun <T : Comparable<T>> createEntityID(id: T, table: CompositeIdTable<*, T>) =
                    CompositeEntityIdPart(id, table)
            }

    @Suppress("UNCHECKED_CAST")
    fun <T : Comparable<T>> createEntityID(id: T, table: CompositeIdTable<*, T>) =
        factory.createEntityID(id, table)
}

private val ForeignKeyConstraint.foreignKeyPart: String
    get() = buildString {
        if (fkName.isNotBlank()) {
            append("CONSTRAINT $fkName ")
        }
        append("FOREIGN KEY ($fromColumn) REFERENCES $targetTable($targetColumn)")
        if (deleteRule != ReferenceOption.NO_ACTION) {
            append(" ON DELETE $deleteRule")
        }
        if (updateRule != ReferenceOption.NO_ACTION) {
            if (currentDialect is OracleDialect) {
                exposedLogger.warn("Oracle doesn't support FOREIGN KEY with ON UPDATE clause. Please check your $fromTable table.")
            } else {
                append(" ON UPDATE $updateRule")
            }
        }
    }

private fun Column<*>.isOneColumnPK(): Boolean = table.primaryKey?.columns?.singleOrNull() == this

/**
 * Base class for an identity table which could be referenced from another tables.
 *
 * @param name table name, by default name will be resolved from a class name with "Table" suffix removed (if present)
 */
abstract class CompositeIdTable<ClassifierID : Comparable<ClassifierID>, ID : Comparable<ID>>(name: String = "") :
    Table(name) {
    abstract val classifierId: Column<ClassifierID>
    abstract val id: Column<CompositeEntityIdPart<ID>>

    /** Converts the @receiver column to an [EntityID] column. */
    @Suppress("UNCHECKED_CAST")
    fun <T : Comparable<T>> Column<T>.compositeIdPart(): Column<CompositeEntityIdPart<T>> {
        val newColumn = Column<CompositeEntityIdPart<T>>(table, name, CompositeEntityIdPartColumnType(this)).also {
            it.indexInPK = indexInPK
            it.defaultValueFun = defaultValueFun?.let {
                {
                    CompositeEntityIDFunctionProvider.createEntityID(
                        it(),
                        table as CompositeIdTable<*, T>
                    )
                }
            }
        }
        return replaceColumn(this, newColumn)
    }

    @Suppress("UNCHECKED_CAST")
    val compositeIdExpression: BiCompositeColumn<ClassifierID, CompositeEntityIdPart<ID>, CompositeEntityID<ClassifierID, ID>>
        get() = object :
            BiCompositeColumn<ClassifierID, CompositeEntityIdPart<ID>, CompositeEntityID<ClassifierID, ID>>(
                classifierId,
                id,
                { Pair(it.classifierId, it._idPart) },
                { o1, o2 ->
                    CompositeEntityID(
                        o1 as ClassifierID,
                        o2?.let { id.columnType.valueFromDB(it) as CompositeEntityIdPart<ID> } ?: CompositeEntityIdPart(
                            null,
                            this@CompositeIdTable
                        ))
                }) {}

    private fun isCustomPKNameDefined(): Boolean = primaryKey?.let { it.name != "pk_$tableName" } == true

    private fun primaryKeyConstraint(): String? {
        return primaryKey?.let { primaryKey ->
            val tr = TransactionManager.current()
            val constraint = tr.db.identifierManager.cutIfNecessaryAndQuote(primaryKey.name)
            return primaryKey.columns.joinToString(
                prefix = "CONSTRAINT $constraint PRIMARY KEY (",
                postfix = ")",
                transform = tr::identity
            )
        }
    }

    override fun createStatement(): List<String> {
        val createSequence = autoIncColumn?.autoIncSeqName?.let { Sequence(it).createStatement() }.orEmpty()

        val addForeignKeysInAlterPart = SchemaUtils.checkCycle(this) && currentDialect !is SQLiteDialect

        val foreignKeyConstraints = columns.mapNotNull { it.foreignKey }.map {
            if (it.target.table is CompositeIdTable<*, *>) {
                val targetCompositeTable = it.target.table as CompositeIdTable<*, *>
                CompositeForeignKeyConstraint(
                    target = listOf(targetCompositeTable.classifierId, it.target),
                    from = listOf(classifierId, it.from),
                    onUpdate = it.updateRule,
                    onDelete = it.deleteRule,
                    null
                )
            } else {
                it
            }
        }

        val createTable = buildString {
            append("CREATE TABLE ")
            if (currentDialect.supportsIfNotExists) {
                append("IF NOT EXISTS ")
            }
            append(TransactionManager.current().identity(this@CompositeIdTable))
            if (columns.isNotEmpty()) {
                columns.joinTo(this, prefix = " (") { it.descriptionDdl() }

                if (isCustomPKNameDefined() || columns.none { it.isOneColumnPK() }) {
                    primaryKeyConstraint()?.let { append(", $it") }
                }

                if (!addForeignKeysInAlterPart && foreignKeyConstraints.isNotEmpty()) {
                    foreignKeyConstraints.joinTo(this, prefix = ", ", separator = ", ") {
                        when (it) {
                            is ForeignKeyConstraint -> it.foreignKeyPart
                            is CompositeForeignKeyConstraint -> it.foreignKeyPart
                            else -> ""
                        }
                    }
                }

                append(")")
            }
        }

        val createConstraint = if (addForeignKeysInAlterPart) {
            foreignKeyConstraints.flatMap { it.createStatement() }
        } else {
            emptyList()
        }

        return createSequence + createTable + createConstraint
    }
}

/**
 * Identity table with autoincrement integer primary key
 *
 * @param name table name, by default name will be resolved from a class name with "Table" suffix removed (if present)
 * @param columnName name for a primary key, "id" by default
 */
open class IntIntIdTable(name: String = "", primaryColumnName: String, secondaryColumnName: String) :
    CompositeIdTable<Int, Int>(name) {
    override val classifierId: Column<Int> = integer(primaryColumnName)
    override val id: Column<CompositeEntityIdPart<Int>> =
        integer(secondaryColumnName).compositeIdPart().autoIncrement()
    override val primaryKey by lazy { super.primaryKey ?: PrimaryKey(classifierId, id) }
}

/**
 * Identity table with autoincrement long primary key
 *
 * @param name table name, by default name will be resolved from a class name with "Table" suffix removed (if present)
 * @param columnName name for a primary key, "id" by default
 */
open class LongLongIdTable(name: String = "", primaryColumnName: String, secondaryColumnName: String) :
    CompositeIdTable<Long, Long>(name) {
    override val classifierId: Column<Long> = long(primaryColumnName)//.entityId()
    override val id: Column<CompositeEntityIdPart<Long>> =
        long(secondaryColumnName).compositeIdPart().autoIncrement()
    override val primaryKey by lazy { super.primaryKey ?: PrimaryKey(classifierId, id) }
}
