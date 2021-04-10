package one.uclass.exposed.dao.id.composite

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.vendors.OracleDialect
import org.jetbrains.exposed.sql.vendors.SQLiteDialect
import org.jetbrains.exposed.sql.vendors.currentDialect
import java.util.*

class ComplexEntityIDGenPartColumnType<T : Comparable<T>>(val genIdColumn: Column<T>) : ColumnType() {

    init {
        require(genIdColumn.table is CompositeIdTable<*, *>) { "CompositeIDGenPart supported only for CompositeIdTable" }
    }

    override fun sqlType(): String = genIdColumn.columnType.sqlType()

    override fun notNullValueToDB(value: Any): Any = genIdColumn.columnType.notNullValueToDB(
        when (value) {
            is CompositeIDGenPart<*> -> value.value
            else -> value
        }
    )

    override fun nonNullValueToString(value: Any): String = genIdColumn.columnType.nonNullValueToString(
        when (value) {
            is CompositeIDGenPart<*> -> value.value
            else -> value
        }
    )

    @Suppress("UNCHECKED_CAST")
    override fun valueFromDB(value: Any): CompositeIDGenPart<T> = CompositeEntityIDFunctionProvider.createEntityID(
        when (value) {
            is CompositeIDGenPart<*> -> value.value as T
            else -> genIdColumn.columnType.valueFromDB(value) as T
        },
        genIdColumn.table as CompositeIdTable<*, T>
    )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ComplexEntityIDGenPartColumnType<*>

        if (genIdColumn != other.genIdColumn) return false

        return true
    }

    override fun hashCode(): Int = 31 * super.hashCode() + genIdColumn.hashCode()
}

interface CompositeEntityIDFactory {
    fun <T : Comparable<T>> createEntityID(genId: T, table: CompositeIdTable<*, T>): CompositeIDGenPart<T>
}

object CompositeEntityIDFunctionProvider {
    private val factory: CompositeEntityIDFactory =
        ServiceLoader.load(CompositeEntityIDFactory::class.java, CompositeEntityIDFactory::class.java.classLoader)
            .firstOrNull()
            ?: object : CompositeEntityIDFactory {
                override fun <T : Comparable<T>> createEntityID(genId: T, table: CompositeIdTable<*, T>) =
                    CompositeIDGenPart(genId, table)
            }

    @Suppress("UNCHECKED_CAST")
    fun <T : Comparable<T>> createEntityID(genId: T, table: CompositeIdTable<*, T>) =
        factory.createEntityID(genId, table)
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
abstract class CompositeIdTable<Const : Comparable<Const>, Gen : Comparable<Gen>>(name: String = "") : Table(name) {
    abstract val constId: Column<Const>
    abstract val genId: Column<CompositeIDGenPart<Gen>>

    /** Converts the @receiver column to an [EntityID] column. */
    @Suppress("UNCHECKED_CAST")
    fun <T : Comparable<T>> Column<T>.compositeIdGenComponent(): Column<CompositeIDGenPart<T>> {
        val newColumn = Column<CompositeIDGenPart<T>>(table, name, ComplexEntityIDGenPartColumnType(this)).also {
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
    val compositeIdColumnsExpression: BiCompositeColumn<Const, CompositeIDGenPart<Gen>, CompositeEntityID<Const, Gen>>
        get() = object :
            BiCompositeColumn<Const, CompositeIDGenPart<Gen>, CompositeEntityID<Const, Gen>>(
                constId,
                genId,
                { Pair(it.constId, it._genId) },
                { o1, o2 -> CompositeEntityID(o1 as Const, CompositeIDGenPart(o2 as Gen, this@CompositeIdTable)) }) {}

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
                    target = listOf(targetCompositeTable.constId, it.target),
                    from = listOf(constId, it.from),
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
    override val constId: Column<Int> = integer(primaryColumnName)
    override val genId: Column<CompositeIDGenPart<Int>> =
        integer(secondaryColumnName).compositeIdGenComponent().autoIncrement()
    override val primaryKey by lazy { super.primaryKey ?: PrimaryKey(constId, genId) }
}

/**
 * Identity table with autoincrement long primary key
 *
 * @param name table name, by default name will be resolved from a class name with "Table" suffix removed (if present)
 * @param columnName name for a primary key, "id" by default
 */
open class LongLongIdTable(name: String = "", primaryColumnName: String, secondaryColumnName: String) :
    CompositeIdTable<Long, Long>(name) {
    override val constId: Column<Long> = long(primaryColumnName)//.entityId()
    override val genId: Column<CompositeIDGenPart<Long>> =
        long(secondaryColumnName).compositeIdGenComponent().autoIncrement()
    override val primaryKey by lazy { super.primaryKey ?: PrimaryKey(constId, genId) }
}
