package one.uclass.exposed.dao.id.composite

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.vendors.*

internal val currentDialectIfAvailable: DatabaseDialect?
    get() = if (TransactionManager.isInitialized() && TransactionManager.currentOrNull() != null) {
        currentDialect
    } else {
        null
    }

fun tableNameWithoutScheme(tableName: String) = tableName.substringAfter(".")

fun String.inProperCase(): String =
    TransactionManager.currentOrNull()?.db?.identifierManager?.inProperCase(this@inProperCase) ?: this

data class CompositeForeignKeyConstraint(
    val target: List<Column<*>>,
    val from: List<Column<*>>,
    private val onUpdate: ReferenceOption?,
    private val onDelete: ReferenceOption?,
    private val name: String?
) : DdlAware {
    private val tx: Transaction
        get() = TransactionManager.current()
    /** Name of the child table. */
    val targetTable: String
        get() = tx.identity(target.first().table)
    /** Name of the foreign key column. */
    val targetColumns: List<String>
        get() = target.map { tx.identity(it) }
    /** Name of the parent table. */
    val fromTable: String
        get() = tx.identity(from.first().table)
    /** Name of the key column from the parent table. */
    val fromColumns: List<String>
        get() = from.map { tx.identity(it) }
    /** Reference option when performing update operations. */
    val updateRule: ReferenceOption?
        get() = onUpdate ?: currentDialectIfAvailable?.defaultReferenceOption
    /** Reference option when performing delete operations. */
    val deleteRule: ReferenceOption?
        get() = onDelete ?: currentDialectIfAvailable?.defaultReferenceOption
    /** Name of this constraint. */
    val fkName: String
        get() = tx.db.identifierManager.cutIfNecessaryAndQuote(
            name ?: "fk_${tableNameWithoutScheme(from.first().table.tableName)}_${from.joinToString("_") {it.name}}_${target.joinToString("_") {it.name}}"
        ).inProperCase()
    val foreignKeyPart: String get() = buildString {
        if (fkName.isNotBlank()) {
            append("CONSTRAINT $fkName ")
        }
        append("FOREIGN KEY (${fromColumns.joinToString(", ")}) REFERENCES $targetTable(${targetColumns.joinToString(", ")})")
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

    override fun createStatement(): List<String> = listOf("ALTER TABLE $fromTable ADD $foreignKeyPart")

    override fun modifyStatement(): List<String> = dropStatement() + createStatement()

    override fun dropStatement(): List<String> {
        val constraintType = when (currentDialect) {
            is MysqlDialect -> "FOREIGN KEY"
            else -> "CONSTRAINT"
        }
        return listOf("ALTER TABLE $fromTable DROP $constraintType $fkName")
    }
}
