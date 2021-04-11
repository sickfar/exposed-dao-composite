package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.ComplexEntityIDGenPartColumnType
import one.uclass.exposed.dao.id.composite.CompositeEntityIdPart
import one.uclass.exposed.dao.id.composite.CompositeIdTable
import org.jetbrains.exposed.sql.*

fun <T> Expression<T>.isNull(): IsNullOp = IsNullOp(this)

fun <T, S : T?> ExpressionWithColumnType<in S>.wrap(value: T): QueryParameter<T> = when (value) {
    is Boolean -> booleanParam(value)
    is Byte -> byteParam(value)
    is UByte -> ubyteParam(value)
    is Short -> shortParam(value)
    is UShort -> ushortParam(value)
    is Int -> intParam(value)
    is UInt -> uintParam(value)
    is Long -> longParam(value)
    is ULong -> ulongParam(value)
    is Float -> floatParam(value)
    is Double -> doubleParam(value)
    is String -> QueryParameter(value, columnType) // String value should inherit from column
    else -> QueryParameter(value, columnType)
} as QueryParameter<T>

infix fun <T : Comparable<T>, E : CompositeEntityIdPart<T>?> ExpressionWithColumnType<E>.eq(t: T?): Op<Boolean> {
    if (t == null) {
        return isNull()
    }
    @Suppress("UNCHECKED_CAST")
    val table = if (columnType is AutoIncColumnType) {
        ((columnType as AutoIncColumnType).delegate as ComplexEntityIDGenPartColumnType<*>).idColumn.table as CompositeIdTable<*, T>
    } else if (columnType is ComplexEntityIDGenPartColumnType<*>) {
        (columnType as ComplexEntityIDGenPartColumnType<*>).idColumn.table as CompositeIdTable<*, T>
    } else {
        throw IllegalStateException("Unsupported column type")
    }
    val entityID = CompositeEntityIdPart(t, table)
    return EqOp(this, wrap(entityID))
}
