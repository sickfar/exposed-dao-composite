package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeEntityID
import one.uclass.exposed.dao.id.composite.CompositeEntityIdPart
import one.uclass.exposed.dao.id.composite.CompositeIdTable
import one.uclass.exposed.dao.id.composite.LongLongIdTable
import org.jetbrains.exposed.sql.Column

enum class IdEnum {
    VALUE_ONE, VALUE_TWO
}

object BaseCompositeTable : LongLongIdTable("base_table", "const_id", "gen_id") {
    val data = text("some_data")
}

object ReferencedCompositeTable: LongLongIdTable("second_table", "const_id", "gen_id") {
    val data = text("some_data")
    val base = reference("base_ref", BaseCompositeTable.id)
}

object BackReferencedCompositeTable: LongLongIdTable("list_table", "const_id", "ref_gen_id") {
    init {
        id.references(BaseCompositeTable.id)
    }
    val value = varchar("value", 100)
}

object EnumTable: CompositeIdTable<Long, IdEnum>("enum_id_table") {
    override val classifierId: Column<Long> = long("const_id")
    override val id: Column<CompositeEntityIdPart<IdEnum>> = enumerationByName("id", 50, IdEnum::class).compositeIdPart()
    val value = varchar("value", 100)
    override val primaryKey by lazy { super.primaryKey ?: PrimaryKey(classifierId, id) }
}

class ListEntity(id: CompositeEntityID<Long, Long>): LongLongEntity(id) {
    companion object: LongLongEntityClass<ListEntity>(BackReferencedCompositeTable)
    var value by BackReferencedCompositeTable.value
}

class BaseEntity(id: CompositeEntityID<Long, Long>): LongLongEntity(id) {
    companion object: LongLongEntityClass<BaseEntity>(BaseCompositeTable)
    var data by BaseCompositeTable.data
    val list by ListEntity referrersOn BackReferencedCompositeTable.id
}

class ReferencedEntity(id: CompositeEntityID<Long, Long>): LongLongEntity(id) {
    companion object: LongLongEntityClass<ReferencedEntity>(ReferencedCompositeTable)
    var base by BaseEntity referencedOn ReferencedCompositeTable.base
    var data by ReferencedCompositeTable.data
}

class EnumEntity(id: CompositeEntityID<Long, IdEnum>): CompositeEntity<Long, IdEnum>(id) {
    companion object: CompositeEntityClass<Long, IdEnum, EnumEntity>(EnumTable)
    var value by EnumTable.value
}
