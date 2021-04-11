package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeEntityID
import one.uclass.exposed.dao.id.composite.LongLongIdTable

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
