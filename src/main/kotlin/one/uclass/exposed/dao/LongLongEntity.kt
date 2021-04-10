package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeEntityID
import one.uclass.exposed.dao.id.composite.CompositeIdTable

abstract class LongLongEntity(id: CompositeEntityID<Long, Long>) : CompositeEntity<Long, Long>(id)

abstract class LongLongEntityClass<out E: LongLongEntity>(table: CompositeIdTable<Long, Long>, entityType: Class<E>? = null) : CompositeEntityClass<Long, Long, E>(table, entityType)

