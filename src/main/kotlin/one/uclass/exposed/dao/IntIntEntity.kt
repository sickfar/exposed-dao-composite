package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeEntityID
import one.uclass.exposed.dao.id.composite.CompositeIdTable

abstract class IntIntEntity(id: CompositeEntityID<Int, Int>) : CompositeEntity<Int, Int>(id)

abstract class IntIntEntityClass<out E: IntIntEntity>(table: CompositeIdTable<Int, Int>, entityType: Class<E>? = null) : CompositeEntityClass<Int, Int, E>(table, entityType)
