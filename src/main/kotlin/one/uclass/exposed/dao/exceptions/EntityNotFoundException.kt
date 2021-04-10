package one.uclass.exposed.dao.exceptions

import one.uclass.exposed.dao.CompositeEntityClass
import one.uclass.exposed.dao.id.composite.CompositeEntityID

class EntityNotFoundException(val id: CompositeEntityID<*, *>, val entity: CompositeEntityClass<*, *, *>)
    : Exception("Entity ${entity.klass.simpleName}, id=$id not found in the database")
