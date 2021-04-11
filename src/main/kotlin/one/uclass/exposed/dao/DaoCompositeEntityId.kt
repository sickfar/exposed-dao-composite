package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeEntityID
import one.uclass.exposed.dao.id.composite.CompositeEntityIdPart
import one.uclass.exposed.dao.id.composite.CompositeIdTable
import org.jetbrains.exposed.sql.transactions.TransactionManager

class DaoCompositeEntityIdPart<ID : Comparable<ID>>(id: ID?, table: CompositeIdTable<*, ID>) :
    CompositeEntityIdPart<ID>(id, table) {

    override fun invokeOnNoValue() {
        TransactionManager.current().compositeEntityCache.flushInserts(table)
    }
}

class DaoCompositeEntityId<ClassifierID : Comparable<ClassifierID>, ID : Comparable<ID>>(classifierId: ClassifierID, idPart: DaoCompositeEntityIdPart<ID>) :
    CompositeEntityID<ClassifierID, ID>(classifierId, idPart) {
    constructor(classifierId: ClassifierID, id: ID?, table: CompositeIdTable<ClassifierID, ID>) : this(classifierId, DaoCompositeEntityIdPart(id, table))
    constructor(classifierId: ClassifierID, idPart: CompositeEntityIdPart<ID>) : this(classifierId, DaoCompositeEntityIdPart(idPart._value as ID?, idPart.table))

}

