package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeEntityID
import one.uclass.exposed.dao.id.composite.CompositeIDGenPart
import one.uclass.exposed.dao.id.composite.CompositeIdTable
import org.jetbrains.exposed.sql.transactions.TransactionManager

class DaoEntityIDGenPart<Gen : Comparable<Gen>>(genId: Gen?, table: CompositeIdTable<*, Gen>) :
    CompositeIDGenPart<Gen>(genId, table) {

    override fun invokeOnNoValue() {
        TransactionManager.current().entityCache.flushInserts(table)
    }
}

class DaoEntityID<Const : Comparable<Const>, Gen : Comparable<Gen>>(constId: Const, genId: DaoEntityIDGenPart<Gen>) :
    CompositeEntityID<Const, Gen>(constId, genId) {
    constructor(constId: Const, genId: Gen?, table: CompositeIdTable<Const, Gen>) : this(constId, DaoEntityIDGenPart(genId, table))
    constructor(constId: Const, genId: CompositeIDGenPart<Gen>) : this(constId, DaoEntityIDGenPart(genId._value as Gen, genId.table))

}

