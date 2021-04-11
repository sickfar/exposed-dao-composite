package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.*

class DaoCompositeEntityIdFactory : CompositeEntityIDFactory {
    fun <T : Comparable<T>, R : Comparable<R>> createEntityID(
        primary: T,
        secondary: R,
        table: CompositeIdTable<T, R>
    ): CompositeEntityID<T, R> {
        return DaoCompositeEntityId(primary, secondary, table)
    }

    override fun <T : Comparable<T>> createEntityID(id: T, table: CompositeIdTable<*, T>): CompositeEntityIdPart<T> {
        return DaoCompositeEntityIdPart(id, table)
    }
}
