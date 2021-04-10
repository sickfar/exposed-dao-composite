package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.*

class DaoEntityIDFactory : CompositeEntityIDFactory {
    fun <T : Comparable<T>, R : Comparable<R>> createEntityID(
        primary: T,
        secondary: R,
        table: CompositeIdTable<T, R>
    ): CompositeEntityID<T, R> {
        return DaoEntityID(primary, secondary, table)
    }

    override fun <T : Comparable<T>> createEntityID(genId: T, table: CompositeIdTable<*, T>): CompositeIDGenPart<T> {
        return DaoEntityIDGenPart(genId, table)
    }
}
