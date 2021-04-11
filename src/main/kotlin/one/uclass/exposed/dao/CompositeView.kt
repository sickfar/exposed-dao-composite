package one.uclass.exposed.dao

import org.jetbrains.exposed.sql.Expression
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.SizedIterable
import org.jetbrains.exposed.sql.SortOrder
import kotlin.reflect.KProperty

class CompositeView<out Target: CompositeEntity<*, *>> (val op : Op<Boolean>, val factory: CompositeEntityClass<*, *, Target>) : SizedIterable<Target> {
    override fun limit(n: Int, offset: Long): SizedIterable<Target> = factory.find(op).limit(n, offset)
    override fun count(): Long = factory.find(op).count()
    override fun empty(): Boolean = factory.find(op).empty()
    override fun forUpdate(): SizedIterable<Target> = factory.find(op).forUpdate()
    override fun notForUpdate(): SizedIterable<Target> = factory.find(op).notForUpdate()

    override operator fun iterator(): Iterator<Target> = factory.find(op).iterator()
    operator fun getValue(o: Any?, desc: KProperty<*>): SizedIterable<Target> = factory.find(op)
    override fun copy(): SizedIterable<Target> = CompositeView(op, factory)
    override fun orderBy(vararg order: Pair<Expression<*>, SortOrder>): SizedIterable<Target> = factory.find(op).orderBy(*order)
}
