package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeEntityID
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transactionScope
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentLinkedQueue

enum class EntityChangeType {
    Created,
    Updated,
    Removed;
}


data class CompositeEntityChange(
    val entityClass: CompositeEntityClass<*, *, CompositeEntity<*, *>>,
    val entityId: CompositeEntityID<*, *>,
    val changeType: EntityChangeType,
    val transactionId: String
)

@Suppress("UNCHECKED_CAST")
fun <ClassifierID : Comparable<ClassifierID>, ID : Comparable<ID>, T : CompositeEntity<ClassifierID, ID>> CompositeEntityChange.toEntity(): T? =
    (entityClass as CompositeEntityClass<ClassifierID, ID, T>).findById(entityId as CompositeEntityID<ClassifierID, ID>)

fun <ClassifierID : Comparable<ClassifierID>, ID : Comparable<ID>, T : CompositeEntity<ClassifierID, ID>> CompositeEntityChange.toEntity(klass: CompositeEntityClass<ClassifierID, ID, T>): T? {
    if (!entityClass.isAssignableTo(klass)) return null
    @Suppress("UNCHECKED_CAST")
    return toEntity<ClassifierID, ID, T>()
}

private val Transaction.compositeEntityEvents: Deque<CompositeEntityChange> by transactionScope { ConcurrentLinkedDeque() }
private val compositeEntitySubscribers = ConcurrentLinkedQueue<(CompositeEntityChange) -> Unit>()

object EntityHook {
    fun subscribe(action: (CompositeEntityChange) -> Unit): (CompositeEntityChange) -> Unit {
        compositeEntitySubscribers.add(action)
        return action
    }

    fun unsubscribe(action: (CompositeEntityChange) -> Unit) {
        compositeEntitySubscribers.remove(action)
    }
}

fun Transaction.registerChange(
    entityClass: CompositeEntityClass<*, *, CompositeEntity<*, *>>,
    entityId: CompositeEntityID<*, *>,
    changeType: EntityChangeType
) {
    CompositeEntityChange(entityClass, entityId, changeType, id).let {
        if (compositeEntityEvents.peekLast() != it) {
            compositeEntityEvents.addLast(it)
        }
    }
}

fun Transaction.alertSubscribers() {
    while (true) {
        val event = compositeEntityEvents.pollFirst() ?: break
        compositeEntitySubscribers.forEach { it(event) }
    }
}

fun Transaction.registeredChanges() = compositeEntityEvents.toList()

fun <T> withHook(action: (CompositeEntityChange) -> Unit, body: () -> T): T {
    EntityHook.subscribe(action)
    try {
        return body().apply {
            TransactionManager.current().commit()
        }
    } finally {
        EntityHook.unsubscribe(action)
    }
}
