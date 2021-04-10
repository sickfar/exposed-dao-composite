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


data class EntityChange(
    val entityClass: CompositeEntityClass<*, *, CompositeEntity<*, *>>,
    val entityId: CompositeEntityID<*, *>,
    val changeType: EntityChangeType,
    val transactionId: String
)

fun <CID : Comparable<CID>, GID : Comparable<GID>, T : CompositeEntity<CID, GID>> EntityChange.toEntity(): T? =
    (entityClass as CompositeEntityClass<CID, GID, T>).findById(entityId as CompositeEntityID<CID, GID>)

fun <CID : Comparable<CID>, GID : Comparable<GID>, T : CompositeEntity<CID, GID>> EntityChange.toEntity(klass: CompositeEntityClass<CID, GID, T>): T? {
    if (!entityClass.isAssignableTo(klass)) return null
    @Suppress("UNCHECKED_CAST")
    return toEntity<CID, GID, T>()
}

private val Transaction.entityEvents: Deque<EntityChange> by transactionScope { ConcurrentLinkedDeque() }
private val entitySubscribers = ConcurrentLinkedQueue<(EntityChange) -> Unit>()

object EntityHook {
    fun subscribe(action: (EntityChange) -> Unit): (EntityChange) -> Unit {
        entitySubscribers.add(action)
        return action
    }

    fun unsubscribe(action: (EntityChange) -> Unit) {
        entitySubscribers.remove(action)
    }
}

fun Transaction.registerChange(
    entityClass: CompositeEntityClass<*, *, CompositeEntity<*, *>>,
    entityId: CompositeEntityID<*, *>,
    changeType: EntityChangeType
) {
    EntityChange(entityClass, entityId, changeType, id).let {
        if (entityEvents.peekLast() != it) {
            entityEvents.addLast(it)
        }
    }
}

fun Transaction.alertSubscribers() {
    while (true) {
        val event = entityEvents.pollFirst() ?: break
        entitySubscribers.forEach { it(event) }
    }
}

fun Transaction.registeredChanges() = entityEvents.toList()

fun <T> withHook(action: (EntityChange) -> Unit, body: () -> T): T {
    EntityHook.subscribe(action)
    try {
        return body().apply {
            TransactionManager.current().commit()
        }
    } finally {
        EntityHook.unsubscribe(action)
    }
}
