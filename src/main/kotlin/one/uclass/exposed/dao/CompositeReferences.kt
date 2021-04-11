package one.uclass.exposed.dao

import one.uclass.exposed.dao.id.composite.CompositeIdTable
import org.jetbrains.exposed.sql.Column
import org.jetbrains.exposed.sql.SizedIterable
import org.jetbrains.exposed.sql.emptySized
import org.jetbrains.exposed.sql.transactions.TransactionManager
import kotlin.properties.ReadOnlyProperty
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

private fun checkReference(reference: Column<*>, factoryTable: CompositeIdTable<*, *>) {
    val refColumn = reference.referee ?: error("Column $reference is not a reference")
    val targetTable = refColumn.table
    if (factoryTable != targetTable) {
        error("Column and factory point to different tables")
    }
}

class Reference<REF:Comparable<REF>, ClassifierID:Comparable<ClassifierID>, ID: Comparable<ID>, out Target : CompositeEntity<ClassifierID, ID>> (val reference: Column<REF>, val factory: CompositeEntityClass<ClassifierID, ID, Target>) {
    init {
        checkReference(reference, factory.table)
    }
}

class OptionalReference<REF:Comparable<REF>, ClassifierID:Comparable<ClassifierID>, ID: Comparable<ID>, out Target : CompositeEntity<ClassifierID, ID>> (val reference: Column<REF?>, val factory: CompositeEntityClass<ClassifierID, ID, Target>) {
    init {
        checkReference(reference, factory.table)
    }
}

internal class BackReference<ClassifierID:Comparable<ClassifierID>, ParentID:Comparable<ParentID>, out Parent: CompositeEntity<ClassifierID, ParentID>, ChildID:Comparable<ChildID>, in Child: CompositeEntity<ClassifierID, ChildID>, REF>
(reference: Column<REF>, factory: CompositeEntityClass<ClassifierID, ParentID, Parent>) : ReadOnlyProperty<Child, Parent> {
    internal val delegate = Referrers<ClassifierID, ChildID, Child, ParentID, Parent, REF>(reference, factory, true)

    override operator fun getValue(thisRef: Child, property: KProperty<*>) = delegate.getValue(thisRef.apply { thisRef.id.id }, property).single() // flush entity before to don't miss newly created entities
}

class OptionalBackReference<ClassifierID:Comparable<ClassifierID>, ParentID:Comparable<ParentID>, out Parent: CompositeEntity<ClassifierID, ParentID>, ChildID:Comparable<ChildID>, in Child: CompositeEntity<ClassifierID, ChildID>, REF>
(reference: Column<REF?>, factory: CompositeEntityClass<ClassifierID, ParentID, Parent>) : ReadOnlyProperty<Child, Parent?> {
    internal val delegate = OptionalReferrers<ClassifierID, ChildID, Child, ParentID, Parent, REF>(reference, factory, true)

    override operator fun getValue(thisRef: Child, property: KProperty<*>) = delegate.getValue(thisRef.apply { thisRef.id.id }, property).singleOrNull()  // flush entity before to don't miss newly created entities
}

class Referrers<ClassifierID:Comparable<ClassifierID>, ParentID:Comparable<ParentID>, in Parent: CompositeEntity<ClassifierID, ParentID>, ChildID:Comparable<ChildID>, out Child: CompositeEntity<ClassifierID, ChildID>, REF>
(val reference: Column<REF>, val factory: CompositeEntityClass<ClassifierID, ChildID, Child>, val cache: Boolean) : ReadOnlyProperty<Parent, SizedIterable<Child>> {
    init {
        reference.referee ?: error("Column $reference is not a reference")

        if (factory.table != reference.table) {
            error("Column and factory point to different tables")
        }
    }

    override operator fun getValue(thisRef: Parent, property: KProperty<*>): SizedIterable<Child> {
        val value = thisRef.run { reference.referee<REF>()!!.lookup() }
        if (thisRef.id._idPart._value == null || value == null) return emptySized()

        val query = {factory.find{reference eq value }}
        return if (cache) TransactionManager.current().compositeEntityCache.getOrPutReferrers(thisRef.id, reference, query) else query()
    }
}

class OptionalReferrers<ClassifierID: Comparable<ClassifierID>, ParentID:Comparable<ParentID>, in Parent: CompositeEntity<ClassifierID, ParentID>, ChildID:Comparable<ChildID>, out Child: CompositeEntity<ClassifierID, ChildID>, REF>
(val reference: Column<REF?>, val factory: CompositeEntityClass<ClassifierID, ChildID, Child>, val cache: Boolean) : ReadOnlyProperty<Parent, SizedIterable<Child>> {
    init {
        reference.referee ?: error("Column $reference is not a reference")

        if (factory.table != reference.table) {
            error("Column and factory point to different tables")
        }
    }

    override operator fun getValue(thisRef: Parent, property: KProperty<*>): SizedIterable<Child> {
        val value = thisRef.run { reference.referee<REF>()!!.lookup() }
        if (thisRef.id._idPart._value == null || value == null) return emptySized()

        val query = {factory.find{reference eq value }}
        return if (cache) TransactionManager.current().compositeEntityCache.getOrPutReferrers(thisRef.id, reference, query)  else query()
    }
}

private fun <SRC: CompositeEntity<*, *>> getReferenceObjectFromDelegatedProperty(entity: SRC, property: KProperty1<SRC, Any?>) : Any? {
    property.isAccessible   = true
    return property.getDelegate(entity)
}

private fun <SRC: CompositeEntity<*, *>> filterRelationsForEntity(entity: SRC, relations: Array<out KProperty1<out CompositeEntity<*, *>, Any?>>): Collection<KProperty1<SRC, Any?>> {
    val validMembers = entity::class.memberProperties
    return validMembers.filter { it in relations } as Collection<KProperty1<SRC, Any?>>
}

@Suppress("UNCHECKED_CAST")
private fun <ClassifierID: Comparable<ClassifierID>, ID: Comparable<ID>> List<CompositeEntity<ClassifierID, ID>>.preloadRelations(vararg relations: KProperty1<out CompositeEntity<*, *>, Any?>,
                                                                            nodesVisited: MutableSet<CompositeEntityClass<*, *, *>> = mutableSetOf())  {
    val entity              = this.firstOrNull() ?: return
    if(nodesVisited.contains(entity.klass)) {
        return
    } else {
        nodesVisited.add(entity.klass)
    }

    val directRelations = filterRelationsForEntity(entity, relations)
    directRelations.forEach {
        when(val refObject = getReferenceObjectFromDelegatedProperty(entity, it)) {
            is Reference<*, *, *, *> -> {
                (refObject as Reference<Comparable<Comparable<*>>, *, *, CompositeEntity<*, *>>).reference.let { refColumn ->
                    this.map { it.run { refColumn.lookup() } }.takeIf { it.isNotEmpty() }?.let { refIds ->
                        refObject.factory.find { refColumn.referee<Comparable<Comparable<*>>>()!! inList refIds.distinct() }.toList()
                    }.orEmpty()
                }
            }
            is OptionalReference<*, *, *, *> -> {
                (refObject as OptionalReference<Comparable<Comparable<*>>, *, *, CompositeEntity<*, *>>).reference.let { refColumn ->
                    this.mapNotNull { it.run { refColumn.lookup() } }.takeIf { it.isNotEmpty() }?.let { refIds ->
                        refObject.factory.find { refColumn.referee<Comparable<Comparable<*>>>()!! inList refIds.distinct() }.toList()
                    }.orEmpty()
                }
            }
            is Referrers<*, *, *, *, *, *> -> {
                (refObject as Referrers<ClassifierID, ID, CompositeEntity<ClassifierID, ID>, *, CompositeEntity<ClassifierID, *>, Any>).reference.let { refColumn ->
                    val refIds = this.map { it.run { refColumn.referee<Any>()!!.lookup() } }
                    refObject.factory.warmUpReferences(entity.id.classifierId, refIds, refColumn)
                }
            }
            is OptionalReferrers<*, *, *, *, *, *> -> {
                (refObject as OptionalReferrers<ClassifierID, ID, CompositeEntity<ClassifierID, ID>, *, CompositeEntity<ClassifierID, *>, Any>).reference.let { refColumn ->
                    val refIds = this.mapNotNull { it.run { refColumn.referee<Any?>()!!.lookup() } }
                    refObject.factory.warmUpOptReferences(entity.id.classifierId, refIds, refColumn)
                }
            }
            is CompositeInnerTableLink<*, *, *, *, *> -> {
                refObject.target.warmUpLinkedReferences(entity.id.classifierId, this.map{ it.id }, refObject.table)
            }
            is BackReference<*, *, *, *, *, *> -> {
                (refObject.delegate as Referrers<ClassifierID, ID, CompositeEntity<ClassifierID, ID>, *, CompositeEntity<ClassifierID, *>, Any>).reference.let { refColumn ->
                    val refIds = this.map { it.run { refColumn.referee<Any>()!!.lookup() } }
                    refObject.delegate.factory.warmUpReferences(entity.id.classifierId, refIds, refColumn)
                }
            }
            is OptionalBackReference<*, *, *, *, *, *> -> {
                (refObject.delegate as OptionalReferrers<ClassifierID, ID, CompositeEntity<ClassifierID, ID>, *, CompositeEntity<ClassifierID, *>, Any>).reference.let { refColumn ->
                    val refIds = this.map { it.run { refColumn.referee<Any>()!!.lookup() } }
                    refObject.delegate.factory.warmUpOptReferences(entity.id.classifierId, refIds, refColumn)
                }
            }
            else -> error("Relation delegate has an unknown type")
        }
    }

    if(directRelations.isNotEmpty() && relations.size != directRelations.size) {
        val remainingRelations      = relations.toList() - directRelations
        directRelations.map { relationProperty ->
            val relationsToLoad = this.flatMap {
                when(val relation = (relationProperty as KProperty1<CompositeEntity<*, *>, *>).get(it)) {
                    is SizedIterable<*> -> relation.toList()
                    is CompositeEntity<*, *> -> listOf(relation)
                    null                -> listOf()
                    else                -> error("Unrecognised loaded relation")
                } as List<CompositeEntity<ClassifierID, Int>>
            }.groupBy { it::class }

            relationsToLoad.forEach { (_, entities) ->
                entities.preloadRelations(*remainingRelations.toTypedArray() as Array<out KProperty1<CompositeEntity<*, *>, Any?>>, nodesVisited = nodesVisited)
            }
        }
    }
}

fun <ClassifierID : Comparable<ClassifierID>, SRClassifierID : Comparable<SRClassifierID>, SRC: CompositeEntity<ClassifierID, SRClassifierID>, REF : CompositeEntity<*, *>, T: Iterable<SRC>> T.with(vararg relations: KProperty1<out REF, Any?>): T = apply {
    toList().preloadRelations(*relations)
}

fun <ClassifierID : Comparable<ClassifierID>, SRClassifierID : Comparable<SRClassifierID>, SRC: CompositeEntity<ClassifierID, SRClassifierID>> SRC.load(vararg relations: KProperty1<out CompositeEntity<*, *>, Any?>): SRC = apply {
    listOf(this).with(*relations)
}
