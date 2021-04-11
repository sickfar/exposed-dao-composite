package one.uclass.exposed.dao.id.composite

open class CompositeEntityIdPart<ID : Comparable<ID>> protected constructor(
    val table: CompositeIdTable<*, ID>,
    id: ID?
) : Comparable<CompositeEntityIdPart<ID>> {
    constructor(id: ID?, table: CompositeIdTable<*, ID>) : this(table, id)

    var _value: Any? = id
    val value: ID
        get() {
            if (_value == null) {
                invokeOnNoValue()
                check(_value != null) { "Entity must be inserted" }
            }

            @Suppress("UNCHECKED_CAST")
            return _value as ID
        }

    protected open fun invokeOnNoValue() {}

    override fun toString() = value.toString()

    override fun hashCode() = value.hashCode()

    override fun equals(other: Any?): Boolean {
        if (other !is CompositeEntityIdPart<*>) return false

        return other._value == _value && other.table == table
    }

    override fun compareTo(other: CompositeEntityIdPart<ID>): Int = value.compareTo(value)
}

open class CompositeEntityID<ClassifierID : Comparable<ClassifierID>, ID : Comparable<ID>> constructor(
    val classifierId: ClassifierID, idPart: CompositeEntityIdPart<ID>
) : Comparable<CompositeEntityID<ClassifierID, ID>> {

    val _idPart = idPart

    val id: ID get() = _idPart.value

    val pair: Pair<ClassifierID, ID> get() = Pair(classifierId, id)

    @Suppress("UNCHECKED_CAST")
    val table: CompositeIdTable<ClassifierID, ID>
        get() = _idPart.table as CompositeIdTable<ClassifierID, ID>

    override fun toString() = "classifierId=${classifierId} id=${id}"

    override fun compareTo(other: CompositeEntityID<ClassifierID, ID>): Int =
        classifierId.compareTo(other.classifierId) - id.compareTo(other.id)

    override fun hashCode(): Int {
        var result = classifierId.hashCode()
        result = 31 * result + id.hashCode()
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null) return false
        if (!javaClass.isAssignableFrom(other.javaClass) && !other.javaClass.isAssignableFrom(javaClass)) {
            return false
        }
        other as CompositeEntityID<*, *>

        if (classifierId != other.classifierId) return false
        if (id != other.id) return false

        return true
    }
}
