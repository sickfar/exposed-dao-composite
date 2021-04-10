package one.uclass.exposed.dao.id.composite

open class CompositeIDGenPart<Gen : Comparable<Gen>> protected constructor(
    val table: CompositeIdTable<*, Gen>,
    genId: Gen?
) : Comparable<CompositeIDGenPart<Gen>> {
    constructor(genId: Gen?, table: CompositeIdTable<*, Gen>) : this(table, genId)

    var _value: Any? = genId
    val value: Gen
        get() {
            if (_value == null) {
                invokeOnNoValue()
                check(_value != null) { "Entity must be inserted" }
            }

            @Suppress("UNCHECKED_CAST")
            return _value as Gen
        }

    protected open fun invokeOnNoValue() {}

    override fun toString() = value.toString()

    override fun hashCode() = value.hashCode()

    override fun equals(other: Any?): Boolean {
        if (other !is CompositeIDGenPart<*>) return false

        return other._value == _value && other.table == table
    }

    override fun compareTo(other: CompositeIDGenPart<Gen>): Int = value.compareTo(value)
}

open class CompositeEntityID<Const : Comparable<Const>, Gen : Comparable<Gen>> constructor(
    val constId: Const, genId: CompositeIDGenPart<Gen>
) : Comparable<CompositeEntityID<Const, Gen>> {

    val _genId = genId

    val genId: Gen get() = _genId.value

    val pair: Pair<Const, Gen> get() = Pair(constId, genId)

    @Suppress("UNCHECKED_CAST")
    val table: CompositeIdTable<Const, Gen>
        get() = _genId.table as CompositeIdTable<Const, Gen>

    override fun toString() = "constId=${constId} genId=${genId}"

    override fun compareTo(other: CompositeEntityID<Const, Gen>): Int =
        constId.compareTo(other.constId) - genId.compareTo(other.genId)

    override fun hashCode(): Int {
        var result = constId.hashCode()
        result = 31 * result + genId.hashCode()
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null) return false
        if (!javaClass.isAssignableFrom(other.javaClass) && !other.javaClass.isAssignableFrom(javaClass)) {
            return false
        }
        other as CompositeEntityID<*, *>

        if (constId != other.constId) return false
        if (genId != other.genId) return false

        return true
    }
}
