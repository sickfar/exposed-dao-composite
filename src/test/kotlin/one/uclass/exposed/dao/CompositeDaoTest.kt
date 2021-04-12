package one.uclass.exposed.dao

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeFalse
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompositeDaoTest {

    private lateinit var database: Database

    @BeforeAll
    fun setup() {
        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = "jdbc:tc:postgresql:12:///postgres"
        hikariConfig.driverClassName = "org.testcontainers.jdbc.ContainerDatabaseDriver"
        hikariConfig.username = "test"
        hikariConfig.password = "test"
        database = Database.connect(HikariDataSource(hikariConfig))
        transaction {
            SchemaUtils.create(BaseCompositeTable, ReferencedCompositeTable, BackReferencedCompositeTable, EnumTable)
        }
    }

    @Test
    @Order(Int.MIN_VALUE)
    fun testCreate() {
        val base = transaction {
            BaseEntity.new(1L) {
                data = "testData"
            }
        }
        assertNotEquals(0L, base.id.id)
        val list = transaction {
            ListEntity.new(1L) {
                value = "testValue"
            }
        }
        assertNotEquals(0L, list.id.id)
        val referenced = transaction {
            ReferencedEntity.new(1L) {
                this.base = base
                data = "testData"
            }
        }
        assertEquals(1L, referenced.id.classifierId)
        assertNotEquals(0L, referenced.id.id)
    }

    @Test
    fun testSelect() {
        val base = transaction {
            BaseEntity.new(1L) {
                data = "testData1"
            }
        }
        val list = transaction {
            ListEntity.new(1L) {
                value = "testValue"
            }
        }
        val referenced = transaction {
            ReferencedEntity.new(1L) {
                this.base = base
                data = "testData"
            }
        }
        assumeFalse(0L == base.id.id)
        assumeFalse(0L == referenced.id.id)
        assumeFalse(0L == list.id.id)
        transaction {
            val selectedBase = BaseEntity.findById(1L, base.id.id)?.load(BaseEntity::list)
            assertNotNull(selectedBase)
            assertFalse(selectedBase!!.list.empty())
            assertEquals(list.id, selectedBase.list.first().id)
        }
        transaction {
            val selectedReferenced = ReferencedEntity.findById(1L, referenced.id.id)?.load(ReferencedEntity::base)
            assertNotNull(selectedReferenced)
            assertEquals(base.id, selectedReferenced!!.base.id)
        }
    }

    @Test
    fun testEnumKey() {
        val entityOne = transaction {
            EnumEntity.new(1, IdEnum.VALUE_ONE) {
                value = "test"
            }
        }
        assertEquals(IdEnum.VALUE_ONE, entityOne.id.id)
        val foundEntity = transaction {
            EnumEntity.findById(1, IdEnum.VALUE_ONE)
        }
        assertNotNull(foundEntity)
        assertEquals(IdEnum.VALUE_ONE, foundEntity!!.id.id)
    }

}
