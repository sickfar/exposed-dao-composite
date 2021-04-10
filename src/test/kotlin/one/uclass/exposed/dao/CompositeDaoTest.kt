package one.uclass.exposed.dao

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeFalse

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
            SchemaUtils.create(BaseCompositeTable, ReferencedCompositeTable, BackReferencedCompositeTable)
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
        assertNotEquals(0L, base.id.genId)
        val list = transaction {
            ListEntity.new(1L) {
                value = "testValue"
            }
        }
        assertNotEquals(0L, list.id.genId)
        val referenced = transaction {
            ReferencedEntity.new(1L) {
                this.base = base
                data = "testData"
            }
        }
        assertEquals(1L, referenced.id.constId)
        assertNotEquals(0L, referenced.id.genId)
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
        assumeFalse(0L == base.id.genId)
        assumeFalse(0L == referenced.id.genId)
        assumeFalse(0L == list.id.genId)
        transaction {
            val selectedBase = BaseEntity.findById(1L, base.id.genId)?.load(BaseEntity::list)
            assertNotNull(selectedBase)
            assertFalse(selectedBase!!.list.empty())
            assertEquals(list.id, selectedBase.list.first().id)
        }
        transaction {
            val selectedReferenced = ReferencedEntity.findById(1L, referenced.id.genId)?.load(ReferencedEntity::base)
            assertNotNull(selectedReferenced)
            assertEquals(base.id, selectedReferenced!!.base.id)
        }
    }

}
