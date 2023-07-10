package querying.main.stores

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

object PostgresqlStore {

  val hikariConfig = new HikariConfig()

  //hikariConfig.setJdbcUrl("jdbc:postgresql://localhost:5432/mimic")

  hikariConfig.setJdbcUrl("jdbc:postgresql://155.223.25.1:5433/mimic")

  //hikariConfig.setUsername("postgres")

  hikariConfig.setUsername("bigdata")

  //hikariConfig.setPassword("admin123")

  hikariConfig.setPassword("postgres")

  hikariConfig.addDataSourceProperty("cachePrepStmts", "true")

  hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250")

  hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")

  hikariConfig.setDriverClassName("org.postgresql.Driver")

  val hikariDataSource = new HikariDataSource(hikariConfig)
  
}
