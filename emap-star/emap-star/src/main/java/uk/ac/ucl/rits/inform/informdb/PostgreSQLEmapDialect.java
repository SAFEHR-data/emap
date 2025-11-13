package uk.ac.ucl.rits.inform.informdb;

import org.hibernate.dialect.PostgreSQL95Dialect;

import java.sql.Types;

/**
 * Associate JDBC types with Postgres types.
 */
public class PostgreSQLEmapDialect extends PostgreSQL95Dialect {
    /**
     * Map all timestamps to "timestamp with time zone" in Postgres, rather
     * than specify it in the @Column annotation, which would make it incompatible
     * with SQL Server.
     * Map all ARRAY types to DOUBLE PRECISION ARRAY. This would be a problem if we
     * ever needed to use more than one type of SQL array. Hopefully we will have
     * found a better way of storing waveform data before then!
     */
    public PostgreSQLEmapDialect() {
        super();
        registerColumnType(Types.TIMESTAMP, "timestamp with time zone");
        registerColumnType(Types.ARRAY, "DOUBLE PRECISION ARRAY");
        // Map @Lob to "text"
        registerColumnType(Types.CLOB, "text");
    }

}
