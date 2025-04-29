package uk.ac.ucl.rits.inform.informdb;

import org.hibernate.dialect.H2Dialect;

import java.sql.Types;

/**
 * Associate JDBC types with H2 types.
 */
public class H2EmapDialect extends H2Dialect {
    /**
     * H2 seems to accept these Postgres-style types.
     */
    public H2EmapDialect() {
        super();
        registerColumnType(Types.TIMESTAMP, "timestamp with time zone");
        registerColumnType(Types.ARRAY, "DOUBLE PRECISION ARRAY");

        registerColumnType(BooleanDefaultFalse.INSTANCE.sqlType(), "boolean default FALSE");
    }
}
