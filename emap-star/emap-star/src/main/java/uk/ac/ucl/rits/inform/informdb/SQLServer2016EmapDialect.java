package uk.ac.ucl.rits.inform.informdb;

import org.hibernate.dialect.SQLServer2016Dialect;

import java.sql.Types;

/**
 * Associate JDBC types with SQL Server types.
 */
public class SQLServer2016EmapDialect extends SQLServer2016Dialect {
    /**
     * Map all ARRAY types to varbinary.
     *
     * This currently doesn't work with waveform data (I think the only place we use arrays),
     * but since the main purpose of the SQL Server version is not about waveform data, I think
     * we can live with this. Importantly though, it does allow the DDL to be created!
     */
    public SQLServer2016EmapDialect() {
        super();
        registerColumnType(Types.ARRAY, "varbinary(max)");
        // it doesn't seem to be necessary to map CLOB to varchar(max)
        registerColumnType(BooleanDefaultFalse.INSTANCE.sqlType(), "bit default 0");
    }
}
