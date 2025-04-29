package uk.ac.ucl.rits.inform.informdb;

import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.type.descriptor.java.BooleanTypeDescriptor;
import org.hibernate.type.descriptor.sql.BitTypeDescriptor;

/**
 * A clunky workaround for the fact that while Hibernate happily maps to appropriate *types* for
 * Postgres and SQL Server (boolean and bit respectively), it doesn't express the default value
 * in the correct way.
 * So, you have to mark your Boolean Java types with @Type(type = "this_class") to map it to
 * this class, which the custom dialects translate to literal SQL:
 *  - "boolean default false" for postgres
 *  - "bit default 0" for SQL Server
 *
 * The better workaround would be to use liquibase to initialise
 * our database in production, but we don't do that currently.
 */
public class BooleanDefaultFalse extends AbstractSingleColumnStandardBasicType<Boolean> {
    /**
     *  Singleton instance.
     */
    public static final BooleanDefaultFalse INSTANCE = new BooleanDefaultFalse();

    /**
     * Map the Java Boolean type to SQL (JDBC?) type.
     */
    public BooleanDefaultFalse() {
        super(BitTypeDescriptor.INSTANCE, BooleanTypeDescriptor.INSTANCE);
    }

    @Override
    public String getName() {
        return "boolean_default_false";
    }
}
