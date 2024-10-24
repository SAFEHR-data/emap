package uk.ac.ucl.rits.inform.informdb.visit_recordings;

import org.hibernate.HibernateError;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;

import java.io.Serializable;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Objects;

public class WaveformArray implements UserType {
    @Override
    public int[] sqlTypes() {
        return new int[]{Types.ARRAY};
    }

    @Override
    public Class returnedClass() {
        return Double[].class;
    }

    @Override
    public boolean equals(Object o, Object o1) throws HibernateException {
        return Objects.equals(o, o1);
    }

    @Override
    public int hashCode(Object o) throws HibernateException {
        return Objects.hashCode(o);
    }

    @Override
    public Object nullSafeGet(ResultSet resultSet,
                              String[] strings,
                              SharedSessionContractImplementor sharedSessionContractImplementor,
                              Object o) throws HibernateException, SQLException {
        String columnName = strings[0];
        Array sqlArray = (Array) resultSet.getObject(columnName);
        Object[] doubleArray = (Object[]) sqlArray.getArray();
        return Arrays.copyOf(doubleArray, doubleArray.length, Double[].class);
    }

    @Override
    public void nullSafeSet(
            PreparedStatement preparedStatement,
            Object objToSet,
            int psIdx,
            SharedSessionContractImplementor sharedSessionContractImplementor
    ) throws HibernateException, SQLException {
        if (objToSet == null) {
            preparedStatement.setNull(psIdx, Types.ARRAY);
        } else {
            Double[] asDoubleArray = (Double[]) objToSet;
            Array sqlArray = preparedStatement.getConnection().createArrayOf("NUMERIC", asDoubleArray);
            preparedStatement.setArray(psIdx, sqlArray);
        }
    }

    @Override
    public Object deepCopy(Object o) throws HibernateException {
        if (o == null) {
            return null;
        }
        Double[] doubleArray = (Double[]) o;
        return doubleArray.clone();
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public Serializable disassemble(Object o) throws HibernateException {
        throw new HibernateError("JES JES JES");
    }

    @Override
    public Object assemble(Serializable serializable, Object o) throws HibernateException {
        throw new HibernateError("JES JES JES");
    }

    @Override
    public Object replace(Object o, Object o1, Object o2) throws HibernateException {
        throw new HibernateError("JES JES JES");
    }
}
