package uk.ac.ucl.rits.inform.informdb;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.persistence.Column;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ensure Instant types are annotated correctly.
 */
class TestInstantTypes {

    private void fieldHasNoManualType(Field field) {
        List<String> columnDefinition = Arrays.stream(field.getAnnotationsByType(Column.class))
                .map(Column::columnDefinition)
                .collect(Collectors.toList());
        if (columnDefinition.size() == 1) {
            // columnDefinition can be either absent or empty string
            assertEquals("", columnDefinition.get(0),
                    String.format("Instant field '%s' cannot specify non-empty columnDefinition text",
                            field.getName()));
        }
    }

    /**
     * Ensure every Instant field is of a timezone aware type.
     */
    @ParameterizedTest
    @MethodSource("uk.ac.ucl.rits.inform.informdb.DBTestUtils#findAllEntities")
    void testEntityInstantFields(Class<?> entityClass) {

        Arrays.stream(entityClass.getDeclaredFields())
                .filter(field -> field.getType().equals(Instant.class))
                .forEach(this::fieldHasNoManualType);
    }
}
