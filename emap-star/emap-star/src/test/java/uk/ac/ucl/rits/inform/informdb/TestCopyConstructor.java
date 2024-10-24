package uk.ac.ucl.rits.inform.informdb;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import uk.ac.ucl.rits.inform.informdb.annotation.AuditTable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static uk.ac.ucl.rits.inform.informdb.DBTestUtils.findAllEntities;


/**
 * Ensure that all copy constructors copy all the fields from one entity to another.
 * @author Tom Young
 */
public class TestCopyConstructor {

    private static final List<String> STRINGS = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K");
    private static final List<Integer> INTEGERS = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    private static final List<String> BASE_CLASS_NAMES = Arrays.asList(
            "boolean",
            "java.lang.Boolean",
            "java.lang.Double",
            "java.lang.Double[]",
            "java.lang.Long",
            "java.lang.String",
            "java.time.Instant",
            "java.time.LocalDate",
            "java.util.List",
            "long",
            "uk.ac.ucl.rits.inform.informdb.TemporalFrom"
    );

    private Integer index = 0;

    /**
     * Given a class, when a copy constructor is present, then it should copy the correct fields.
     */
    @TestFactory
    Stream<DynamicTest> testsForClassesHavingCorrectCopyConstructor() {

        AtomicReference<Stream<DynamicTest>> tests = new AtomicReference<>(Stream.empty());

        findAllEntities()
                .filter(this::hasCopyConstructor)
                .filter(this::isNotCoreClass)
                .forEach(entity -> tests.set(Stream.concat(tests.get(), testsForCorrectlyCopiedFields(entity))));

        return tests.get();
    }

    /**
     * Stream of tests on all fields being identical when copied, for a class which has a copy constructor.
     * @param entity Class entity
     */
    Stream<DynamicTest> testsForCorrectlyCopiedFields(Class<?> entity) {

        Stream.Builder<DynamicTest> testStreamBuilder = Stream.builder();

        Object instance = newInstance(entity.getName());
        Object instanceCopy = copyOf(entity, instance);

        for (Method getMethod : getterMethodsOf(entity)) {

            String testName = entity.getName() + ": " + getMethod.getName();
            DynamicTest test = DynamicTest.dynamicTest(testName,
                    () -> assertEquals(getMethod.invoke(instance), getMethod.invoke(instanceCopy)));

            testStreamBuilder.add(test);
        }

        return testStreamBuilder.build();
    }

    /**
     * List of all the getter methods of a class. Primary keys must be copied for classes annotated with AuditTable but
     * not otherwise. For example, for a RoomState class entity then the roomStateId can be different, so filter
     * these out.
     * @param entity Class entity
     */
    List<Method> getterMethodsOf(Class<?> entity) {

        boolean isAuditTable = Arrays.stream(entity.getDeclaredAnnotations())
                .anyMatch(a -> a.annotationType().equals(AuditTable.class));
        String primaryKeyGetter = "get" + entity.getSimpleName() + "Id";

        return Arrays.stream(entity.getMethods())
                .filter(m -> m.getName().startsWith("get"))
                .filter(m -> isAuditTable || !m.getName().equals(primaryKeyGetter))
                .collect(Collectors.toList());
    }

    boolean hasCopyConstructor(Class<?> entity) {
        return Arrays.stream(entity.getDeclaredConstructors()).anyMatch(c -> isCopyConstructorOf(c, entity));
    }

    boolean isNotCoreClass(Class<?> entity) {
        return !entity.getName().endsWith("Core");
    }

    /**
     * Increment the internal counter as to cycle through the string and integer types. This allows different string
     * fields to be set with different values (up to a maximum of 11) as to be able to check copy constructors which
     * have the following error:
     * <p>
     * X(other){
     * this.a = other.b;
     * this.b = other.a;
     * }
     * <p>
     * where both a and b are strings.
     */
    void incrementIndex() {
        index = (index + 1) % Collections.min(Arrays.asList(STRINGS.size(), INTEGERS.size()));
    }

    /**
     * Get a class entity from only a name
     */
    Class<?> classFromName(String className) {

        try {
            return Class.forName(className);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to generate an instance of " + className, e);
        }
    }

    /**
     * Get an instance from a class entity using the default no args constructor
     */
    Object defaultConstructedInstance(Class<?> entity) {

        try {
            return entity.getConstructor().newInstance();

        } catch (InvocationTargetException | InstantiationException
                 | IllegalAccessException | NoSuchMethodException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to generate an instance of " + entity.getName(), e);
        }
    }

    /**
     * Get all the setter methods of a class that probably have corresponding getters
     */
    List<Method> setterMethodsOf(Class<?> entity) {

        return Arrays.stream(entity.getMethods())
                .filter(m -> m.getName().startsWith("set"))
                .filter(m -> !m.getName().equals("setValueAsBytes"))
                .collect(Collectors.toList());
    }

    /**
     * Create a new instance of a base/primitive type/class
     */
    Object newBaseInstance(String className) {
        switch (className) {
            case "java.lang.Boolean":
            case "boolean":
                return true;
            case "java.time.Instant":
                return Instant.now();
            case "java.lang.Double":
                return (double) INTEGERS.get(index);
            case "java.lang.Double[]":
                return new Double[]{(double) INTEGERS.get(index)};
            case "long":
            case "java.lang.Long":
                return (long) INTEGERS.get(index);
            case "java.lang.String":
                return STRINGS.get(index);
            case "uk.ac.ucl.rits.inform.informdb.TemporalFrom":
                return new TemporalFrom(Instant.now(), Instant.now().plus(1, ChronoUnit.SECONDS));
            case "java.time.LocalDate":
                return LocalDate.now();
            case "java.util.List":
                return List.of();
        }

        throw new IllegalArgumentException("Need to add an implementation for initialising object of type: " + className);
    }

    /**
     * Create a new instance of a class given a name of it
     * @param className Name of a class
     * @return Instance of the class
     */
    Object newInstance(String className) {

        incrementIndex();

        if (BASE_CLASS_NAMES.contains(className)) {
            return newBaseInstance(className);
        }

        Class<?> entity = classFromName(className);
        Object instance = defaultConstructedInstance(entity);
        setAllFieldsOf(entity, instance);

        return instance;
    }

    /**
     * Given an instance of a class that corresponds to a particular entity set all the fields/attributes with
     * default values which may require instanciating new instances of wither base classes (e.g. integers or strings,
     * or recursively calling newInstance and setting fields of those parameters)
     * @param entity   Class entity
     * @param instance Class instance
     */
    void setAllFieldsOf(Class<?> entity, Object instance) {

        for (Method method : setterMethodsOf(entity)) {

            Object[] params = Arrays.stream(method.getParameterTypes())
                    .map(p -> newInstance(p.getTypeName()))
                    .toArray();

            try {
                method.invoke(instance, params);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Is a method of an entity a copy constructor?
     */
    boolean isCopyConstructorOf(Constructor<?> constructor, Class<?> entity) {
        return constructor.getParameterCount() == 1
                && constructor.getParameterTypes()[0].getName().equals(entity.getTypeName());
    }

    /**
     * Create a copy of a class instance given its entity and current instance by invoking the copy constructor
     * @param entity   Class entity
     * @param instance Class instance
     */
    Object copyOf(Class<?> entity, Object instance) {

        Method copyMethod = Arrays.stream(entity.getMethods())
                .filter(m -> m.getName().equals("copy"))
                .findFirst().orElseThrow(() -> new RuntimeException("No copy method"));

        try {
            return copyMethod.invoke(instance);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to call copy method", e);
        }
    }
}
