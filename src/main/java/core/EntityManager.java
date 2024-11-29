package core;

import annotation.Column;
import annotation.Entity;
import annotation.Id;
import interfaces.DbContext;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;

public class EntityManager<E> implements DbContext<E> {

    private Connection connection;

    public EntityManager(Connection connection) {
        this.connection = connection;
    }

    @Override
    public boolean persist(E entity) throws IllegalAccessException, SQLException {
        Field primaryKey = getId(entity.getClass());
        primaryKey.setAccessible(true);
        Object value = primaryKey.get(entity);

        if (value == null || (long) value <= 0) {
            return doInsert(entity);
        }

        return doUpdate(entity, primaryKey);
    }

    @Override
    public void doCreate(Class<E> entityClass) throws SQLException {
        String tableName = getTableName(entityClass);
        String query = String.format("""
                        CREATE TABLE %s (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                        %s
                        );
                        """,
                tableName,
                getAllFieldsAndDataTypes(entityClass));

        PreparedStatement ps = connection.prepareStatement(query);

        ps.execute();
    }

    @Override
    public void doAlter(E entity) throws SQLException {
        String tableName = getTableName(entity.getClass());
        String query = String.format("""
                ALTER TABLE %s
                %s;
                """,
                tableName,
                getAddColumnStringForAllNewFields(entity.getClass()));

        PreparedStatement ps = connection.prepareStatement(query);
        ps.executeUpdate();
    }

    @Override
    public boolean delete(Class<E> entityClass, String where) throws SQLException {
        String tableName = getTableName(entityClass);
        String query = String.format("""
                DELETE FROM %s
                WHERE %s;
                """,
                tableName,
                where);

        PreparedStatement ps = connection.prepareStatement(query);

        return ps.executeUpdate() > 0;
    }

    private String getAddColumnStringForAllNewFields(Class<?> entityClass) throws SQLException {
        StringBuilder sb = new StringBuilder();
        Set<String> fields = getAllFieldsFromTable(entityClass);

        Arrays.stream(entityClass.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Column.class))
                .forEach(f -> {
                    String fieldName = f.getAnnotation(Column.class).name();
                    if (!fields.contains(fieldName)) {
                        sb.append("ADD COLUMN ").append(getNameAndDataTypeOfField(f).substring(4));
                    }
                });

        return sb.substring(0, sb.length() - 2);
    }

    private Set<String> getAllFieldsFromTable(Class<?> entityClass) throws SQLException {
        Set<String> allFields = new HashSet<>();
        String query = String.format("""
                        SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`
                        WHERE TABLE_SCHEMA = 'test'
                        AND `COLUMN_NAME` != 'id'
                        AND `TABLE_NAME` = '%s';
                        """,
                getTableName(entityClass));

        PreparedStatement ps = connection.prepareStatement(query);

        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            allFields.add(rs.getString(1));
        }

        return allFields;
    }


    @Override
    public Iterable<E> find(Class<E> entityClass) throws SQLException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
        return find(entityClass, null);
    }

    @Override
    public Iterable<E> find(Class<E> entityClass, String criteria) throws SQLException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        String tableName = getTableName(entityClass);
        String where = criteria != null ? "WHERE " + criteria : "";

        String query = String.format("""
                        SELECT * FROM %s
                        %s
                        """,
                tableName,
                where
        );

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);

        List<E> entities = new ArrayList<>();

        while (rs.next()) {
            E entity = entityClass.getDeclaredConstructor().newInstance();
            fillEntity(entityClass, entity, rs);
            entities.add(entity);
        }

        return entities;
    }

    @Override
    public E findFirst(Class<E> entityClass) throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return findFirst(entityClass, null);
    }

    @Override
    public E findFirst(Class<E> entityClass, String criteria) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String tableName = getTableName(entityClass);
        String where = criteria != null ? "WHERE " + criteria : "";

        String query = String.format("""
                        SELECT * FROM %s
                        %s
                        LIMIT 1;
                        """,
                tableName,
                where
        );

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);

        E entity = entityClass.getDeclaredConstructor().newInstance();

        rs.next();
        fillEntity(entityClass, entity, rs);

        return entity;
    }

    private String getAllFieldsAndDataTypes(Class<E> entityClass) {
        StringBuilder sb = new StringBuilder();

        Arrays.stream(entityClass.getDeclaredFields()).filter(f -> f.isAnnotationPresent(Column.class))
                .forEach(f -> sb.append(getNameAndDataTypeOfField(f)));

        return sb.substring(0, sb.length() - 2);
    }

    private String getNameAndDataTypeOfField(Field field) {
        field.setAccessible(true);
        return "    " + transformFieldNameToColumnName(field) + " " + transformFieldTypeToDataType(field) + ",\n";
    }

    private static String transformFieldTypeToDataType(Field field) {
        Class<?> fieldType = field.getType();
        if (fieldType == String.class) {
            return "VARCHAR(255)";
        } else if (fieldType == int.class || fieldType == Integer.class) {
            return "INT";
        } else if (fieldType == long.class || fieldType == Long.class) {
            return "BIGINT";
        } else if (fieldType == LocalDate.class) {
            return "DATE";
        }

        throw new IllegalArgumentException("Data type transform not implemented for field of type " + fieldType);
    }


    private static String transformFieldNameToColumnName(Field field) {
        return field.getAnnotation(Column.class).name();
    }

    private void fillEntity(Class<E> entityClass, E entity, ResultSet resultSet) throws SQLException, IllegalAccessException {
        Field[] fields = entityClass.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            fillField(field, entity, resultSet);
        }
    }

    private void fillField(Field field, E entity, ResultSet resultSet) throws SQLException, IllegalAccessException {
        if (field.getType() == int.class || field.getType() == long.class) {
            field.set(entity, resultSet.getInt(field.getName()));
        } else if (field.getType() == LocalDate.class) {
            field.set(entity, LocalDate.parse(resultSet.getString(field.getAnnotation(Column.class).name())));
        } else if (field.getType() == String.class) {
            field.set(entity, resultSet.getString(field.getAnnotation(Column.class).name()));
        }
    }

    private boolean doInsert(E entity) throws SQLException, IllegalAccessException {
        String tableName = getTableName(entity.getClass());
        String[] columnNames = getColumnNames(entity);
        String[] columnValues = getColumnValues(entity);

        String query = String.format("""
                        INSERT INTO %s (%s)
                        VALUES(%s);
                        """,
                tableName,
                String.join(", ", columnNames),
                String.join(", ", "?".repeat(columnNames.length).split(""))
        );

        PreparedStatement ps = connection.prepareStatement(query);

        for (int i = 0; i < columnValues.length; i++) {
            ps.setString(i + 1, columnValues[i]);
        }

        return ps.executeUpdate() > 0;
    }

    private boolean doUpdate(E entity, Field primaryKey) throws IllegalAccessException, SQLException {
        String tableName = getTableName(entity.getClass());
        String[] columnNames = getColumnNames(entity);
        String[] columnValues = getColumnValues(entity);

        String query = String.format("""
                        UPDATE %s
                        SET %s
                        WHERE id = %s;
                        """,
                tableName,
                String.join(", ", Arrays.stream(columnNames).map(c -> c + " = ?").toArray(String[]::new)),
                primaryKey.get(entity)
        );

        PreparedStatement ps = connection.prepareStatement(query);

        for (int i = 0; i < columnValues.length; i++) {
            ps.setString(i + 1, columnValues[i]);
        }

        return ps.executeUpdate() > 0;
    }

    private Field getId(Class<?> entity) {
        return Arrays.stream(entity.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .findFirst()
                .orElseThrow(() -> new UnsupportedOperationException("Entity does not have a primary key"));
    }

    private String[] getColumnValues(E entity) throws IllegalAccessException {
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Column.class))
                .map(f -> {
                    f.setAccessible(true);
                    try {
                        return String.valueOf(f.get(entity));
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }).toArray(String[]::new);
    }

    private static <E> String[] getColumnNames(E entity) {
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Column.class))
                .map(f -> f.getAnnotation(Column.class).name())
                .toArray(String[]::new);
    }

    private String getTableName(Class<?> entityClass) {
        return entityClass.getAnnotation(Entity.class).name();
    }

}
