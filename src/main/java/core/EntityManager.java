package core;

import annotation.Column;
import annotation.Entity;
import annotation.Id;
import interfaces.DbContext;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
            return doInsert(entity, primaryKey);
        }

        return doUpdate(entity, primaryKey);
    }

    @Override
    public Iterable<E> find(Class<E> entityClass) throws SQLException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
        return find(entityClass, null);
    }

    @Override
    public Iterable<E> find(Class<E> entityClass, String condition) throws SQLException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        String tableName = getTableName(entityClass);
        String where = condition != null ? "WHERE " + condition : "";

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
    public E findFirst(Class<E> entityClass, String condition) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String tableName = getTableName(entityClass);
        String where = condition != null ? "WHERE " + condition : "";

        String query = String.format("""
                SELECT * FROM %s
                %s
                LIMIT 1
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

    private boolean doInsert(E entity, Field primaryKey) throws SQLException, IllegalAccessException {
        String tableName = getTableName(entity.getClass());
        String[] columnNames = getColumnNames(entity);
        String[] columnValues = getColumnValues(entity);

        String query = String.format("""
                        INSERT INTO %s (%s)
                        VALUES(%s)
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
                        WHERE id = %s
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
