import core.EntityManager;
import entity.User;
import orm.MyConnector;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

public class Main {
    public static void main(String[] args) throws SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, InstantiationException {
        MyConnector.createConnection("root", "peanutbutter", "test");
        Connection connection = MyConnector.getConnection();

        EntityManager<User> entityManager = new EntityManager<>(connection);

        entityManager.persist(new User("name", 10, LocalDate.now()));
        entityManager.persist(new User("second", 21, LocalDate.now()));
        entityManager.persist(new User("some name", 20, LocalDate.now()));

        List<User> all = (List<User>) entityManager.find(User.class, "age > 15");

        all.forEach(e -> System.out.println(e.getUsername()  + " registered on " + e.getRegistration()));
    }
}
