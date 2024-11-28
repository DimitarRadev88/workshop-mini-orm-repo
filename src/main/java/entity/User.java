package entity;

import annotation.Column;
import annotation.Entity;
import annotation.Id;

import java.time.LocalDate;

@Entity(name = "user")
public class User {
    @Id
    private long id;
    @Column(name = "username")
    private String username;
    @Column(name = "age")
    private int age;
    @Column(name = "registration")
    private LocalDate registration;

    public User() {}

    public User(String username, int age, LocalDate registration) {
        setUsername(username);
        setAge(age);
        setRegistration(registration);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public LocalDate getRegistration() {
        return registration;
    }

    public void setRegistration(LocalDate registration) {
        this.registration = registration;
    }
}
