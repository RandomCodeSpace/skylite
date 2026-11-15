package io.github.randomcodespace.skylite.acceptance;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Version;

@Entity
public class Account {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private long balance;

    @Version
    private Long version;

    protected Account() {}

    public Account(long balance) {
        this.balance = balance;
    }

    public Long getId() { return id; }
    public long getBalance() { return balance; }
    public void setBalance(long balance) { this.balance = balance; }
    public Long getVersion() { return version; }
}
