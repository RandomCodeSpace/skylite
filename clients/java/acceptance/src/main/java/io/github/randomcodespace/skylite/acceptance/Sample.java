package io.github.randomcodespace.skylite.acceptance;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

/** One column of each type an ORM maps; every column is nullable. */
@Entity
public class Sample {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String text;
    private Long number;
    private Double ratio;
    private Boolean flag;

    @Column(precision = 12, scale = 2)
    private BigDecimal amount;

    private Instant createdAt;
    private LocalDate day;

    @Column(length = 1024 * 1024)
    private byte[] payload;

    public Long getId() { return id; }
    public String getText() { return text; }
    public void setText(String text) { this.text = text; }
    public Long getNumber() { return number; }
    public void setNumber(Long number) { this.number = number; }
    public Double getRatio() { return ratio; }
    public void setRatio(Double ratio) { this.ratio = ratio; }
    public Boolean getFlag() { return flag; }
    public void setFlag(Boolean flag) { this.flag = flag; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public LocalDate getDay() { return day; }
    public void setDay(LocalDate day) { this.day = day; }
    public byte[] getPayload() { return payload; }
    public void setPayload(byte[] payload) { this.payload = payload; }
}
