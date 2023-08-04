package com.bbva.lrba.mx.jsprk.examen.v00.model;

import java.math.BigInteger;
import java.sql.Timestamp;

public class RowDataEjercicio2 {

    private String service;
    private BigInteger count;

    private Timestamp date;

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public BigInteger getCount() {
        return count;
    }

    public void setCount(BigInteger count) {
        this.count = count;
    }

    public Timestamp getDate() {
        return date;
    }

    public void setDate(Timestamp date) {
        this.date = date;
    }
}
