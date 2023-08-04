package com.bbva.lrba.mx.jsprk.examen.v00.model;

import java.sql.Timestamp;

public class RowDataEjercicio4 {

    private String title;
    private String country;
    private String dateAdded;
    private String releaseYear;
    private String rating;
    private String duration;
    private String listedIn;
    private Timestamp date;


    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getDateAdded() {
        return dateAdded;
    }

    public void setDateAdded(String dateAdded) {
        this.dateAdded = dateAdded;
    }

    public String getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(String releaseYear) {
        this.releaseYear = releaseYear;
    }

    public String getRating() {
        return rating;
    }

    public void setRating(String rating) {
        this.rating = rating;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getListedIn() {
        return listedIn;
    }

    public void setListedIn(String listedIn) {
        this.listedIn = listedIn;
    }

    public Timestamp getDate() {
        return date;
    }

    public void setDate(Timestamp date) {
        this.date = date;
    }
}
