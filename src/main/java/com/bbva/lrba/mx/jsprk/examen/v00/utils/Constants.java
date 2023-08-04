package com.bbva.lrba.mx.jsprk.examen.v00.utils;

public enum Constants {
    ALL_COLUMNS("*"),
    CODE_COLUMN("code"),
    SERVICE_COLUMN("service"),
    TYPE_COLUMN("type"),
    DIRECTOR_COLUMN("director"),
    DURATION_COLUMN("duration"),
    NO_MOVIES_COLUMN("No. Movies"),
    TITLE_COLUMN("title"),
    CAST_COLUMN("cast"),
    DESCRIPTION_COLUMN("description"),

    CODE_NETFLIX("NFLX"),
    CODE_DISNEY("DSNY+"),
    CODE_AMAZON("AMZN"),
    SERVICE_NETFLIX("Netflix"),
    SERVICE_DISNEY("Disney+"),
    SERVICE_AMAZON("Amazon Prime"),

    MOVIE_TYPE("Movie");
    private String value;
    Constants(String string) {
        this.value = string;
    }

    public String getValue() {
        return value;
    }

}
