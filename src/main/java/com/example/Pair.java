package com.example;

public class Pair<T1, T2> {
    private final T1 key;
    private final T2 value;
    public Pair(T1 key, T2 value) {
        this.key = key;
        this.value = value;
    }
    public T1 getKey() {
        return key;
    }
    public T2 getValue() {
        return value;
    }
    public static <T1, T2> Pair<T1, T2> of(T1 key, T2 value) {
        return new Pair<T1,T2>(key, value);
    }
}
