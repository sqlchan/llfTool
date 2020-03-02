package com.llf.util.DTO;

public class Address {
    private String add;
    private int num;

    public Address() {
    }

    public Address(String add, int num) {
        this.add = add;
        this.num = num;
    }

    public String getAdd() {
        return add;
    }

    public void setAdd(String add) {
        this.add = add;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
