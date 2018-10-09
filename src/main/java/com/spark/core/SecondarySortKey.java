package com.spark.core;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 自定义二次排序 key
 * 需要实现Ordered接口
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {

    private static final long serialVersionUID = 7462167501518669533L;

    /**
     * 首先在自定义key里面, 定义需要进行排序的列
     * @param that
     * @return
     */
    private int first;
    private int secondary;

    @Override
    public int compare(SecondarySortKey other) {
        if (this.first - other.getFirst() != 0) {
            return this.first - other.getFirst();
        } else {
            return this.first - other.getSecondary();
        }
    }

    @Override
    public int compareTo(SecondarySortKey other) {
        if (this.first - other.getFirst() != 0) {
            return this.first - other.getFirst();
        } else {
            return this.first - other.getSecondary();
        }
    }

    /**
     * 定义大于的情况
     * @param other
     * @return
     */
    @Override
    public boolean $greater(SecondarySortKey other) {
        if (this.first > other.getFirst()) {
            return true;
        } else if (this.first == other.getFirst() && this.secondary > other.getSecondary()) {
            return true;
        }
        return false;
    }

    /**
     * 定义大于等于的情况
     * @param other
     * @return
     */
    @Override
    public boolean $greater$eq(SecondarySortKey other) {
        if (this.$greater(other)) {
            return true;
        } else if(this.first == other.getFirst() && this.secondary == other.getSecondary()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less(SecondarySortKey other) {
        if (this.first < other.getFirst()) {
            return true;
        } else if (this.first == other.getFirst() && this.secondary < other.getSecondary()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey other) {
        if (this.$less(other)) {
            return true;
        } else if(this.first == other.getFirst() && this.secondary == other.getSecondary()) {
            return true;
        }

        return false;
    }

    /**
     * 为要排序的列提供getter、setter、hashcode和equals方法
     */
    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecondary() {
        return secondary;
    }

    public void setSecondary(int secondary) {
        this.secondary = secondary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){return true;}
        if (o == null || getClass() != o.getClass()) {return false;}

        SecondarySortKey that = (SecondarySortKey) o;

        if (first != that.first) {return false;}
        return secondary == that.secondary;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + secondary;
        return result;
    }

    public SecondarySortKey(int first, int secondary) {
        this.first = first;
        this.secondary = secondary;
    }
}
