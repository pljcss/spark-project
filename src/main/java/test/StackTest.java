package test;

public class StackTest {

    Object obj1 = new Object();
    Object obj2 = new Object();

    public void fun1() {
        synchronized (obj1) {
            fun2();
        }
    }

    public void fun2() {
        synchronized (obj2) {
            while (true){
                System.out.println("--------");
            }
        }
    }

    public static void main(String[] args) {
        StackTest stackTest = new StackTest();

        stackTest.fun1();
    }
}
