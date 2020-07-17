package main.java;

public class Nan {
    public static void main(String[] args){
        double num = 0.0;
        double den = 0;
        double quo = num / den;
        System.out.println("quo: " + quo);
        if(Double.isNaN(quo)){
            System.out.println("quo is NaN");
        } else {
            System.out.println("quo is not NaN");
        }
    }
}
