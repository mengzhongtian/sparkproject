public class Singleton {
    private static Singleton sin=null;
    private Singleton() {

    }

    public static Singleton getInstance() {
        if (sin == null) {
            synchronized (Singleton.class) {
                if (sin == null) sin = new Singleton();
            }
        }


        return sin;
    }


}
