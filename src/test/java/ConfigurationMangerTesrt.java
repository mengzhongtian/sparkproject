import conf.ConfigurationManager;

public class ConfigurationMangerTesrt {
    public static void main(String[] args) {
        String a = ConfigurationManager.getProperty("a");
        System.out.println(a);
    }
}
