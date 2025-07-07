package br.com.sankhya.acoesgrautec.extensions;

public class LogConfiguration {
    private static String path;

    public LogConfiguration() {
    }

    public static String getPath() {
        return path;
    }

    public static void setPath(String path) {
        LogConfiguration.path = path;
    }
}
