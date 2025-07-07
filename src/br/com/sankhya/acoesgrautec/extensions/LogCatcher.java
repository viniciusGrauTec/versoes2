package br.com.sankhya.acoesgrautec.extensions;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import br.com.sankhya.acoesgrautec.util.LogType;

public class LogCatcher {
    public LogCatcher() {
    }

    public static void logError(Throwable error) {
        StringWriter errors = new StringWriter();
        error.printStackTrace(new PrintWriter(errors));
        String message = buildMessage(LogType.ERROR, errors.toString());

        try {
            log(message);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void logError(String error) {
        String message = buildMessage(LogType.ERROR, error);

        try {
            log(message);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void logInfo(String info) {
        String message = buildMessage(LogType.INFO, info);

        try {
            log(message);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void log(String message) throws Exception {
        String path = LogConfiguration.getPath() + "/log" + LocalDate.now().toString() + ".txt";
        File logfile = new File(path);
        logfile.createNewFile();
        FileWriter writer = new FileWriter(path, true);
        writer.write(message);
        writer.close();
    }

    private static String buildMessage(LogType type, String message) {
        String fmessage = actualDateTime() + " -  [" + type.toString() + "]  - " + message + "\n";
        return fmessage;
    }

    private static String actualDateTime() {
        LocalDateTime dt = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        String formatoPersonalizado = dt.format(formatter);
        return formatoPersonalizado;
    }
}
