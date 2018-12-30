package logger;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class Logger {
  private static final String errorTag = "Error: ";   // Error tag for output
  private static String fileName;                     // Logger file

  /**
   * Setting logger file
   * @param logName
   */
  public static void setLogFile(String logName) {
    fileName = logName;
  }

  /**
   * Write some data in logger file
   * @param data
   */
  public static void writeLn(String data) {
    PrintStream logWriter;
    try {
      if (fileName == null) {
        logWriter = System.out;
      } else {
        logWriter = new PrintStream(new BufferedOutputStream(new FileOutputStream(fileName, true)));
      }
      logWriter.println(data);
      if (fileName != null)
        logWriter.close();
    } catch (IOException ex) {
      System.out.println("logger.logger Error!");
      System.out.println(ex);
    }
  }

  /**
   * Write error string in logger file
   * @param error
   */
  public static void writeErrorLn(String error) {
    writeLn(errorTag + error);
  }

  /**
   * Write IOException error in logger file
   * @param error
   */
  public static void writeErrorLn(IOException error) {
    error.printStackTrace();
  }

  /**
   * Write InterruptedException error string in logger file
   * @param error
   */
  public static void writeErrorLn(InterruptedException error) {
    error.printStackTrace();
  }
}
