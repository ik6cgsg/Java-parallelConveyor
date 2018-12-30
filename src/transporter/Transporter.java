package transporter;

import logger.Logger;
import encoder.Encoder;
import PermutationCoder.PermutationCoder;
import executor.Executor;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Conveyor of executors
 */
public class Transporter {
  private static final String splitDelim = " |:|=";  // Delimiter in config file
  private static final String comment = "#";         // Comment in config file
  private static final int sleepTime = 1000;
  private ArrayList<Executor> exs;                   // Array of all executors in conveyor
  private DataInputStream inputFile;
  private DataOutputStream outputFile;
  private Map<Executor, ArrayList<Integer>> executorConsumer; // Array of executors to their consumers (array of indices in 'exs')

  /**
   * Types of params in config
   */
  private enum valTypes {
    EXECUTOR
  }

  private static final Map<String, valTypes> mapTypes;  // Map of params name to types of params

  static {
    mapTypes = new HashMap<>();
    mapTypes.put("executor", valTypes.EXECUTOR);
  }

  /**
   * Conveyor constructor
   * @param inFile
   * @param outFile
   * @param confFile
   * @throws IOException
   */
  public Transporter(String inFile, String outFile, String confFile) throws IOException {
    inputFile = new DataInputStream(new FileInputStream(inFile));
    outputFile = new DataOutputStream(new FileOutputStream(outFile));
    exs = new ArrayList<>();
    executorConsumer = new HashMap<>();
    /* setting configs  */
    setConfigs(confFile);
    if (exs.isEmpty())
      throw new IOException("Empty list of executors");
    /* acquainting executors */
    introduce();
  }

  /**
   * Set configs of conveyor from config file
   * @param confFile
   * @throws IOException
   */
  private void setConfigs(String confFile) throws IOException {
    BufferedReader configReader = new BufferedReader(new FileReader(confFile));
    String line;
    while ((line = configReader.readLine()) != null) {
      String[] words = line.split(splitDelim);
      if (words.length < 2)
        throw new IOException("Wrong number of arguments in file: " + confFile + " at: " + line);
      if (words[0].startsWith(comment))
        continue;
      valTypes type = mapTypes.get(words[0]);
      if (type == null)
        throw new IOException("Unknown config: " + words[0] + " in file: " + confFile + " at: " + line);
      switch (type) {
        case EXECUTOR: {
           Executor newExecutor;
          /* generating new executor by reflection */
          try {
            Class newEx = Class.forName(words[1]);
            newExecutor = (Executor) newEx.newInstance();
          } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            Logger.writeErrorLn(e.getMessage());
            return;
          }
          newExecutor.setConfigFile(words[2]);
          exs.add(newExecutor);
          ArrayList<Integer> consumersIds = new ArrayList();  // Array of indices from config file
          for (int i = 3; i < words.length; i++) {
            consumersIds.add(Integer.parseInt(words[i]));
          }
          executorConsumer.put(newExecutor, consumersIds);
          break;
        }
      }
    }
  }

  /**
   * Acquaint executors
   * @throws IOException
   */
  private void introduce() throws IOException {
    exs.get(0).setInput(inputFile);

    for (Executor cur : exs) {
      ArrayList<Integer> ids = executorConsumer.get(cur);
      for (int id : ids) {
        cur.setConsumer(exs.get(id - 1));
      }
    }

    exs.get(exs.size() - 1).setOutput(outputFile);
  }

  /**
   * Conveyor thread start function
   */
  public void run() {
    try {
      ArrayList<Thread> threads = new ArrayList<>();  // Array of all executors threads
      for (Executor ex : exs) {
        Thread th = new Thread(ex);
        threads.add(th);
        th.start();
        Logger.writeLn(th.getName() + " is started");
      }
      /* checking if conveyor finish work (first executor 'reader' set flag 'isOver' to true) */
      while (!exs.get(0).isOver())
        Thread.sleep(sleepTime);

      for (Thread th : threads) {
        while (th.isAlive())
          th.interrupt();
        Logger.writeLn(th.getName() + " is closed");
      }

      inputFile.close();
      outputFile.close();
      Logger.writeLn("i/o streams closed");
    } catch (IOException ex) {
      Logger.writeLn("Conveyer error! ");
      Logger.writeErrorLn(ex);
    } catch (InterruptedException ex) {
      Logger.writeErrorLn(ex);
    }
  }
}
