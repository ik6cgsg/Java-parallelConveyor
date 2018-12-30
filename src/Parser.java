import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Main config file parser class
 */
public class Parser {
  private static final String delim = " |:|=";          // Delimiter in config file
  private static final String comment = "#";            // Comment in config file
  private static final Map<String, valTypes> mapTypes;  // Map of params name to types of file

  /**
   * Types of files in config
   */
  public enum valTypes {
    SRC_FILE,
    DST_FILE,
    CONF_FILE,
    LOG_FILE
  }

  static {
    mapTypes = new HashMap<>();
    mapTypes.put("src", valTypes.SRC_FILE);
    mapTypes.put("dst", valTypes.DST_FILE);
    mapTypes.put("conf", valTypes.CONF_FILE);
    mapTypes.put("log", valTypes.LOG_FILE);
  }

  /**
   * Parsing main cofiguration file (fill map of names to parameters)
   * @param configFileName
   * @throws IOException in case of file errors
   */
  public Map<valTypes, String> parse(String configFileName) throws IOException {
    Map<valTypes, String> fileNames = new HashMap<>();

    BufferedReader configReader = new BufferedReader(new FileReader(configFileName));
    String line;
    while ((line = configReader.readLine()) != null) {
      if (line.isEmpty())
        continue;
      String[] words = line.split(delim);
      if (words.length != 2)
        throw new IOException("Wrong number of arguments at " + line + " 2 arguments with delimeter ' :=' expected");
      if (words[0].startsWith(comment))
        continue;
      valTypes type = mapTypes.get(words[0]);
      if (type == null)
        throw new IOException("Unknown directive in file: " + configFileName + " at line: " + line);
      switch (type) {
        case SRC_FILE: {
          fileNames.put(valTypes.SRC_FILE, words[1]);
          break;
        }
        case DST_FILE: {
          fileNames.put(valTypes.DST_FILE, words[1]);
          break;
        }
        case CONF_FILE: {
          fileNames.put(valTypes.CONF_FILE, words[1]);
          break;
        }
        case LOG_FILE: {
          fileNames.put(valTypes.LOG_FILE, words[1]);
          break;
        }
      }
    }
    configReader.close();

    return fileNames;
  }
}
