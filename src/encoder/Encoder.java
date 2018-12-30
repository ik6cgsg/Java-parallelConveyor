package encoder;

import adapter.AdapterType;
import adapter.ByteAdapter;
import adapter.DoubleAdapter;
import executor.Executor;
import javafx.util.Pair;
import logger.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Encoder class
 */
public class Encoder implements Executor {
  private static final String splitDelim = " |:|="; // Delimiter in config file
  private static final String delim = " ";          // Delimiter in table of probabilities file
  private static final String comment = "#";        // Comment in config file
  private static final String endl = "\n";
  private static final int sleepTime = 500;
  private static final Map<String, targetType> tMap;        // Map target name to target type
  private static final Map<String, confTypes> configMap;    // Map config name to config type
  private static final Map<String, tableMethodType> metMap; // Map table method name to table method type

  private enum targetType {
    ENCODE,
    DECODE,
    READ,
    WRITE
  }

  private enum tableMethodType {
    READ,   // read probabilities from table
    WRITE   // set probabilities to table file
  }

  private enum confTypes {
    BLOCK_SIZE,
    SEQUENCE_LEN,
    TEXT_LEN,
    PROBABILITY,
    DECODE_CONF,
    TARGET,
    TABLE_FILE,
    TABLE_METHOD,
    START   // start index for reading from provider adapter
  }

  class DoubleAdapterClass implements DoubleAdapter {
    private int
            index = 0,  // current index in dataOut
            offset,     // offset from begin of dataOut
            block;      // block to read

    /**
     * Get next double from dataOut
     * @return Double: next double if exists, null otherwise
     */
    @Override
    public Double getNextDouble() {
      if (block == 0)
        block = ((ArrayList<Byte>) dataOut).size();
      if (index >= ((ArrayList<Double>) dataOut).size() || index >= offset + block) {
        index = offset;
        return null;
      }
      return ((ArrayList<Double>) dataOut).get(index++);
    }

    /**
     * Set metrics to adapter
     * @param start
     * @param offset
     */
    @Override
    public void setMetrics(int start, int offset) {
      this.offset = start;
      this.block = offset;
      index = start;
    }

    /**
     * Get start index
     * @return Integer: get start property from super class (provider)
     */
    @Override
    public Integer getStart() {
      return start;
    }
  }

  class ByteAdapterClass implements ByteAdapter {
    private int
            index = 0,  // current index in dataOut
            offset,     // offset from begin of dataOut
            block;      // block to read

    /**
     * Get next byte from dataOut
     * @return Byte: next byte if exists, null otherwise
     */
    @Override
    public Byte getNextByte() {
      if (block == 0)
        block = ((ArrayList<Byte>) dataOut).size();
      if (index >= ((ArrayList<Byte>) dataOut).size() || index >= offset + block) {
        index = offset;
        return null;
      }
      return ((ArrayList<Byte>) dataOut).get(index++);
    }

    /**
     * Set metrics to adapter
     * @param start
     * @param offset
     */
    @Override
    public void setMetrics(int start, int offset) {
      this.offset = start;
      this.block = offset;
      index = start;
    }

    /**
     * Get start index
     * @return Integer: get start property from super class (provider)
     */
    @Override
    public Integer getStart() {
      return start;
    }
  }

  static {
    tMap = new HashMap<>();
    tMap.put("encode", targetType.ENCODE);
    tMap.put("decode", targetType.DECODE);
    tMap.put("read", targetType.READ);
    tMap.put("write", targetType.WRITE);

    configMap = new HashMap<>();
    configMap.put("num", confTypes.SEQUENCE_LEN);
    configMap.put("len", confTypes.TEXT_LEN);
    configMap.put("prob", confTypes.PROBABILITY);
    configMap.put("decconf", confTypes.DECODE_CONF);
    configMap.put("target", confTypes.TARGET);
    configMap.put("block", confTypes.BLOCK_SIZE);
    configMap.put("table", confTypes.TABLE_FILE);
    configMap.put("table_method", confTypes.TABLE_METHOD);
    configMap.put("start", confTypes.START);

    metMap = new HashMap<>();
    metMap.put("read", tableMethodType.READ);
    metMap.put("write", tableMethodType.WRITE);
  }

  private DataInputStream inputFile;
  private DataOutputStream outputFile;
  private String tableFile;
  private Map<Byte, Double> probability;                      // Map of letters to their probabilities
  private Map<Byte, Segment> segs;                            // Map of letters to their segments
  private int textLen, numSeq, blockSize, dataLen, start;
  private ArrayList<Executor> consumers;                      // Array of all consumers
  private Map<Executor, Pair<Object, AdapterType>> adapters;  // Map of providers to pair of their adapter and adapter type
  private Object dataOut;
  private boolean isReadyToRead;
  private boolean isReadyToWrite;
  private boolean isAvailable;                                // For double lock
  private boolean over;                                       // For first executor to check if conveyor finish work
  private targetType target = targetType.ENCODE;
  private ArrayList<AdapterType> readableTypes;               // Array of current readable types
  private ArrayList<AdapterType> writableTypes;               // Array of current writable types
  private Map<Integer, Pair<Object, AdapterType>> startToAdapter; // Map of start indices to pair of adapter and its adapter type
  private ArrayList<Integer> starts;                              // Array of start indices to sort provides

  public Encoder() {
    probability = new HashMap<>();
    readableTypes = new ArrayList<>();
    writableTypes = new ArrayList<>();
    consumers = new ArrayList<>();
    adapters = new HashMap<>();
    segs = new HashMap<>();
    textLen = 0;
    start = 0;
    over = false;
    startToAdapter = new HashMap<>();
    starts = new ArrayList<>();
  }

  /**
   * Set encoder configs from file
   * @param confFile
   * @throws IOException
   */
  private void setConfigs(String confFile) throws IOException {
    BufferedReader configReader = new BufferedReader(new FileReader(confFile));
    String line;
    while ((line = configReader.readLine()) != null) {
      String[] words = line.split(splitDelim);
      if (words.length != 2 && words.length != 3)
        throw new IOException("Wrong number of arguments in file: " + confFile + " at: " + line);
      if (words[0].startsWith(comment))
        continue;
      confTypes type = configMap.get(words[0]);
      if (type == null)
        throw new IOException("Unknown config: " + words[0] + " in file: " + confFile + " at: " + line);
      switch (type) {
        case SEQUENCE_LEN: {
          numSeq = Integer.parseInt(words[1]);
          break;
        }
        case START: {
          start = Integer.parseInt(words[1]);
          break;
        }
        case TEXT_LEN: {
          textLen = Integer.parseInt(words[1]);
          break;
        }
        case PROBABILITY: {
          byte ch = (byte) Integer.parseInt(words[1]);
          probability.put(ch, Double.parseDouble(words[2]));
          segs.put(ch, new Segment());
          break;
        }
        case TARGET: {
          target = tMap.get(words[1]);
          if (target == null)
            throw new IOException("Unknown target: " + words[1] + " in file: " + confFile + " at: " + line + " decode|encode expected");
          switch (target) {
            case ENCODE: {
              isReadyToRead = true;
              isReadyToWrite = false;
              isAvailable = true;
              readableTypes.add(AdapterType.BYTE);
              writableTypes.add(AdapterType.DOUBLE);
              break;
            }
            case DECODE: {
              isReadyToRead = true;
              isReadyToWrite = false;
              isAvailable = true;
              writableTypes.add(AdapterType.BYTE);
              readableTypes.add(AdapterType.DOUBLE);
              break;
            }
            case READ: {
              isReadyToRead = false;
              isReadyToWrite = false;
              isAvailable = false;
              writableTypes.add(AdapterType.BYTE);
              break;
            }
            case WRITE: {
              isReadyToRead = true;
              isReadyToWrite = false;
              isAvailable = true;
              readableTypes.add(AdapterType.BYTE);
              readableTypes.add(AdapterType.DOUBLE);
              break;
            }
          }
          break;
        }
        case BLOCK_SIZE: {
          blockSize = Integer.parseInt(words[1]);
          break;
        }
        case TABLE_FILE: {
          tableFile = words[1];
          break;
        }
        case TABLE_METHOD: {
          tableMethodType tm = metMap.get(words[1]);
          if (tm == null)
            throw new IOException("Unknown method: " + words[1] + "in file: " + confFile + " at: " + line + " read|write expected");
          switch (tm) {
            case READ: {
              setConfigs(tableFile);  // read table of probabilities
              break;
            }
            case WRITE: {
              if (words.length != 3)
                throw new IOException("Need file name to count probabilities");
              countProb(words[2]);
              writeDecodeConf();
            }
          }
          break;
        }
      }
    }
    configReader.close();
    Logger.writeLn("Configs have been set");
  }

  /**
   * Count probabilities of letters in input file
   * @param inputFileName
   * @throws IOException
   */
  private void countProb(String inputFileName) throws IOException {
    DataInputStream copy = new DataInputStream(new FileInputStream(inputFileName));
    while (copy.available() > 0) {
      byte ch = copy.readByte();
      textLen++;
      if (!probability.containsKey(ch))
        probability.put(ch, 1.0);
      else
        probability.replace(ch, probability.get(ch) + 1);

      segs.putIfAbsent(ch, new Segment());
    }

    copy.close();

    for (Byte key : probability.keySet())
      probability.replace(key, probability.get(key) / textLen);
    Logger.writeLn("Probability have been counted");
  }

  /**
   * Set segments of letters
   */
  private void defineSegments() {
    double l = 0;

    for (Map.Entry<Byte, Segment> entry : segs.entrySet()) {
      entry.getValue().left = l;
      entry.getValue().right = l + probability.get(entry.getKey());
      l = entry.getValue().right;
    }
  }

  /**
   * Write table file
   * @throws IOException
   */
  private void writeDecodeConf() throws IOException {
    BufferedWriter encWriter = new BufferedWriter(new FileWriter(tableFile));

    for (Map.Entry<String, confTypes> entry : configMap.entrySet()) {
      switch (entry.getValue()) {
        case PROBABILITY: {
          for (Map.Entry<Byte, Double> prEntry : probability.entrySet()) {
            encWriter.write(entry.getKey() + delim + prEntry.getKey() + delim + prEntry.getValue() + endl);
          }
          break;
        }
      }
    }
    encWriter.close();
  }

  /**
   * Code data
   * @param data
   * @return Object: coded data
   */
  public Object code(Object data) {
    switch (target) {
      case ENCODE: {
        try {
          return encode((ArrayList<Byte>) data);
        } catch (IOException ex) {
          Logger.writeLn("Encoding Error!");
          Logger.writeErrorLn(ex);
          System.exit(1);
        }
        break;
      }
      case DECODE: {
        try {
          return decode((ArrayList<Double>) data);
        } catch (IOException ex) {
          Logger.writeLn("Decoding Error!");
          Logger.writeErrorLn(ex);
          System.exit(1);
        }
        break;
      }
    }
    return null;
  }

  /**
   * Encode bytes array
   * @param data
   * @return ArrayList<Double>: array of encoded double
   * @throws IOException
   */
  private ArrayList<Double> encode(ArrayList<Byte> data) throws IOException {
    Logger.writeLn("Encoding...");
    defineSegments();

    int size = (int) Math.ceil((double) data.size() / numSeq);
    ArrayList<Double> newData = new ArrayList<>();

    for (int i = 0; i < size; i++) {
      double left = 0, right = 1;
      for (int j = 0; j < numSeq; j++) {
        if (i * numSeq + j >= dataLen)
          break;
        byte ch = data.get(i * numSeq + j);
        double newR = left + (right - left) * segs.get(ch).right;
        double newL = left + (right - left) * segs.get(ch).left;
        right = newR;
        left = newL;
      }
      newData.add((left + right) / 2);
    }
    Logger.writeLn("Encoding finished!!");
    return newData;
  }

  /**
   * Decode from array of doubles
   * @param data
   * @return ArrayList<Byte>: decoded byte array
   * @throws IOException
   */
  private ArrayList<Byte> decode(ArrayList<Double> data) throws IOException {
    Logger.writeLn("Decoding...");
    defineSegments();

    ArrayList<Byte> newData = new ArrayList<>(numSeq * data.size());

    for (int i = 0; i < data.size(); i++) {
      double code = data.get(i);
      for (int j = 0; j < numSeq; j++) {
        for (Map.Entry<Byte, Segment> entry : segs.entrySet())
          if (code >= entry.getValue().left && code < entry.getValue().right) {
            newData.add(numSeq * i + j, entry.getKey());
            code = (code - entry.getValue().left) / (entry.getValue().right - entry.getValue().left);
            break;
          }
      }
    }
    Logger.writeLn("Decoding finished!!!");
    return newData;
  }

  /**
   * Check if consumers are ready to read
   * @return boolean: true if all consumers are ready to read, false otherwise
   */
  private boolean consumersReadyToRead() {
    for (Executor cons : consumers) {
      if (!cons.isAvailable() || !cons.isReadyToRead())
          return false;
    }
    return true;
  }

  /**
   * Check if providers are ready to write
   * @return boolean: true if all providers are ready to write, false otherwise
   */
  private boolean providersReadyToWrite() {
    for (Executor prov : adapters.keySet()) {
      if (!prov.isAvailable() || !prov.isReadyToWrite())
          return false;
    }
    return true;
  }

  /**
   * Set config file to executor
   * @param configFile
   * @throws IOException
   */
  @Override
  public void setConfigFile(String configFile) throws IOException {
    setConfigs(configFile);
  }

  /**
   * Set consumer to executor
   * @param consumer
   * @throws IOException
   */
  @Override
  public void setConsumer(Executor consumer) throws IOException {
    consumers.add(consumer);
    boolean canCommunicate = false;

    for (adapter.AdapterType type : consumer.getReadableTypes()) {
      if (writableTypes.contains(type)) {
        canCommunicate = true;
        switch (type) {
          case BYTE: {
            consumer.setAdapter(this, new ByteAdapterClass(), AdapterType.BYTE);
            break;
          }
          case DOUBLE: {
            consumer.setAdapter(this, new DoubleAdapterClass(), AdapterType.DOUBLE);
            break;
          }
        }
      }
    }

    if (!canCommunicate) {
      throw new IOException("Can't communicate, wrong transporter structure");
    }
  }

  /**
   * Set adapter to executor
   * @param provider
   * @param adapter
   * @param typeOfAdapter
   */
  @Override
  public void setAdapter(Executor provider, Object adapter, AdapterType typeOfAdapter) {
    switch (typeOfAdapter) {
      case DOUBLE: {
        ((DoubleAdapter) adapter).setMetrics(start, blockSize);
        break;
      }
      case BYTE: {
        ((ByteAdapter) adapter).setMetrics(start, blockSize);
        break;
      }
    }
    adapters.put(provider, new Pair<>(adapter, typeOfAdapter));
  }

  /**
   * Get readable types from consumer
   * @return ArrayList<AdapterType>: readable types
   */
  @Override
  public ArrayList<AdapterType> getReadableTypes() {
    return readableTypes;
  }

  /**
   * Set output stream
   * @param output
   */
  @Override
  public void setOutput(DataOutputStream output) {
    outputFile = output;
  }

  /**
   * Set input stream
   * @param input
   */
  @Override
  public void setInput(DataInputStream input) {
    inputFile = input;
  }

  /**
   * Check if executor is ready to write
   * @return boolean: true if ready, false otherwise
   */
  @Override
  public boolean isReadyToWrite() {
    return isReadyToWrite;
  }

  /**
   * Check if executor is ready to read
   * @return boolean: true if ready, false otherwise
   */
  @Override
  public boolean isReadyToRead() {
    return isReadyToRead;
  }

  /**
   * Check if executor is available
   * @return boolean: true if ready, false otherwise
   */
  @Override
  public boolean isAvailable() {
    return isAvailable;
  }

  /**
   * Check if executors finished work
   * @return boolean: true finished, false otherwise
   */
  @Override
  public boolean isOver() {
    return over;
  }

  /**
   * Reader thread
   */
  private void runReader() {
    try {
      while (!Thread.interrupted()) {
        while (inputFile.available() > 0) {
          if (!consumersReadyToRead())
            continue;
          isReadyToWrite = false;
          isAvailable = false;
          // reading by blocks
          byte[] data = new byte[blockSize];
          if (inputFile.available() > blockSize)
            dataLen = blockSize;
          else
            dataLen = inputFile.available();
          int readLen = inputFile.read(data, 0, dataLen);
          ArrayList<Byte> bdata = new ArrayList<>();
          for (int i = 0; i < readLen; i++)
            bdata.add(data[i]);
          dataOut = bdata; // data is ready
          isReadyToWrite = true;
          isAvailable = true;
          Logger.writeLn(Thread.currentThread().getName() + " read block");
          Thread.sleep(sleepTime);
        }
        if (!consumersReadyToRead())
          continue;
        isReadyToWrite = false;
        isAvailable = false;
        over = true;
      }
    } catch (IOException ex) {
      Logger.writeLn("Executor reader run error");
      Logger.writeErrorLn(ex);
    } catch (InterruptedException ex) {
    }
  }

  /**
   * Writer thread
   */
  private void runWriter() {
    sortAdapters();  // sorting all adapters for right output
    try {
      while (!Thread.interrupted()) {
        if (!providersReadyToWrite())
          continue;
        isReadyToRead = false;
        isAvailable = false;
        for (Integer st : starts) {
          Pair<Object, AdapterType> ad = startToAdapter.get(st);
          switch (ad.getValue()) {
            case DOUBLE: {
              DoubleAdapter doubleAdapter = (DoubleAdapter) ad.getKey();
              Double cur;
              while ((cur = doubleAdapter.getNextDouble()) != null) {
                outputFile.writeDouble(cur);
              }
              break;
            }
            case BYTE: {
              ByteAdapter byteAdapter = (ByteAdapter) ad.getKey();
              Byte cur;
              while ((cur = byteAdapter.getNextByte()) != null) {
                outputFile.write(cur);
              }
              break;
            }
          }
        }
        isReadyToRead = true;
        isAvailable = true;
        Logger.writeLn(Thread.currentThread().getName() + " wrote block");
        Thread.sleep(sleepTime);
      }
    } catch (IOException ex) {
      Logger.writeLn("Executor writer run error");
      Logger.writeErrorLn(ex);
    } catch (InterruptedException ex) {
    }
  }

  /**
   * Coder thread
   */
  private void runCoder() {
    sortAdapters();  // sort adapters for right work
    try {
      while (!Thread.interrupted()) {
        if (!providersReadyToWrite())
          continue;
        if (!consumersReadyToRead()) {
          isReadyToRead = false;
          continue;
        }
        int i = 0;
        for (Integer st : starts) {
          Pair<Object, AdapterType> ad = startToAdapter.get(st);
          isReadyToRead = false;
          isReadyToWrite = false;
          isAvailable = false;

          Object currentAdapter = ad.getKey();
          AdapterType type = ad.getValue();

          switch (type) {
            case BYTE: {
              ArrayList<Byte> bdata = new ArrayList<>();
              ByteAdapterClass byteAdapter = (ByteAdapterClass) currentAdapter;
              Byte cur;
              while ((cur = byteAdapter.getNextByte()) != null) {
                bdata.add(cur);
              }
              dataLen = bdata.size();
              dataOut = code(bdata);  // data ready
              break;
            }
            case DOUBLE: {
              ArrayList<Double> ddata = new ArrayList<>();
              DoubleAdapterClass doubleAdapter = (DoubleAdapterClass) currentAdapter;
              Double cur;
              while ((cur = doubleAdapter.getNextDouble()) != null) {
                ddata.add(cur);
              }
              dataLen = ddata.size();
              dataOut = code(ddata);  // data ready
              break;
            }
          }
          isAvailable = true;
          isReadyToWrite = true;
          Logger.writeLn(Thread.currentThread().getName() + " coded input data from " + i++ + " provider");
          Thread.sleep(sleepTime);
        }
        while (!consumersReadyToRead())
          ; // waiting for consumers to handle data
        isReadyToRead = true;
        isReadyToWrite = false;
        isAvailable = true;
        Logger.writeLn(Thread.currentThread().getName() + " coded all input data");
        Thread.sleep(sleepTime);
      }
    } catch (InterruptedException ex) {
    }
  }

  /**
   * Sort adapters by start indices
   */
  private void sortAdapters() {
    for (Pair<Object, AdapterType> ad : adapters.values()) {
      switch (ad.getValue()) {
        case DOUBLE: {
          Integer st = ((DoubleAdapter)ad.getKey()).getStart();
          starts.add(st);
          startToAdapter.put(st, ad);
          break;
        }
        case BYTE: {
          Integer st = ((ByteAdapter)ad.getKey()).getStart();
          starts.add(st);
          startToAdapter.put(st, ad);
          break;
        }
      }
    }
    Collections.sort(starts);
  }

  /**
   * Common thread run function
   */
  @Override
  public void run() {
    switch (target) {
      case READ: {
        runReader();
        break;
      }
      case WRITE: {
        runWriter();
        break;
      }
      default: {
        runCoder();
        break;
      }
    }
  }
}