package net.kanstren.kafka.influx;

/**
 * @author Teemu Kanstren.
 */
public class Log {
  private final String parent;

  public Log(String parent) {
    this.parent = parent;
  }

  public Log(Class parent) {
    this.parent = parent.toString();
  }

  public void d(String msg) {
    System.out.println(msg);
  }

  private String formatMsg(String msg) {
    return parent+": "+msg;
  }
}
