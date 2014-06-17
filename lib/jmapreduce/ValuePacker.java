package org.fingertap.jmapreduce;

import java.lang.ClassNotFoundException;
import java.io.UnsupportedEncodingException;

import java.net.URLEncoder;
import java.net.URLDecoder;

import org.msgpack.MessagePack;

public class ValuePacker {
  private static String delimiter = Character.toString('\u0000');
  
  public static String pack(Object value) throws UnsupportedEncodingException {
    if (value instanceof String)
      return value.toString();
    
    String raw = new String(MessagePack.pack(value));
    return value.getClass().getName() + delimiter + URLEncoder.encode(raw, "UTF-8");
  }
  
    public static Object unpack(String value) throws ClassNotFoundException, UnsupportedEncodingException {
    if (value.indexOf(delimiter) == -1)
      return value;
    
    String[] tokens = value.split(delimiter);
    if (tokens.length != 2)
      return value;

    Class rowClass = null;
    try {
      rowClass = Class.forName(tokens[0]);
    } catch (ClassNotFoundException e) {
      System.err.println("ClassNotFoundException for string: " + value);
      // assume we could not find class because the original string had delimiter 
      // and we have not yet inserted the class we should use, return original string
      return value;
    }

    String raw = URLDecoder.decode(tokens[1], "UTF-8");
    return MessagePack.unpack(raw.getBytes(), Class.forName(tokens[0]));
  }
}
