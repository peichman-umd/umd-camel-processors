package edu.umd.lib.camel.processors;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;

/**
 * Test utilities
 */
public class TestUtils {
  /**
   * Retrieves the given filename from the classpath resources
   *
   * @param filepath the relative path to the file in the resources
   * @return the contents of the file.
   * @throws IOException if an I/O error occurs.
   */
  public static String getResourceAsString(final String filepath) throws IOException {
    final InputStream resource = Thread.currentThread().getContextClassLoader().getResourceAsStream(filepath);
    assert resource != null;
    return IOUtils.toString(resource, Charset.forName("UTF-8"));
  }
}