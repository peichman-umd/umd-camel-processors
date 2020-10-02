package edu.umd.lib.camel.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.marmotta.ldpath.LDPath;
import org.apache.marmotta.ldpath.backend.linkeddata.LDCacheBackend;
import org.apache.marmotta.ldpath.exception.LDPathParseException;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.StringReader;
import java.util.Collection;
import java.util.Map;

public class LdpathProcessor implements Processor, Serializable {
  private final Logger logger = LoggerFactory.getLogger(LdpathProcessor.class);

  private String query;

  private final LDPath<Value> ldpath;

  private final ObjectMapper objectMapper;

  public LdpathProcessor() {
    objectMapper = new ObjectMapper();
    ldpath = new LDPath<>(new LDCacheBackend());
  }

  @Override
  public void process(final Exchange exchange) throws LDPathParseException, JsonProcessingException {
    final Message in = exchange.getIn();
    final String uri = in.getHeader("CamelFcrepoUri", String.class);
    logger.debug("Executing LDPath on {}", uri);
    logger.trace(query);
    String jsonResult;
    try {
      jsonResult = execute(uri);
    } catch (LDPathParseException e) {
      logger.error("LDPath parse error: {}", e.getMessage());
      throw e;
    } catch (JsonProcessingException e) {
      logger.error("JSON processing error: {}", e.getMessage());
      throw e;
    }
    assert jsonResult != null;
    assert !jsonResult.isEmpty();
    in.setBody(jsonResult);
    in.setHeader("Content-Type", "application/json");
  }

  Map<String, Collection<?>> executeQuery(final String uri) throws LDPathParseException {
    final Map<String, Collection<?>> results = ldpath.programQuery(new URIImpl(uri), new StringReader(query));
    for (Map.Entry<String, Collection<?>> entry : results.entrySet()) {
      logger.debug("LDPath result: Key: {} Value: {}", entry.getKey(), entry.getValue());
    }
    return results;
  }

  String execute(final String uri) throws LDPathParseException, JsonProcessingException {
    return objectMapper.writeValueAsString(executeQuery(uri));
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }
}
