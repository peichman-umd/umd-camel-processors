package edu.umd.lib.camel.processors;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Link;
import java.io.Serializable;
import java.util.List;

public class DescriptionURI implements Processor, Serializable {
  private static final Logger logger = LoggerFactory.getLogger(DescriptionURI.class);

  public static final String DESCRIBED_BY_HEADER = "DescribedBy";

  @Override
  public void process(Exchange exchange) {
    final Message in = exchange.getIn();
    final List<?> linkHeaders = in.getHeader("Link", List.class);
    logger.debug("Link headers: {}", linkHeaders);
    for (final Object h : linkHeaders) {
      logger.debug("Parsing header: {}", h);
      final Link link = Link.valueOf((String) h);
      logger.debug("URI = {}", link.getUri());
      logger.debug("Rel = {}", link.getRel());
      if (link.getRel().equals("describedby")) {
        in.setHeader(DESCRIBED_BY_HEADER, link.getUri().toString());
        break;
      }
    }
  }
}
