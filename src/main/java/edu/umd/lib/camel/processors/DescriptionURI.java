package edu.umd.lib.camel.processors;

import edu.umd.lib.camel.utils.LinkHeaders;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.util.List;

public class DescriptionURI implements Processor, Serializable {
  private static final Logger logger = LoggerFactory.getLogger(DescriptionURI.class);

  public static final String DESCRIBED_BY_HEADER = "DescribedBy";

  @Override
  public void process(Exchange exchange) {
    final Message in = exchange.getIn();
    final List<?> incomingLinkHeaders = in.getHeader("Link", List.class);

    if (logger.isDebugEnabled()) {
      for (final Object h : incomingLinkHeaders) {
        logger.debug("Incoming Link header: {}", h);
      }
    }

    final LinkHeaders linkHeaders = new LinkHeaders(incomingLinkHeaders);

    final URI describedByUri = linkHeaders.getUriByRel("describedby");
    if (describedByUri != null) {
      in.setHeader(DESCRIBED_BY_HEADER, describedByUri.toString());
    }
  }
}
