package edu.umd.lib.camel.processors;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

/**
 * This test demonstrates how to test a Processor using a Camel
 * Exchange object, instead of relying on routes and ProducerTemplates.
 *
 * This code provides the same tests as LdPathProcessorRouteTest
 */
public class LdpathProcessorExchangeTest extends CamelTestSupport {

  protected final String uri = "http://localhost:8080/rest/af/c6/d8/20/afc6d820-427a-4932-9df5-3eb002958fd2";

  @Test
  public void testSimpleLdpathProcessor() throws Exception {
    CamelContext ctx = new DefaultCamelContext();
    Exchange exchange = new DefaultExchange(ctx);

    LdpathProcessor processor = new TestLdpathProcessor();
    processor.setQuery(TestUtils.getResourceAsString("simpleProgram.ldpath"));

    Message in = exchange.getIn();
    in.setHeader("CamelFcrepoUri", uri);
    in.setBody("");

    processor.process(exchange);

    assertEquals("{\"id\":[\"" + uri + "\"]}", in.getBody());
  }

  @Test
  public void testComplexLdpathProcessor() throws Exception {
    CamelContext ctx = new DefaultCamelContext();
    Exchange exchange = new DefaultExchange(ctx);

    LdpathProcessor processor = new TestLdpathProcessor();
    processor.setQuery(TestUtils.getResourceAsString("complexProgram.ldpath"));

    Message in = exchange.getIn();
    in.setHeader("CamelFcrepoUri", uri);
    in.setBody("");

    processor.process(exchange);

    final String jsonResult = exchange.getIn().getBody(String.class);
    assertFalse(jsonResult.isEmpty());
    assertTrue(jsonResult.startsWith("{"));
    assertTrue(jsonResult.endsWith("}"));
  }
}
