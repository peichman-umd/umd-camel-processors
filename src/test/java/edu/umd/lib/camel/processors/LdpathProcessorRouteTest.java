package edu.umd.lib.camel.processors;

import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultProducerTemplate;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Before;
import org.junit.Test;

/**
 * This test demonstrates how to test a Processor using a Camel
 * route, instead of using an Exchange object. Using an Exchange
 * object is generally preferred, as it is simpler.
 *
 * This code provides the same tests as LdPathProcessorExchangeTest
 */

public class LdpathProcessorRouteTest extends CamelTestSupport {
  protected LdpathProcessor simpleProcessor;
  protected LdpathProcessor complexProcessor;

  protected final String uri = "http://localhost:8080/rest/af/c6/d8/20/afc6d820-427a-4932-9df5-3eb002958fd2";

  @Override
  @Before
  public void setUp() throws Exception {
    simpleProcessor = new TestLdpathProcessor();
    simpleProcessor.setQuery(TestUtils.getResourceAsString("simpleProgram.ldpath"));

    complexProcessor = new TestLdpathProcessor();
    complexProcessor.setQuery(TestUtils.getResourceAsString("complexProgram.ldpath"));

    super.setUp();
  }

  @Override
  protected RoutesBuilder createRouteBuilder() throws Exception {
    return new RouteBuilder() {
      @Override
      public void configure() throws Exception {
        // "simple" processor route
        from("direct:simple")
        .process(simpleProcessor)
        .to("mock:result");

        // "complex" processor route
        from("direct:complex")
        .process(complexProcessor)
        .to("mock:result");
      }
    };
  }

  @Test
  public void testSimpleLdpathProcessor() throws Exception {
    MockEndpoint resultEndpoint = context.getEndpoint("mock:result", MockEndpoint.class);
    resultEndpoint.expectedBodiesReceived("{\"id\":[\"" + uri + "\"]}");

    ProducerTemplate simpleStart = DefaultProducerTemplate.newInstance(context, "direct:simple");

    simpleStart.start();
    simpleStart.sendBodyAndHeader("", "CamelFcrepoUri", uri);
    simpleStart.stop();

    resultEndpoint.assertIsSatisfied();
  }

  @Test
  public void testComplexLdpathProcessor() throws Exception {
    MockEndpoint resultEndpoint = context.getEndpoint("mock:result", MockEndpoint.class);
    resultEndpoint.setExpectedCount(1);

    ProducerTemplate complexStart = DefaultProducerTemplate.newInstance(context, "direct:complex");
    complexStart.start();
    complexStart.sendBodyAndHeader("", "CamelFcrepoUri", uri);
    complexStart.stop();

    resultEndpoint.assertIsSatisfied();

    final Exchange exchange = resultEndpoint.getExchanges().get(0);
    final String jsonResult = exchange.getIn().getBody(String.class);
    assertFalse(jsonResult.isEmpty());
    assertTrue(jsonResult.startsWith("{"));
    assertTrue(jsonResult.endsWith("}"));
  }
}
