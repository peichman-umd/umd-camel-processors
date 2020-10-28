package edu.umd.lib.camel.processors;

import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.CamelSpringBootRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@ContextConfiguration
@RunWith(CamelSpringBootRunner.class)
public class LdpathProcessorContextTest {

  @EndpointInject(uri = "mock:result")
  protected MockEndpoint resultEndpoint;

  @Produce(uri = "direct:simple")
  protected ProducerTemplate simpleStart;

  @Produce(uri = "direct:complex")
  protected ProducerTemplate complexStart;

  private final String uri = "http://localhost:8080/rest/af/c6/d8/20/afc6d820-427a-4932-9df5-3eb002958fd2";

  @DirtiesContext
  @Test
  public void testSimpleLdpathProcessor() throws InterruptedException {
    resultEndpoint.expectedBodiesReceived("{\"id\":[\"" + uri + "\"]}");

    simpleStart.sendBodyAndHeader("", "CamelFcrepoUri", uri);

    resultEndpoint.assertIsSatisfied();
  }

  @DirtiesContext
  @Test
  public void testComplexLdpathProcessor() throws InterruptedException {
    resultEndpoint.setExpectedCount(1);

    complexStart.sendBodyAndHeader("", "CamelFcrepoUri", uri);

    resultEndpoint.assertIsSatisfied();

    final Exchange exchange = resultEndpoint.getExchanges().get(0);
    final String jsonResult = exchange.getIn().getBody(String.class);
    assertFalse(jsonResult.isEmpty());
    assertTrue(jsonResult.startsWith("{"));
    assertTrue(jsonResult.endsWith("}"));
  }
}

