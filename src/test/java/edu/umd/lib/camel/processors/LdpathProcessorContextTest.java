package edu.umd.lib.camel.processors;

import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultExchange;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.openrdf.repository.RepositoryException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LdpathProcessorContextTest {

  @EndpointInject(uri = "mock:result")
  protected MockEndpoint resultEndpoint;

  @Produce(uri = "direct:simple")
  protected ProducerTemplate simpleStart;

  @Produce(uri = "direct:complex")
  protected ProducerTemplate complexStart;

  private final String uri = "http://localhost:8080/rest/af/c6/d8/20/afc6d820-427a-4932-9df5-3eb002958fd2";

  private String getResourceAsString(final String name) throws IOException {
    final InputStream resource = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    assert resource != null;
    return IOUtils.toString(resource, Charset.forName("UTF-8"));
  }
  
  /**
   * Overrides LdpathProcessor to simplify authorization token and linkedDataResourceUrl creation
   */
  class TestLdpathProcessor extends LdpathProcessor {
    public TestLdpathProcessor() throws RepositoryException {
      super();
    }
    
    @Override
    protected String getAuthToken(final Exchange exchange, final String issuer) {
      return "abcd-1234";
    }
    
    @Override
    protected String getLinkedDataResourceUrl(String authToken, String containerBasedUri) {
      return containerBasedUri;
    }
  }
  
  protected LdpathProcessor simpleLdpathProcessor() throws RepositoryException, IOException {
    LdpathProcessor processor = new TestLdpathProcessor();
    processor.setQuery(getResourceAsString("simpleProgram.ldpath"));
    return processor;
  }

  protected LdpathProcessor complexLdpathProcessor() throws RepositoryException, IOException {
    LdpathProcessor processor = new TestLdpathProcessor();
    processor.setQuery(getResourceAsString("complexProgram.ldpath"));
    return processor;
  }
  
  @Test
  public void testSimpleLdpathProcessor() throws Exception {
    CamelContext ctx = new DefaultCamelContext();
    Exchange exchange = new DefaultExchange(ctx);
  
    LdpathProcessor processor = simpleLdpathProcessor();

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
    
  
    LdpathProcessor processor = complexLdpathProcessor();
    
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
