package edu.umd.lib.camel.processors;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.protocol.BasicHttpContext;
import org.junit.Test;

import static org.apache.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertTrue;

public class Http308RedirectTest {
  @Test
  public void testHttp308Redirect() throws ProtocolException {
    final RedirectStrategy redirectStrategy = new DefaultRedirectStrategy();
    final HttpRequest req = new HttpGet("http://vocab.lib.umd.edu/form");
    final HttpResponse res = new BasicHttpResponse(HTTP_1_1, 308, "Permanent Redirect");
    final BasicHttpContext ctx = new BasicHttpContext();

    assertTrue(redirectStrategy.isRedirected(req, res, ctx));
  }
}
