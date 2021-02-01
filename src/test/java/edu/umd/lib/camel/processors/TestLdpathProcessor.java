package edu.umd.lib.camel.processors;

import org.apache.camel.Exchange;
import org.openrdf.repository.RepositoryException;

/**
* Overrides LdpathProcessor to stub out authorization token and
* linkedDataResourceUrl creation
*/
public class TestLdpathProcessor extends LdpathProcessor {
  private static final long serialVersionUID = 1L;

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