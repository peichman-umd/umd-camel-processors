package edu.umd.lib.camel.processors;

import edu.umd.lib.fcrepo.AuthTokenService;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;

import java.util.Date;

import static edu.umd.lib.fcrepo.LdapRoleLookupService.ADMIN_ROLE;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.HOURS;

public class AddBearerAuthorizationProcessor implements Processor {
  public final static String SUBJECT = "camel";

  public final static String USERNAME_HEADER_NAME = "CamelFcrepoUser";

  private AuthTokenService authTokenService;

  public AddBearerAuthorizationProcessor() {}

  public AuthTokenService getAuthTokenService() {
    return authTokenService;
  }

  public void setAuthTokenService(AuthTokenService authTokenService) {
    this.authTokenService = authTokenService;
  }

  @Override
  public void process(Exchange exchange) throws Exception {
    final Message in = exchange.getIn();
    final String issuer = in.getHeader(USERNAME_HEADER_NAME, String.class);
    final Date expirationDate = Date.from(now().plus(1, HOURS));
    final String token = authTokenService.createToken(SUBJECT, issuer, expirationDate, ADMIN_ROLE);
    in.setHeader("Authorization", "Bearer " + token);
  }
}
