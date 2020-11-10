package edu.umd.lib.camel.processors;

import static edu.umd.lib.camel.processors.AddBearerAuthorizationProcessor.USERNAME_HEADER_NAME;
import static edu.umd.lib.fcrepo.LdapRoleLookupService.ADMIN_ROLE;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.HOURS;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.marmotta.ldclient.api.endpoint.Endpoint.PRIORITY_HIGH;

import java.io.Serializable;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.RuntimeCamelException;
import org.apache.http.Header;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.marmotta.commons.http.ContentType;
import org.apache.marmotta.ldcache.api.LDCachingBackend;
import org.apache.marmotta.ldcache.backend.infinispan.LDCachingInfinispanBackend;
import org.apache.marmotta.ldcache.model.CacheConfiguration;
import org.apache.marmotta.ldcache.services.LDCache;
import org.apache.marmotta.ldclient.api.endpoint.Endpoint;
import org.apache.marmotta.ldclient.api.provider.DataProvider;
import org.apache.marmotta.ldclient.endpoint.rdf.LinkedDataEndpoint;
import org.apache.marmotta.ldclient.model.ClientConfiguration;
import org.apache.marmotta.ldclient.provider.rdf.LinkedDataProvider;
import org.apache.marmotta.ldpath.LDPath;
import org.apache.marmotta.ldpath.backend.linkeddata.LDCacheBackend;
import org.apache.marmotta.ldpath.exception.LDPathParseException;
import org.jasig.cas.client.util.URIBuilder;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.umd.lib.fcrepo.AuthTokenService;

/**
 * Ldoath Processor
 * 
 * This class expects a "REPO_INTERNAL_URL" environment variable to exist,
 * indicating the (container-based) URL to the frepo repository.
 */
public class LdpathProcessor implements Processor, Serializable {
  private static final long serialVersionUID = 1L;

  private final Logger logger = LoggerFactory.getLogger(LdpathProcessor.class);

  private String query;

  private final LDCachingBackend cachingBackend;

  private final ObjectMapper objectMapper;

  private final ClientConfiguration clientConfig;

  public LdpathProcessor() throws RepositoryException {
    clientConfig = new ClientConfiguration();
    cachingBackend = new LDCachingInfinispanBackend();
    //cachingBackend = new LDCachingFileBackend(new File("/tmp"));
    cachingBackend.initialize();
    
    Endpoint endpoint = new LinkedDataEndpoint();
    endpoint.addContentType(new ContentType("application", "n-triples", 1.0));
    endpoint.setType(ProxiedLinkedDataProvider.PROVIDER_NAME);
    endpoint.setPriority(PRIORITY_HIGH);
    clientConfig.addEndpoint(endpoint);

    final ProxiedLinkedDataProvider provider = new ProxiedLinkedDataProvider();
    String repoInternalUrl = System.getenv("REPO_INTERNAL_URL");
    if (repoInternalUrl == null) {
      repoInternalUrl = "http://repository:8080/rest";
      logger.warn("REPO_INTERNAL_URL environment variable not set. Using default of '{}", repoInternalUrl);
    }
       
    provider.setRealUrl(repoInternalUrl);
    
    Set<DataProvider> providers = new HashSet<>();
    providers.add(provider);
    clientConfig.setProviders(providers);

    objectMapper = new ObjectMapper();
  }

  @Override
  public void process(final Exchange exchange) {
    AddBearerAuthorizationProcessor addBearerAuthProcessor = (AddBearerAuthorizationProcessor)
        exchange.getContext().getRegistry().lookupByName("addBearerAuthorization"); 
    final AuthTokenService authTokenService = (AuthTokenService) addBearerAuthProcessor.getAuthTokenService();
    final Message in = exchange.getIn();
    final String issuer = in.getHeader(USERNAME_HEADER_NAME, String.class);
    final Date oneHourHence = Date.from(now().plus(1, HOURS));
    final String authToken = authTokenService.createToken("camel-ldpath", issuer, oneHourHence, ADMIN_ROLE);

    final List<Header> headers = new ArrayList<>();
    headers.add(new BasicHeader(AUTHORIZATION, "Bearer " + authToken));
//    headers.add(new BasicHeader("X-Forwarded-Host", "localhost:8080"));
//    headers.add(new BasicHeader("X-Forwarded-Proto", "http"));
    for (Header h : headers) {
      logger.info("HTTP client header: {}: {}", h.getName(), h.getValue());
    }
    final HttpClient httpClient = HttpClientBuilder.create().setDefaultHeaders(headers).build();
    clientConfig.setHttpClient(httpClient);
    final CacheConfiguration cacheConfig = new CacheConfiguration(clientConfig);
    final LDCacheBackend cacheBackend = new LDCacheBackend(new LDCache(cacheConfig, cachingBackend));
    final LDPath<Value> ldpath = new LDPath<>(cacheBackend);

    final String resourceURI = in.getHeader("CamelFcrepoUri", String.class);
    final String uri = in.getHeader("CamelHttpUri", String.class);
    logger.info("Sending request to {} for {}", uri, resourceURI);
    logger.debug("LDPath query: {}", query);
    String jsonResult;
    try {
      jsonResult = execute(ldpath, resourceURI);
    } catch (LDPathParseException e) {
      logger.error("LDPath parse error: {}", e.getMessage());
      throw new RuntimeCamelException("LDPath parse error", e);
    } catch (JsonProcessingException e) {
      logger.error("JSON processing error: {}", e.getMessage());
      throw new RuntimeCamelException("JSON processing error", e);
    }
    assert jsonResult != null;
    assert !jsonResult.isEmpty();
    in.setBody(jsonResult, String.class);
    in.setHeader("Content-Type", "application/json");
  }

  Map<String, Collection<?>> executeQuery(final LDPath<Value> ldpath, final String uri) throws LDPathParseException {
    final Map<String, Collection<?>> results = ldpath.programQuery(new URIImpl(uri), new StringReader(query));
    for (Map.Entry<String, Collection<?>> entry : results.entrySet()) {
      logger.debug("LDPath result: Key: {} Value: {}", entry.getKey(), entry.getValue());
    }
    return results;
  }

  String execute(final LDPath<Value> ldpath, final String uri) throws LDPathParseException, JsonProcessingException {
    return objectMapper.writeValueAsString(executeQuery(ldpath, uri));
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }
}

/**
 * LinkedDataProvider implementation that overrides the request URL to provide a container-based internal URL
 * for a resource
 */
class ProxiedLinkedDataProvider extends LinkedDataProvider {
  private static final Logger logger = LoggerFactory.getLogger(ProxiedLinkedDataProvider.class);

  public static final String PROVIDER_NAME = "Proxied Linked Data";

  private String realUrl;

  @Override
  public String getName() {
    return PROVIDER_NAME;
  }

  @Override
  public List<String> buildRequestUrl(String resourceUri, Endpoint endpoint) {
    try {
      final URL resourceURL = new URL(resourceUri);
    
      final String realURL = new URIBuilder(realUrl)
          .setPath(resourceURL.getPath())
          .setEncodedQuery(resourceURL.getQuery())
          .build()
          .toString();
      logger.debug("Real URL is {}", realURL);
      return Collections.singletonList(realURL);
    } catch (MalformedURLException e) {
      logger.error("Malformed URL: {}", resourceUri);
      throw new IllegalArgumentException("Malformed URL: " + resourceUri, e);
    }
  }


  /**
   * Sets the actual (container-based) URL to use when querying fcrepo
   * @param url the actual (container-based) URL to use when querying fcrepo
   */
  public void setRealUrl(String url) {
    realUrl = url;
  }
  
  public String getRealUrl() {
    return realUrl;
  }
}
