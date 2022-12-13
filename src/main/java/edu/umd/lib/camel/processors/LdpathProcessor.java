package edu.umd.lib.camel.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.lib.camel.utils.LinkHeaders;
import edu.umd.lib.fcrepo.AuthTokenService;
import edu.umd.lib.ldpath.ProxiedLinkedDataProvider;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.RuntimeCamelException;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.marmotta.ldcache.api.LDCachingBackend;
import org.apache.marmotta.ldcache.backend.infinispan.LDCachingInfinispanBackend;
import org.apache.marmotta.ldcache.model.CacheConfiguration;
import org.apache.marmotta.ldcache.services.LDCache;
import org.apache.marmotta.ldclient.api.endpoint.Endpoint;
import org.apache.marmotta.ldclient.api.provider.DataProvider;
import org.apache.marmotta.ldclient.endpoint.rdf.LinkedDataEndpoint;
import org.apache.marmotta.ldclient.model.ClientConfiguration;
import org.apache.marmotta.ldpath.LDPath;
import org.apache.marmotta.ldpath.backend.linkeddata.LDCacheBackend;
import org.apache.marmotta.ldpath.exception.LDPathParseException;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static edu.umd.lib.camel.processors.AddBearerAuthorizationProcessor.USERNAME_HEADER_NAME;
import static edu.umd.lib.fcrepo.LdapRoleLookupService.ADMIN_ROLE;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.HOURS;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.marmotta.ldclient.api.endpoint.Endpoint.PRIORITY_HIGH;

/**
 * Processor that converts RDF triples into JSON, using an Apache Marmotta
 * LDPath template.
 * <p>
 * When processing non-RDF resources (such as a PDF binary file), the
 * processor will use the "describedBy" link in the HTTP headers to
 * retrieve the RDF metadata.
 * <p>
 * Note: This processor is intended for use in a Docker Swarm or Kubernetes
 * stack, where the fcrepo web application is available on an "internal"
 * container-based URL, which is separate from the "external" URL.
 */
public class LdpathProcessor implements Processor, Serializable {
  private static final long serialVersionUID = 1L;

  private final Logger logger = LoggerFactory.getLogger(LdpathProcessor.class);

  /**
   * The HTTP Header Link value to use to identify non-RDF resources
   */
  private static final String NON_RDF_SOURCE_URI = "http://www.w3.org/ns/ldp#NonRDFSource";

  /**
   * The LDPath query for transforming RDF to JSON
   */
  private String query;

  private final LDCachingBackend cachingBackend;

  private final ObjectMapper objectMapper;

  private final ClientConfiguration clientConfig;

  final ProxiedLinkedDataProvider provider;

  public LdpathProcessor() {
    clientConfig = new ClientConfiguration();
    cachingBackend = new LDCachingInfinispanBackend();
    cachingBackend.initialize();

    Endpoint endpoint = new LinkedDataEndpoint();
    endpoint.setType(ProxiedLinkedDataProvider.PROVIDER_NAME);
    endpoint.setPriority(PRIORITY_HIGH);
    clientConfig.addEndpoint(endpoint);

    provider = new ProxiedLinkedDataProvider();

    Set<DataProvider> providers = new HashSet<>();
    providers.add(provider);
    clientConfig.setProviders(providers);

    objectMapper = new ObjectMapper();
  }

  @Override
  public void process(final Exchange exchange) {
    // Retrieve message headers
    final Message in = exchange.getIn();
    final String issuer = in.getHeader(USERNAME_HEADER_NAME, String.class);
    final String resourceURI = in.getHeader("CamelFcrepoUri", String.class);
    final String containerBasedUri = in.getHeader("CamelHttpUri", String.class);

    // Remove the resourceURI from the cache, as it is being updated (and any
    // cache entry is now stale).
    cachingBackend.removeEntry(new URIImpl(resourceURI));

    final String authToken = getAuthToken(exchange, issuer);

    // Set up X-Forwarded headers
    URL resourceUrl;
    try {
      resourceUrl = new URL(resourceURI);
    } catch(MalformedURLException mue) {
      logger.error("Cannot parse '"+resourceURI+"' as a URL", mue);
      return;
    }

    final List<Header> headers = new ArrayList<>();
    headers.add(new BasicHeader(AUTHORIZATION, "Bearer " + authToken));
    headers.add(new BasicHeader("X-Forwarded-Host", getForwardedHost(resourceUrl)));
    headers.add(new BasicHeader("X-Forwarded-Proto", resourceUrl.getProtocol()));
    if (logger.isDebugEnabled()) {
      for (final Header h : headers) {
        logger.debug("HTTP client header: {}: {}", h.getName(), h.getValue());
      }
    }

    // Configure HttpClient for making resource request
    final HttpClient httpClient = HttpClientBuilder.create().setDefaultHeaders(headers).build();
    clientConfig.setHttpClient(httpClient);

    // Get the URL for Linked Data
    String linkedDataResourceUrl = getLinkedDataResourceUrl(authToken, containerBasedUri);
    // Set linkedDataResourceUrl in the provider, so it can be retrieved in the "buildRequestUrl" method
    logger.debug("Adding {} to setLinkedDataMapping with value of {}", resourceURI, linkedDataResourceUrl);
    provider.setLinkedDataMapping(resourceURI, linkedDataResourceUrl);

    // Set up LDPath
    final CacheConfiguration cacheConfig = new CacheConfiguration(clientConfig);
    final LDCacheBackend cacheBackend = new LDCacheBackend(new LDCache(cacheConfig, cachingBackend));
    final LDPath<Value> ldpath = new LDPath<>(cacheBackend);

    logger.debug("Sending request to {} for {}", containerBasedUri, resourceURI);
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

    logger.debug("Removing {} from linkedDataMapKey", resourceURI);
    provider.removeLinkedDataMapping(resourceURI);

    // Force the Exchange to use UTF-8, otherwise Japanese characters are not
    // passed properly to Solr.
    exchange.setProperty(Exchange.CHARSET_NAME, "UTF-8");

    // Add the JSON result to the message
    in.setBody(jsonResult, String.class);
    in.setHeader("Content-Type", "application/json");
  }

  protected String getAuthToken(final Exchange exchange, final String issuer) {
    // Generate an authorization token
    AddBearerAuthorizationProcessor addBearerAuthProcessor = (AddBearerAuthorizationProcessor)
        exchange.getContext().getRegistry().lookupByName("addBearerAuthorization");
    final AuthTokenService authTokenService = addBearerAuthProcessor.getAuthTokenService();
    final Date oneHourHence = Date.from(now().plus(1, HOURS));
    return authTokenService.createToken("camel-ldpath", issuer, oneHourHence, ADMIN_ROLE);
  }

  /**
   * Returns the URL for the Linked Data representation of the given resource URI
   * or the URL of the resource URI, if no other Linked Data representation is found.
   * <p>
   * For non-RDF resources, this method looks for a "describedBy" link in the headers
   * returned by an HTTP HEAD request, and returns the value, if found.
   *
   * @param authToken the JWT authorization token used to authenticate to the resource URI
   * @param containerBasedUri the container-based resource URI to get the "describedBy" URL of
   * @return the URL for the Linked Data representation of the given resource URI,
   * or the URL of the resource URI, if no other Linked Data representation is found.
   */
  protected String getLinkedDataResourceUrl(String authToken, String containerBasedUri) {
    Objects.requireNonNull(containerBasedUri);

    // Create a new HttpClient with the authorization token
    //
    // Note: Can't use HttpClient from "process" because the "X-Forwarded" headers
    // will cause the URL to be returned with the host in the header.
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      final HttpHead request = new HttpHead(containerBasedUri);
      request.addHeader(new BasicHeader(AUTHORIZATION, "Bearer " + authToken));

      final HttpResponse response = httpClient.execute(request);
      logger.debug("Got: {} for HEAD {}", response.getStatusLine().getStatusCode(), containerBasedUri);

      final Header[] responseHeaders = response.getAllHeaders();
      if (logger.isDebugEnabled()) {
        for (Header h : responseHeaders) {
          logger.debug("Response header: {}: {}", h.getName(), h.getValue());
        }
      }
      final LinkHeaders linkHeaders = new LinkHeaders(responseHeaders);
      final String describedBy = linkHeaders.getUriByRel("describedby").toString();
      final boolean nonRdfSource = linkHeaders.contains("type", NON_RDF_SOURCE_URI);

      if (nonRdfSource && (describedBy != null)) {
        logger.debug("For non-RDF resource {}, returning LinkedDataResourceUrl from 'describedBy' URI of {}",
                containerBasedUri, describedBy);
        return describedBy;
      }
    } catch(IOException ioe) {
      logger.error("I/O error retrieving HEAD {}", containerBasedUri);
    }

    logger.debug("Returning LinkedDataResourceUrl of {}", containerBasedUri);
    return containerBasedUri;
  }

  /**
   * Execute the LDPath query, Map
   *
   * @param ldpath the LDPath class performing the query
   * @param uri the "external" resource URI
   * @return a Map containing the results of the query.
   */
  private Map<String, Collection<?>> executeQuery(final LDPath<Value> ldpath, final String uri) throws LDPathParseException {
    final Map<String, Collection<?>> results = ldpath.programQuery(new URIImpl(uri), new StringReader(query));
    for (Map.Entry<String, Collection<?>> entry : results.entrySet()) {
      logger.debug("LDPath result: Key: {} Value: {}", entry.getKey(), entry.getValue());
    }
    return results;
  }

  /**
   * Executes the LDPath query, returning a JSON-formatted string
   *
   * @param ldpath the LDPath class performing the query
   * @param uri the "external" resource URI
   * @return a JSON-formatted string representing the results from the query.
   * @throws LDPathParseException if an LDPath parsing problem occurs
   * @throws JsonProcessingException if a JSON parsing problem occurs
   */
  private String execute(final LDPath<Value> ldpath, final String uri) throws LDPathParseException, JsonProcessingException {
    return objectMapper.writeValueAsString(executeQuery(ldpath, uri));
  }

  private String getForwardedHost(final URL resourceUrl) {
    final int forwardedPort = resourceUrl.getPort();
    if (forwardedPort != -1) {
      // Note: Using "X-Forwarded-Port" header does not seem to be recognized,
      // so appending the port to the host.
      return resourceUrl.getHost() + ":" + forwardedPort;
    } else {
      return resourceUrl.getHost();
    }
  }

  /**
   * Returns the LDPath query
   *
   * @return the LDPath query
   */
  public String getQuery() {
    return query;
  }

  /**
   * Sets the LDPAth query used to convert RDF to JSON
   *
   * @param query the query
   */
  public void setQuery(String query) {
    this.query = query;
  }
}
