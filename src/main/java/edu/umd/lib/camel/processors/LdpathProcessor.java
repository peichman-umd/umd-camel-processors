package edu.umd.lib.camel.processors;

import static edu.umd.lib.camel.processors.AddBearerAuthorizationProcessor.USERNAME_HEADER_NAME;
import static edu.umd.lib.fcrepo.LdapRoleLookupService.ADMIN_ROLE;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.HOURS;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.marmotta.ldclient.api.endpoint.Endpoint.PRIORITY_HIGH;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.ws.rs.core.Link;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.RuntimeCamelException;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpHead;
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
import org.apache.marmotta.ldclient.provider.rdf.LinkedDataProvider;
import org.apache.marmotta.ldpath.LDPath;
import org.apache.marmotta.ldpath.backend.linkeddata.LDCacheBackend;
import org.apache.marmotta.ldpath.exception.LDPathParseException;
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
 */
public class LdpathProcessor implements Processor, Serializable {
  private static final long serialVersionUID = 1L;

  private final Logger logger = LoggerFactory.getLogger(LdpathProcessor.class);

  private String query;

  private final LDCachingBackend cachingBackend;

  private final ObjectMapper objectMapper;

  private final ClientConfiguration clientConfig;
  
  private final String NON_RDF_SOURCE_URI = "http://www.w3.org/ns/ldp#NonRDFSource";

  final ProxiedLinkedDataProvider provider;
  
  public LdpathProcessor() throws RepositoryException {
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
    AddBearerAuthorizationProcessor addBearerAuthProcessor = (AddBearerAuthorizationProcessor)
        exchange.getContext().getRegistry().lookupByName("addBearerAuthorization"); 
    final AuthTokenService authTokenService = (AuthTokenService) addBearerAuthProcessor.getAuthTokenService();
    final Message in = exchange.getIn();
    final String issuer = in.getHeader(USERNAME_HEADER_NAME, String.class);
    final String resourceURI = in.getHeader("CamelFcrepoUri", String.class);
    final String containerBasedUri = in.getHeader("CamelHttpUri", String.class);
    
    URL resourceUrl;
    try {
      resourceUrl = new URL(resourceURI);
    } catch(MalformedURLException mue) {
      logger.error("Cannot parse '"+resourceURI+"' as a URL", mue);
      return;
    }
    
    String forwardedProto = resourceUrl.getProtocol();
    String forwardedHost = resourceUrl.getHost();
    int forwardedPort = resourceUrl.getPort();
    if (forwardedPort != -1) {
      // Note: Using "X-Forwarded-Port" header does not seem to be recognized,
      // so appending the port to the host.
      forwardedHost = forwardedHost + ":"+ forwardedPort;
    }

    // Create authorization token
    final Date oneHourHence = Date.from(now().plus(1, HOURS));
    final String authToken = authTokenService.createToken("camel-ldpath", issuer, oneHourHence, ADMIN_ROLE);
    
    final List<Header> headers = new ArrayList<>();
    headers.add(new BasicHeader(AUTHORIZATION, "Bearer " + authToken));
    headers.add(new BasicHeader("X-Forwarded-Host", forwardedHost));
    headers.add(new BasicHeader("X-Forwarded-Proto", forwardedProto));
    for (Header h : headers) {
      logger.info("HTTP client header: {}: {}", h.getName(), h.getValue());
    }
    
    final HttpClient httpClient = HttpClientBuilder.create().setDefaultHeaders(headers).build();
    clientConfig.setHttpClient(httpClient);
    final CacheConfiguration cacheConfig = new CacheConfiguration(clientConfig);
    final LDCacheBackend cacheBackend = new LDCacheBackend(new LDCache(cacheConfig, cachingBackend));
    final LDPath<Value> ldpath = new LDPath<>(cacheBackend);

    // Get the linked data resource url, if this is a non-RDF resource
    String linkedDataResourceUrl = getLinkedDataResourceUrl(authToken, containerBasedUri);
    provider.addLinkedDataMapping(resourceURI, linkedDataResourceUrl);
    
    logger.info("Sending request to {} for {}", containerBasedUri, resourceURI);
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
  
  /**
   * Returns the URL for the Linked Data representation of the given resource URI
   * or the URL of the resource URI, if no other Linked Data representation is found.
   * 
   * For non-RDF resources, this method looks for a "describedBy" link in the headers
   * returned by an HTTP HEAD request, and returns the value, if found.
   * 
   * @param authToken the JWT authorization token used to authenticate to the resource URI
   * @param containerBasedUri the container-based resource URI to get the "describedBy" URL of
   * @return the URL for the Linked Data representation of the given resource URI,
   * or the URL of the resource URI, if no other Linked Data representation is found.
   */
  private String getLinkedDataResourceUrl(String authToken, String containerBasedUri) {
    String result = containerBasedUri;    
    
    Objects.requireNonNull(containerBasedUri);
    
    // Create a new HttpClient with the authorization token
    //
    // Note: Can't use HttpClient from "process" because the "X-Forwarded" headers
    // will cause the URL to be returned with the host in the header.
    HttpClient httpClient = HttpClientBuilder.create().build();
    final HttpHead request = new HttpHead(containerBasedUri);
    request.addHeader(new BasicHeader(AUTHORIZATION, "Bearer " + authToken));
    try {
      final HttpResponse response = httpClient.execute(request);
      logger.debug("Got: {} for HEAD {}", response.getStatusLine().getStatusCode(), containerBasedUri);
      
      Header[] headers = response.getAllHeaders();
      String describedBy = null;
      boolean nonRdfSource = false;
      for (Header h: headers) {
        logger.debug("header:{} ", h);
        if ("link".equalsIgnoreCase(h.getName())) {
          Link link = Link.valueOf(h.getValue());
          String rel = link.getRel();
          
          if ("describedby".equalsIgnoreCase(rel)) {
            describedBy = link.getUri().toString();
          }
          
          if ("type".equalsIgnoreCase(rel)) {
            String type = link.getUri().toString();
            if (type.contains(NON_RDF_SOURCE_URI)) {
              nonRdfSource = true;
            }            
          }
        }
      }
      
      if (nonRdfSource && (describedBy != null)) {
        logger.debug("For non-RDF resource {}, returning LinkedDataResourceUrl from 'describedBy' URI of {}",
            containerBasedUri, describedBy);
        return describedBy;
      }
    } catch(IOException ioe) {
      logger.error("I/O error retrieving HEAD {}", containerBasedUri);
    }
    
    logger.debug("Returning LinkedDataResourceUrl of {}", result);
    return result;
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
* LinkedDataProvider implementation that overrides the (external) request URL to provide an (internal) container-based
* URL for a resource
*/
class ProxiedLinkedDataProvider extends LinkedDataProvider {
  private static final Logger logger = LoggerFactory.getLogger(ProxiedLinkedDataProvider.class);

  public static final String PROVIDER_NAME = "Proxied Linked Data";
  
  private static Map<String, String> linkedDataMap = new HashMap<>();

  @Override
  public String getName() {
    return PROVIDER_NAME;
  }

  public List<String> buildRequestUrl(final String resourceUri, final Endpoint endpoint) {
    if (linkedDataMap.containsKey(resourceUri)) {
      String linkedDataResourceUrl = linkedDataMap.get(resourceUri);
      linkedDataMap.remove(resourceUri);
    
      if (linkedDataResourceUrl != null) {
        return Collections.singletonList(linkedDataResourceUrl);
      }
    }
    return Collections.singletonList(resourceUri);
  }
  
  public void addLinkedDataMapping(String resourceUri, String linkedDataResourceUrl) {
    linkedDataMap.put(resourceUri, linkedDataResourceUrl);
  }
}
