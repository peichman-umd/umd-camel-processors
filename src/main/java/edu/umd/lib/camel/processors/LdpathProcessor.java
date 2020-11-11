package edu.umd.lib.camel.processors;

import static edu.umd.lib.camel.processors.AddBearerAuthorizationProcessor.USERNAME_HEADER_NAME;
import static edu.umd.lib.fcrepo.LdapRoleLookupService.ADMIN_ROLE;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.HOURS;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.marmotta.ldclient.api.endpoint.Endpoint.PRIORITY_HIGH;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.marmotta.commons.sesame.model.ModelCommons;
import org.apache.marmotta.ldcache.api.LDCachingBackend;
import org.apache.marmotta.ldcache.backend.infinispan.LDCachingInfinispanBackend;
import org.apache.marmotta.ldcache.model.CacheConfiguration;
import org.apache.marmotta.ldcache.services.LDCache;
import org.apache.marmotta.ldclient.api.endpoint.Endpoint;
import org.apache.marmotta.ldclient.api.provider.DataProvider;
import org.apache.marmotta.ldclient.endpoint.rdf.LinkedDataEndpoint;
import org.apache.marmotta.ldclient.exception.DataRetrievalException;
import org.apache.marmotta.ldclient.model.ClientConfiguration;
import org.apache.marmotta.ldclient.provider.rdf.LinkedDataProvider;
import org.apache.marmotta.ldpath.LDPath;
import org.apache.marmotta.ldpath.backend.linkeddata.LDCacheBackend;
import org.apache.marmotta.ldpath.exception.LDPathParseException;
import org.jasig.cas.client.util.URIBuilder;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParserRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.umd.lib.fcrepo.AuthTokenService;
import javolution.util.function.Predicate;

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
  
  private final String repoInternalUrl;
  
  private final String NON_RDF_SOURCE_URI = "http://www.w3.org/ns/ldp#NonRDFSource";

  private Endpoint endpoint;
  
  public LdpathProcessor() throws RepositoryException {
    clientConfig = new ClientConfiguration();
    cachingBackend = new LDCachingInfinispanBackend();
    //cachingBackend = new LDCachingFileBackend(new File("/tmp"));
    cachingBackend.initialize();
    
    endpoint = new LinkedDataEndpoint();
//    endpoint.addContentType(new ContentType("application", "n-triples", 1.0));
    endpoint.setType(ProxiedLinkedDataProvider.PROVIDER_NAME);
    endpoint.setPriority(PRIORITY_HIGH);
    clientConfig.addEndpoint(endpoint);

    final ProxiedLinkedDataProvider provider = new ProxiedLinkedDataProvider();
    String repoInternalUrl = System.getenv("REPO_INTERNAL_URL");
    if (repoInternalUrl == null) {
      repoInternalUrl = "http://repository:8080/rest";
      logger.warn("REPO_INTERNAL_URL environment variable not set. Using default of '{}", repoInternalUrl);
    }
    this.repoInternalUrl = repoInternalUrl;
       
//    provider.setRealUrl(repoInternalUrl);
//    
    Set<DataProvider> providers = new HashSet<>();
    providers.add(provider);
    clientConfig.setProviders(providers);

    objectMapper = new ObjectMapper();
  }

  
  private String getLinkedDataResourceUrl(String authToken, String resourceUri) {
    String result = resourceUri;

    final URL resourceUrl;
    try {
      resourceUrl = new URL(resourceUri);
    } catch (MalformedURLException e) {
      logger.error("Malformed URL: {}", resourceUri);
      throw new IllegalArgumentException("Malformed URL: " + resourceUri, e);
    }
          
    final String realUrl = new URIBuilder(this.repoInternalUrl)
        .setPath(resourceUrl.getPath())
        .setEncodedQuery(resourceUrl.getQuery())
        .build()
        .toString();
    logger.debug("Real URL is {}", realUrl);
    
    
    Objects.requireNonNull(realUrl);
      
    HttpClient httpClient = HttpClientBuilder.create().build();
    final HttpHead request = new HttpHead(realUrl);
    request.addHeader(new BasicHeader(AUTHORIZATION, "Bearer " + authToken));
    try {
      final HttpResponse response = httpClient.execute(request);
      logger.debug("Got: {} for HEAD {}", response.getStatusLine().getStatusCode(), resourceUri);
      
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
        logger.debug("Returning LinkedDataResourceUrl from 'describedBy' URI of {}", describedBy);
        return describedBy;
      }
      
    } catch(IOException ioe) {
      logger.error("I/O error retrieving HEAD {}", realUrl);
    }
    
    logger.debug("Returned LinkedDataResourceUrl of {}", result);
    return result;
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
    
    String linkedDataResourceUrl = getLinkedDataResourceUrl(authToken, resourceURI);
    endpoint.setProperty(resourceURI, linkedDataResourceUrl);
    
    logger.info("Sending request to {} for {}", uri, resourceURI);
    logger.debug("LDPath query: {}", query);
    String jsonResult;
    try {
      jsonResult = execute(ldpath, resourceURI);
//      jsonResult = execute(ldpath, linkedDataResourceUrl);
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
* LinkedDataProvider implementation that overrides the (external) request URL to provide an (internal) container-based
* URL for a resource
*/
class ProxiedLinkedDataProvider extends LinkedDataProvider {
  private static final Logger logger = LoggerFactory.getLogger(ProxiedLinkedDataProvider.class);

  public static final String PROVIDER_NAME = "Proxied Linked Data";

  @Override
  public String getName() {
    return PROVIDER_NAME;
  }

  public List<String> buildRequestUrl(final String resourceUri, final Endpoint endpoint) {
    String linkedDataResourceUrl = endpoint.getProperty(resourceUri);
    if (linkedDataResourceUrl != null) {
      return Collections.singletonList(linkedDataResourceUrl);
    }
    return Collections.singletonList(resourceUri);
  }

  /**
   * Overrides the default implementation to:
   * 
   * 1) Accept all RDF statements returned in the response
   * 2) Replace the "Subject" in all RDF statements with the given resourceUri
   * 
   * @param resourceUri the URI to use as the Subject for all statements
   * @param containerBasedUrl the container-based "describedBy" URL actually used to make the request
   * @param triples the Model that will be populated with RDF triples
   * @param in the InputStream containing the response
   * @param contentType the content type of the response
   */
  @Override
  public List<String> parseResponse(String resourceUri, String containerBasedUrl, Model triples, InputStream in,
      String contentType) throws DataRetrievalException {
    RDFFormat format = RDFParserRegistry.getInstance().getFileFormatForMIMEType(contentType, RDFFormat.RDFXML);
  
    try {
        ModelCommons.add(triples, in, resourceUri, format, new Predicate<Statement>() {
            @Override
            public boolean test(Statement param) {
              // Assume all statements are valid
              logger.debug("Subject: {}, Predicate: {}, Object: {}", param.getSubject(), param.getPredicate(), param.getObject());
              return true;
  //              return StringUtils.equals(param.getSubject().stringValue(), resourceUri);
            }
        });
  
        // Swap the Subject, which should be the similar to the requestUrl,
        // with the resourceUri. The following replaces the Subjects in all
        // statements.
        Iterator<Statement> itStatements = triples.iterator();
        List<Statement> proxiedStatements = new ArrayList<>();
        Resource proxiedResource = new URIImpl(resourceUri);
        while(itStatements.hasNext()) {
          Statement statement = itStatements.next();
          Statement proxiedStatement = new StatementImpl(proxiedResource, statement.getPredicate(), statement.getObject());
          proxiedStatements.add(proxiedStatement);
        }
        triples.clear();
        triples.addAll(proxiedStatements);
        
        return Collections.emptyList();
    } catch (RDFParseException e) {
        throw new DataRetrievalException("parse error while trying to parse remote RDF content",e);
    } catch (IOException e) {
        throw new DataRetrievalException("I/O error while trying to read remote RDF content",e);
    }
  }
  
}

///**
// * LinkedDataProvider implementation that overrides the request URL to provide a container-based internal URL
// * for a resource
// */
//class ProxiedLinkedDataProvider extends LinkedDataProvider {
//  private static final Logger logger = LoggerFactory.getLogger(ProxiedLinkedDataProvider.class);
//
//  public static final String PROVIDER_NAME = "Proxied Linked Data";
//  private String authToken = null;
//
//  private String realUrl;
//
//  @Override
//  public String getName() {
//    return PROVIDER_NAME;
//  }
//  
//  private final String NON_RDF_SOURCE_URI = "http://www.w3.org/ns/ldp#NonRDFSource";
//  
////
////  @Override
////  public List<String> buildRequestUrl(String resourceUri, Endpoint endpoint) {
////    try {
////      final URL resourceURL = new URL(resourceUri);
////    
////      final String realURL = new URIBuilder(realUrl)
////          .setPath(resourceURL.getPath())
////          .setEncodedQuery(resourceURL.getQuery())
////          .build()
////          .toString();
////      logger.debug("Real URL is {}", realURL);
////      return Collections.singletonList(realURL);
////    } catch (MalformedURLException e) {
////      logger.error("Malformed URL: {}", resourceUri);
////      throw new IllegalArgumentException("Malformed URL: " + resourceUri, e);
////    }
////  }
////
//  /**
//   * Sets the actual (container-based) URL to use when querying fcrepo
//   * @param url the actual (container-based) URL to use when querying fcrepo
//   */
//  public void setRealUrl(String url) {
//    realUrl = url;
//  }
//  
//  public String getRealUrl() {
//    return realUrl;
//  }
//  
//  public void setAuthToken(String authToken) {
//    this.authToken = authToken;
//  }
//  
//  /*
//   * Return the describedBy URL for NonRdfSource resources. Return the resourseUri otherwise.
//   */
//  @Override
//  public List<String> buildRequestUrl(final String resourceUri, final Endpoint endpoint) {
//    logger.debug("Processing: " + resourceUri);
//    final URL resourceURL;
//    final String realURL;
//    
//    try {
//      resourceURL = new URL(resourceUri);
//    
//      realURL = new URIBuilder(realUrl)
//          .setPath(resourceURL.getPath())
//          .setEncodedQuery(resourceURL.getQuery())
//          .build()
//          .toString();
//      logger.debug("Real URL is {}", realURL);
////      return Collections.singletonList(realURL);
//    } catch (MalformedURLException e) {
//      logger.error("Malformed URL: {}", resourceUri);
//      throw new IllegalArgumentException("Malformed URL: " + resourceUri, e);
//    }
//    
//
//    Objects.requireNonNull(realUrl);
//    try {
//        final Optional<String> nonRdfSourceDescUri =
//                getNonRDFSourceDescribedByUri(realUrl);
//        if ( nonRdfSourceDescUri.isPresent() ) {
//            String nonRdfSourceDescribedByUrl = nonRdfSourceDescUri.get();
//            logger.info("nonRdfSourceDescribedByUrl={}", nonRdfSourceDescribedByUrl);
//            return Collections.singletonList(nonRdfSourceDescribedByUrl);
//        }
//    } catch (final IOException ex) {
//        throw new UncheckedIOException(ex);
//    }
////    return Collections.singletonList(resourceUri);
//    return Collections.singletonList(realURL);
//  }
//
//  /*
//  * Get the describedBy Uri if the resource has a NON_RDF_SOURCE_URI link header.
//  */
//  private Optional<String> getNonRDFSourceDescribedByUri(final String resourceUri) throws IOException {
//      Optional<String> nonRdfSourceDescUri = Optional.empty();
//      final Header[] links = getLinkHeaders(resourceUri);
//      if ( links != null ) {
//          String descriptionUri = null;
//          boolean isNonRDFSource = false;
//          for ( final Header h : links ) {
//            logger.info("header: " + h);
////              final FcrepoLink link = new FcrepoLink(h.getValue());
////              if ( link.getRel().equals("describedby") ) {
////                  descriptionUri = link.getUri().toString();
////              } else if ( link.getUri().toString().contains(NON_RDF_SOURCE_URI)) {
////                  isNonRDFSource = true;
////              }
//          }
////          logger.debug("isNonRDFSource: " + isNonRDFSource);
////          if (isNonRDFSource && descriptionUri != null) {
////              nonRdfSourceDescUri = Optional.of(descriptionUri);
////          }
//      }
//      return nonRdfSourceDescUri;
//  }
//
//  /*
//  * Get the link headers for the resource at the given Uri.
//  */
//  private Header[] getLinkHeaders(final String resourceUri) throws IOException {
//      HttpClient httpClient = HttpClientBuilder.create().build();
//      final HttpHead request = new HttpHead(resourceUri);
//      request.addHeader(new BasicHeader(AUTHORIZATION, "Bearer " + authToken));
//      final HttpResponse response = httpClient.execute(request);
//      logger.debug("Got: " + response.getStatusLine().getStatusCode() + " for HEAD " + resourceUri);
//      return response.getHeaders("Link");
//  }
//}
