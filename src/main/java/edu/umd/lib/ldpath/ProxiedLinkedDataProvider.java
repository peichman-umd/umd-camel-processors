package edu.umd.lib.ldpath;

import org.apache.marmotta.ldclient.api.endpoint.Endpoint;
import org.apache.marmotta.ldclient.provider.rdf.LinkedDataProvider;
import org.jasig.cas.client.util.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LinkedDataProvider implementation that overrides the request URL, based on
 * a URL provided in a Map.
 * <p>
 * This enables an "internal" container-based URL to be used for retrieving the
 * resource, while maintaining the expected URL for the RDF triples.
 * <p>
 * This class relies on the "REPO_INTERNAL_URL" and "REPO_EXTERNAL_URL" environment
 * variables to properly convert URLs.
 */
public class ProxiedLinkedDataProvider extends LinkedDataProvider {
  private static final Logger logger = LoggerFactory.getLogger(ProxiedLinkedDataProvider.class);

  public static final String PROVIDER_NAME = "Proxied Linked Data";

  private static final Map<String, String> linkedDataMap = new HashMap<>();

  private static String repoInternalUrl;

  private static String repoExternalUrl;


  public ProxiedLinkedDataProvider() {
    String repoInternalUrl = System.getenv("REPO_INTERNAL_URL");
    if (repoInternalUrl == null) {
      repoInternalUrl = "http://repository:8080/rest";
      logger.warn("REPO_INTERNAL_URL environment variable not set. Using default of '{}", repoInternalUrl);
    }
    ProxiedLinkedDataProvider.repoInternalUrl = repoInternalUrl;

    String repoExternalUrl = System.getenv("REPO_EXTERNAL_URL");
    if (repoExternalUrl == null) {
      repoExternalUrl = "http://localhost:8080/rest";
      logger.warn("REPO_EXTERNAL_URL environment variable not set. Using default of '{}", repoExternalUrl);
    }
    ProxiedLinkedDataProvider.repoExternalUrl = repoExternalUrl;
  }

  @Override
  public String getName() {
    return PROVIDER_NAME;
  }

  /**
   * Returns either the linked data resource URL (if in the linkedDataMap), or
   * the given resourceURI,
   *
   * @param resourceUri the "external" URI of the resource being queried
   * @param endpoint    the Endpoint associated with the request.
   */
  public List<String> buildRequestUrl(final String resourceUri, final Endpoint endpoint) {
    String linkedDataMapKey = resourceUri;
    String fragment = "";
    if (linkedDataMapKey.contains("#")) {
      logger.debug("Stripping fragment from {}", linkedDataMapKey);
      // Strip off any URL fragments, as linkedDataMap keys won't have them.
      int hashIndex = resourceUri.indexOf("#");
      fragment = resourceUri.substring(hashIndex);
      linkedDataMapKey = linkedDataMapKey.substring(0, hashIndex);
    }

    if (linkedDataMap.containsKey(linkedDataMapKey)) {
      String linkedDataResourceUrl = linkedDataMap.get(linkedDataMapKey);

      if (linkedDataResourceUrl != null) {
        logger.debug("Returning {} for {}", linkedDataResourceUrl, resourceUri);
        return Collections.singletonList(linkedDataResourceUrl + fragment);
      }
    }

    // Sometimes resources come through without being in the linkedDataMapKey
    // (not sure how this happens -- might be node traversal in LDPath?)
    logger.debug("resourceURL of '{}' not found in linkedDataMap.", resourceUri);

    URL resourceURL;
    try {
      resourceURL = new URL(resourceUri);
    } catch (MalformedURLException e) {
      logger.error("Malformed URL: {}", resourceUri);
      throw new IllegalArgumentException("Malformed URL: " + resourceUri, e);
    }

    // If link does not match externalRepoUrl, then just return it
    if (!resourceURL.toString().startsWith(repoExternalUrl)) {
      logger.debug("resourceURL does not match repoExternalUrl, return '{}'", resourceURL);
      return Collections.singletonList(resourceURL.toString());
    }

    // Otherwise, it matches, so use the repoInternalUrl to rewrite the resource,
    final String internalURL = new URIBuilder(repoInternalUrl)
        .setPath(resourceURL.getPath())
        .setEncodedQuery(resourceURL.getQuery())
        .build()
        .toString();

    logger.debug("Returning modified URL of: {}", internalURL);
    return Collections.singletonList(internalURL);
  }

  /**
   * Sets a mapping between the "external" resourceUri, and the "internal"
   * linkedDataResourceUrl. The mapping will be removed when the
   * "buildRequestUrl" method is called.
   *
   * @param resourceUri           the "external" resourceUri
   * @param linkedDataResourceUrl the "internal" URL of the RDF metadata.
   */
  public void setLinkedDataMapping(String resourceUri, String linkedDataResourceUrl) {
    linkedDataMap.put(resourceUri, linkedDataResourceUrl);
  }

  public void removeLinkedDataMapping(String resourceUri) {
    linkedDataMap.remove(resourceUri);
  }
}
