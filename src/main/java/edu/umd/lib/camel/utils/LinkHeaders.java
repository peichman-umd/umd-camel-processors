package edu.umd.lib.camel.utils;

import org.apache.http.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Link;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class LinkHeaders {
    private static final Logger logger = LoggerFactory.getLogger(LinkHeaders.class);

    final private List<Link> linkHeaders;

    public LinkHeaders(final List<?> linkHeaders) {
        this.linkHeaders = linkHeaders.stream().map(h -> Link.valueOf((String) h)).collect(Collectors.toList());
    }

    public LinkHeaders(final Header[] headers) {
        this.linkHeaders = new ArrayList<>();
        for (final Header h : headers) {
            if ("link".equalsIgnoreCase(h.getName())) {
                linkHeaders.add(Link.valueOf(h.getValue()));
            }
        }
    }

    /**
     * Get the first URI in this set of link headers whose link relation equals the
     * given rel value. If none is found, returns null.
     *
     * @param rel link relation name
     * @return first URI found, or null
     */
    public URI getUriByRel(final String rel) {
        final Optional<Link> link = linkHeaders.stream().filter(l -> l.getRel().equals(rel)).findFirst();
        return link.map(Link::getUri).orElse(null);
    }

    /**
     * Check if this set of links headers has a link matching the given rel value
     * and containing the given uri.
     *
     * @param rel link relation name
     * @param uri link target
     * @return boolean
     */
    public boolean contains(final String rel, final String uri) {
        return linkHeaders.stream().anyMatch(l -> l.getRel().equals(rel) && l.getUri().toString().contains(uri));
    }
}
