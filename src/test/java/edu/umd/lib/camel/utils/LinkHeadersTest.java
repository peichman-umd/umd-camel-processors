package edu.umd.lib.camel.utils;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class LinkHeadersTest {
    @Test
    public void testHeadersArrayConstructor() throws URISyntaxException {
        final Header[] headers = {
                new BasicHeader("Link", "<http://example.com/foo>; rel=\"describedby\""),
                new BasicHeader("Link", "<http://example.com/modelA>; rel=\"type\""),
                new BasicHeader("Content-Type", "text/plain")
        };

        final LinkHeaders linkHeaders = new LinkHeaders(headers);

        assertEquals(new URI("http://example.com/foo"), linkHeaders.getUriByRel("describedby"));
        assertNull(linkHeaders.getUriByRel("notalinkrelation"));

        assertTrue(linkHeaders.contains("type", "http://example.com/modelA"));
        assertFalse(linkHeaders.contains("type", "http://example.com/missingModel"));
    }

    @Test
    public void testListConstructor() throws URISyntaxException {
        final List<String> headerValues = new ArrayList<>();
        headerValues.add("<http://example.com/foo>; rel=\"describedby\"");
        headerValues.add("<http://example.com/modelA>; rel=\"type\"");

        final LinkHeaders linkHeaders = new LinkHeaders(headerValues);

        assertEquals(new URI("http://example.com/foo"), linkHeaders.getUriByRel("describedby"));
        assertNull(linkHeaders.getUriByRel("notalinkrelation"));

        assertTrue(linkHeaders.contains("type", "http://example.com/modelA"));
        assertFalse(linkHeaders.contains("type", "http://example.com/missingModel"));
    }
}
