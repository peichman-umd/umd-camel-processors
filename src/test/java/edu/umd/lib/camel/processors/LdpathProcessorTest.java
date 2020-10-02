package edu.umd.lib.camel.processors;

import org.apache.commons.io.IOUtils;
import org.apache.marmotta.ldpath.exception.LDPathParseException;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.*;

public class LdpathProcessorTest {
    private final String uri = "http://localhost:8080/rest/dd/40/45/58/dd404558-f769-410c-8bda-8c8d892f67ad";

    private String getResourceAsString(final String name) throws IOException {
        final InputStream resource = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
        assert resource != null;
        return IOUtils.toString(resource);
    }

    @Test
    public void testSimpleProgram() throws LDPathParseException, IOException {
        LdpathProcessor processor = new LdpathProcessor();
        processor.setQuery(getResourceAsString("simpleProgram.ldpath"));
        Map<String, Collection<?>> result = processor.executeQuery(uri);
        assertEquals(1, result.size());
    }

    @Test
    public void testComplexProgram() throws IOException, LDPathParseException {
        LdpathProcessor processor = new LdpathProcessor();
        processor.setQuery(getResourceAsString("complexProgram.ldpath"));
        Map<String, Collection<?>> result = processor.executeQuery(uri);
        assertTrue(result.size() > 0);
    }

    @Test
    public void testJsonResult() throws LDPathParseException, IOException {
        LdpathProcessor processor = new LdpathProcessor();
        processor.setQuery(getResourceAsString("complexProgram.ldpath"));
        String jsonResult = processor.execute(uri);
        assertFalse(jsonResult.isEmpty());
    }
}
