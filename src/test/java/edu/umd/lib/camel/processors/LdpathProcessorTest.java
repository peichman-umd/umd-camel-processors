package edu.umd.lib.camel.processors;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

public class LdpathProcessorTest {
    private final String uri = "http://localhost:8080/rest/af/c6/d8/20/afc6d820-427a-4932-9df5-3eb002958fd2";

    private String getResourceAsString(final String name) throws IOException {
        final InputStream resource = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
        assert resource != null;
        return IOUtils.toString(resource);
    }
/*
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
*/
}
