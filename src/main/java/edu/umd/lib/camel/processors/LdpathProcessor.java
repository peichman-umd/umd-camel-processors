package edu.umd.lib.camel.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.marmotta.ldpath.LDPath;
import org.apache.marmotta.ldpath.backend.linkeddata.LDCacheBackend;

import java.io.StringReader;
import java.util.Map;

public class LdpathProcessor implements Processor {
    private String query;

    private final ObjectMapper objectMapper;

    public LdpathProcessor() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        final Message in = exchange.getIn();
        final String uri = in.getHeader("FcrepoCamelUri", String.class);
        LDCacheBackend backend = new LDCacheBackend();
        LDPath ldpath = new LDPath(backend);
        final Map<String, String> results = ldpath.programQuery(uri, new StringReader(query));
        final String json = objectMapper.writeValueAsString(results);
        in.setBody(json);
        in.setHeader("Content-Type", "application/json");
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
