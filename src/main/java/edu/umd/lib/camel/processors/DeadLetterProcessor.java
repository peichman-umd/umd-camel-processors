package edu.umd.lib.camel.processors;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;

import java.io.Serializable;

public class DeadLetterProcessor implements Processor, Serializable {
    @Override
    public void process(final Exchange exchange) {
        final Message in = exchange.getIn();
        // record the last endpoint that Camel delivered to
        final String lastEndpointUri = exchange.getProperty(Exchange.TO_ENDPOINT, String.class);
        in.setHeader("CamelLastEndpointUri", lastEndpointUri);
        // record the exception message
        final Exception cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        in.setHeader("CamelExceptionMessage", cause.getMessage());
    }
}
