package edu.umd.lib.camel.processors;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.jena.datatypes.xsd.XSDDatatype.XSDdateTime;
import static org.apache.jena.datatypes.xsd.XSDDatatype.XSDstring;
import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;
import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;
import static org.apache.jena.riot.RDFDataMgr.write;
import static org.apache.jena.riot.RDFFormat.NTRIPLES;
import static org.apache.jena.vocabulary.RDF.type;

/**
 * A processor that converts an audit message into an RDF graph using the PREMIS vocabulary.
 *
 * Adapted from code in the fcrepo-camel-toolkit by Aaron Coburn and Esmé Cowles
 *
 */
public class AuditPremisProcessor implements Processor, Serializable {
    private static final Logger log = LoggerFactory.getLogger(AuditPremisProcessor.class);

    private static final String AUDIT = "http://fedora.info/definitions/v4/audit#";
    private static final String PREMIS = "http://www.loc.gov/premis/rdf/v1#";
    private static final String PROV = "http://www.w3.org/ns/prov#";
    private static final String EVENT_TYPE = "http://id.loc.gov/vocabulary/preservation/eventType/";
    private static final String EVENT_NAMESPACE = "http://fedora.info/definitions/v4/event#";
    private static final String AS_NAMESPACE = "https://www.w3.org/ns/activitystreams#";
    private static final String REPOSITORY = "http://fedora.info/definitions/v4/repository#";

    private static final String CONTENT_MOD = AUDIT + "contentModification";
    private static final String CONTENT_REM = AUDIT + "contentRemoval";
    private static final String METADATA_MOD = AUDIT + "metadataModification";

    private static final String CONTENT_ADD = EVENT_TYPE + "ing";
    private static final String OBJECT_ADD = EVENT_TYPE + "cre";
    private static final String OBJECT_REM = EVENT_TYPE + "del";

    private static final String EVENT_BASE_URI = "CamelAuditEventBaseUri";
    private static final String EVENT_URI = "CamelAuditEventUri";
    private static final String FCREPO_EVENT_ID = "CamelFcrepoEventId";
    private static final String FCREPO_EVENT_TYPE = "CamelFcrepoEventType";
    private static final String FCREPO_RESOURCE_TYPE = "CamelFcrepoResourceType";
    private static final String FCREPO_DATE_TIME = "CamelFcrepoDateTime";
    private static final String FCREPO_URI = "CamelFcrepoUri";

    /**
     * Define how a message should be processed.
     *
     * @param exchange the current camel message exchange
     */
    public void process(final Exchange exchange) throws Exception {
        final Message in = exchange.getIn();
        final String eventURIBase = in.getHeader(EVENT_BASE_URI, String.class);
        final String eventID = in.getHeader(FCREPO_EVENT_ID, String.class);
        final String fcrepoURI = in.getHeader(FCREPO_URI, String.class);
        final Resource eventURI = createResource(eventURIBase + "/" + eventID);
        final Optional<String> premisType = getAuditEventType(getEventTypes(in), getResourceTypes(in));
        log.debug("Processing message for audit info for {} (MessageID: {})", fcrepoURI, in.getMessageId());

        // update exchange
        premisType.ifPresent(rdfType -> in.setHeader("CamelAuditEventType", rdfType));
        in.setBody(serializedGraphForMessage(in, eventURI));
        in.setHeader(EVENT_URI, eventURI.toString());
        in.setHeader(Exchange.CONTENT_TYPE, "application/n-triples");
    }

    // namespaces and properties
    private static final Resource INTERNAL_EVENT = createResource(AUDIT + "InternalEvent");
    private static final Resource PREMIS_EVENT = createResource(PREMIS + "Event");
    private static final Resource PROV_EVENT = createResource(PROV + "InstantaneousEvent");

    private static final Property PREMIS_TIME = createProperty(PREMIS + "hasEventDateTime");
    private static final Property PREMIS_OBJ = createProperty(PREMIS + "hasEventRelatedObject");
    private static final Property PREMIS_AGENT = createProperty(PREMIS + "hasEventRelatedAgent");
    private static final Property PREMIS_TYPE = createProperty(PREMIS + "hasEventType");

    private static final String EMPTY_STRING = "";

    private static List<String> parseStringToList(final String string) {
        if (string == null) {
            return emptyList();
        } else {
            return new ArrayList<>(Arrays.asList(string.split(",")));
        }
    }

    private static List<String> getEventTypes(final Message message) {
        return parseStringToList(message.getHeader(FCREPO_EVENT_TYPE, String.class));
    }

    private static List<String> getResourceTypes(final Message message) {
        return parseStringToList(message.getHeader(FCREPO_RESOURCE_TYPE, String.class));
    }

    /**
     * Convert a Camel message to audit event description.
     * @param message Camel message produced by an audit event
     * @param subject RDF subject of the audit description
     */
    private static String serializedGraphForMessage(final Message message, final Resource subject) throws IOException {

        // serialize triples
        final ByteArrayOutputStream serializedGraph = new ByteArrayOutputStream();
        final Model model = createDefaultModel();

        // get info from jms message headers
        final String dateTime = message.getHeader(FCREPO_DATE_TIME, EMPTY_STRING, String.class);
        final String user = message.getHeader("CamelFcrepoUser", String.class);
        final String userAgent = message.getHeader("CamelFcrepoUserAgent", String.class);
        final String identifier = message.getHeader(FCREPO_URI, EMPTY_STRING, String.class);
        final Optional<String> premisType = getAuditEventType(getEventTypes(message), getResourceTypes(message));

        model.add( model.createStatement(subject, type, INTERNAL_EVENT) );
        model.add( model.createStatement(subject, type, PREMIS_EVENT) );
        model.add( model.createStatement(subject, type, PROV_EVENT) );

        // basic event info
        model.add( model.createStatement(subject, PREMIS_TIME, createTypedLiteral(dateTime, XSDdateTime)) );
        model.add( model.createStatement(subject, PREMIS_OBJ, createResource(identifier)) );

        model.add( model.createStatement(subject, PREMIS_AGENT, createTypedLiteral(user, XSDstring)) );
        model.add( model.createStatement(subject, PREMIS_AGENT, createTypedLiteral(userAgent, XSDstring)) );

        premisType.ifPresent(rdfType -> model.add(model.createStatement(subject, PREMIS_TYPE, createResource(rdfType))));

        write(serializedGraph, model, NTRIPLES);
        return serializedGraph.toString("UTF-8");
    }

    /**
     * Returns the Audit event type based on fedora event type and properties.
     *
     * @param eventTypes from Fedora
     * @return Audit event
     */
    private static Optional<String> getAuditEventType(final List<String> eventTypes, final List<String> resourceType) {
        // mapping event type/properties to audit event type
        if (eventTypes.contains(EVENT_NAMESPACE + "ResourceCreation") || eventTypes.contains(AS_NAMESPACE + "Create")) {
            if (resourceType.contains(REPOSITORY + "Binary")) {
                return of(CONTENT_ADD);
            } else {
                return of(OBJECT_ADD);
            }
        } else if (eventTypes.contains(EVENT_NAMESPACE + "ResourceDeletion") ||
                eventTypes.contains(AS_NAMESPACE + "Delete")) {
            if (resourceType.contains(REPOSITORY + "Binary")) {
                return of(CONTENT_REM);
            } else {
                return of(OBJECT_REM);
            }
        } else if (eventTypes.contains(EVENT_NAMESPACE + "ResourceModification") ||
                eventTypes.contains(AS_NAMESPACE + "Update")) {
            if (resourceType.contains(REPOSITORY + "Binary")) {
                return of(CONTENT_MOD);
            } else {
                return of(METADATA_MOD);
            }
        }
        return empty();
    }


}
