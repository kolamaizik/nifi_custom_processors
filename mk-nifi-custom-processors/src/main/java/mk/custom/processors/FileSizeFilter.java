package mk.custom.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"FileSizeFilter"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class FileSizeFilter extends AbstractProcessor {

    private static int MAX_FILE_SIZE = 0;

    public static final PropertyDescriptor MAX_FILE_SIZE_ATTRIBUTE_PROPERTY = new PropertyDescriptor.Builder()
            .name("Maximum File Size")
            .description("The maximum size that a file can be in order to be pulled")
            .required(true)
            .defaultValue("some.file.size")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("Pass the file")
            .description("This file will be passed on to success")
            .build();

    public static final Relationship FAIL_RELATIONSHIP = new Relationship.Builder()
            .name("Don't pass the file")
            .description("This file will NOT be passed on to success")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MAX_FILE_SIZE_ATTRIBUTE_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAIL_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        String attributeMax = context.getProperty(MAX_FILE_SIZE_ATTRIBUTE_PROPERTY).getValue();

        getLogger().info("Got a flow file");
        getLogger().info("MAX_FILE_SIZE = " + MAX_FILE_SIZE);
        getLogger().info("attributeMax = " + attributeMax);
        getLogger().info("flowFile.getAttribute(attributeMax) = " + flowFile.getAttribute(attributeMax));
        getLogger().info("flowFile.getSize() = " + flowFile.getSize());

        if (flowFile.getAttribute(attributeMax) != null){
            MAX_FILE_SIZE = Integer.parseInt(flowFile.getAttribute(attributeMax));
            getLogger().info("Reset MAX file size value to " + MAX_FILE_SIZE + ";  Attrib Ref: " + attributeMax);
            session.remove(flowFile);
            return;
        }

        if (flowFile.getSize() < MAX_FILE_SIZE){
            getLogger().info("File passed, size less that " + MAX_FILE_SIZE + " (" + flowFile.getSize() + ")");
            session.transfer(flowFile, SUCCESS_RELATIONSHIP);
        } else {
            getLogger().info("File NOT passed, size less that " + MAX_FILE_SIZE + " (" + flowFile.getSize() + ")");
            session.transfer(flowFile, FAIL_RELATIONSHIP);
        }


    }
}