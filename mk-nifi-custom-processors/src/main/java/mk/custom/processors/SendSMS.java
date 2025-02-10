package mk.custom.processors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
//import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@TriggerWhenEmpty
@Tags({"sms", "text"})
@CapabilityDescription("Sends text message to specified recipient cell phone number in input file.")
public class SendSMS extends AbstractProcessor {

    //--------------------------------------------------------------
    public void writeOutFile(String msg) {
        String text = msg + "\n";
        String filePath = "";
        try {
            Files.write(Paths.get(filePath), text.getBytes(), StandardOpenOption.APPEND);
        }
        catch (IOException e) {
            System.out.println(e);
        }
    }
    //--------------------------------------------------------------


//    public static final PropertyDescriptor PHONE_NUMBER = new PropertyDescriptor.Builder()
//            .name("Phone number")
//            .description("Phone number")
//            .required(true)
//            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//            .build();

    public static final Relationship REL_SMS_SEND = new Relationship.Builder()
            .name("sms_send")
            .description("The orginal text message send to recipient cell phone number.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If sms can not send for some reason, the original message will be routed to this destination")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
//    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();

        this.properties = Collections.unmodifiableList(properties);

//        final List<PropertyDescriptor> descriptors = new ArrayList<>();
//        descriptors.add(PHONE_NUMBER);
//        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SMS_SEND);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            writeOutFile("flowFile is null");
            return;
        }

        final Properties property = new Properties();

        getLogger().info("onTrigger");

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream rawIn) throws IOException {
                property.load(rawIn);
                String contactNumber = property.getProperty("contact_number");
                String smsText = property.getProperty("message_text");

                writeOutFile("property contact_number: " + contactNumber);
                writeOutFile("property message_text: " + smsText);

                getLogger().info("Contact number: " + contactNumber);
                getLogger().info("Text SMS: " + smsText);

                try {
                    getLogger().info("Result***********: " + sendSMS(contactNumber, smsText));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return;
            }
        });

        session.transfer(flowFile, REL_SMS_SEND);
    }

    private String sendSMS(String contactNumber, String smsText) throws Exception{

        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost("http://smsgateway.ca/services/message.svc/XAO706fy87/" + contactNumber);
        String input = "{\"MessageBody\":\"" + smsText+"\"}";
        StringEntity params = new StringEntity(input,"UTF-8");
        params.setContentType("application/json; charset=UTF-8");
        post.setEntity(params);

        HttpResponse response = client.execute(post);
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        return result.toString();
    }
}
