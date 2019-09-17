package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;

import consumer.MsgConsumer;
import enity.AbstractHeaderReplacer;
import enity.HardWareHeader;
import enity.InterfaceHeader;
import producer.MsgProducer;

public class JsonUtils {
    private static final String CSV_TYPE = ".csv";
    private static final String COMMA = ",";
    private static final String ERROR = "ERROR: ";
    private static final String DASH = "_";
    private static final String DEVICE_ID = "device-id";
    private static final String DEVICE_SPECIFIC_DATA = "device-specific-data";
    private static final String TIME_STAMP = "Time stamp";
    private String GENERIC_KEY = "\"%s\":\"%s\",";

    public static JsonUtils IN = new JsonUtils();
    
    private JsonUtils() { }
    
    public void convertAllStringToFile(String fileName, Timestamp timeStamp, String device, String component) throws IOException {
        StringBuilder fb = new StringBuilder(fileName);
        JsonNode jsonTree = new ObjectMapper().readTree(component);
        StringBuilder builder = new StringBuilder("[{");
        processJson(null, builder, jsonTree);
        builder.append("}]");
        builder = buildHeader(jsonTree, builder, fb);
        
        try (PrintWriter writer = new PrintWriter(new File(fb.toString()))){
            writeDeviceNameAndTimestampIntoFile(writer, timeStamp, device);
            writeToCSVFile(writer, builder);
        } catch(FileNotFoundException e) {
            System.out.println(ERROR + e.getMessage());
        }
    }
    
    private void writeDeviceNameAndTimestampIntoFile(PrintWriter writer, Timestamp timeStamp, String value) throws JsonMappingException, JsonProcessingException {
        StringBuilder sb = new StringBuilder();
        sb.append(TIME_STAMP); sb.append(COMMA); sb.append(timeStamp + "\n");
        JsonNode jsonTree = new ObjectMapper().readTree(value);
        jsonTree.fieldNames().forEachRemaining(fieldName -> {sb.append(fieldName + "\n");} );
        jsonTree.elements().next().fieldNames().forEachRemaining(fieldName -> {sb.append(fieldName + "\n");} );
        sb.append(DEVICE_ID); sb.append(COMMA); sb.append(getJsonNodeValue(jsonTree, DEVICE_ID) + "\n");
        sb.append(DEVICE_SPECIFIC_DATA); sb.append(COMMA); sb.append(getJsonNodeValue(jsonTree, DEVICE_SPECIFIC_DATA) + "\n" + "\n");
        writer.write(sb.toString());
    }
    
    private void writeToCSVFile(PrintWriter writer, StringBuilder builder) throws IOException {
        JsonNode result = new ObjectMapper().readTree(builder.toString());
        Builder csvSchemaBuilder = CsvSchema.builder();
        result.elements().next().fieldNames().forEachRemaining(fieldName -> {csvSchemaBuilder.addColumn(fieldName);});
        CsvMapper csvMapper = new CsvMapper();
        CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();
        csvMapper.writerFor(JsonNode.class).with(csvSchema).writeValue(writer, result);
    }

    private StringBuilder buildHeader(JsonNode jsonTree, StringBuilder builder, StringBuilder fileName) {
        AbstractHeaderReplacer header = null;
        Iterator<String> elements = jsonTree.fieldNames();
        if(elements.hasNext()) {
            String next = elements.next();
            if(next.equals(HardWareHeader.NAME)) {
                header = new HardWareHeader();
            } else {
                header = new InterfaceHeader();
            }
            fileName.append("_");
            fileName.append(next);
            fileName.append(CSV_TYPE);
        }
        builder = header.replaceHeaders(builder);
        return builder;
    }

    //Formatting a nested JSON message with new header
    private void processJson(String key, StringBuilder builder, JsonNode node) {
        if (key == null) {
            key = "";
        }
        if (node.isArray()) {
            Iterator<JsonNode> elements = node.elements();
            while (elements.hasNext()) {
                processJson(key, builder, elements.next());
                builder.deleteCharAt(builder.lastIndexOf(COMMA)).append("},{");
            }
        } else {
            Iterator<Entry<String, JsonNode>> fields = node.fields();
            if (!fields.hasNext()) {
                builder.append(String.format(GENERIC_KEY, key.substring(0, key.length() - 1), node.asText()));
            } else {
                String tmp = key;
                while (fields.hasNext()) {
                    Entry<String, JsonNode> f = fields.next();
                    key += f.getKey() + DASH;
                    processJson(key, builder, f.getValue());
                    key = tmp;
                }
            }
        }
    }
    
    private String getJsonNodeValue(JsonNode jsonTree, String parentName) {
        return jsonTree.findParent(parentName).path(parentName).textValue();
    }
    
    public void readFile(String server, FileReader reader, String topic, MsgProducer producer) throws InterruptedException, ExecutionException {
        producer = new MsgProducer(server);
        try {
            BufferedReader br = new BufferedReader(reader);
            String line;
            while ((line = br.readLine()) != null) {
                producer.put(topic, "user1", line);
            }
        } catch (IOException e) {
            System.out.println(ERROR + e.getMessage());
        }
        producer.close();
    }
    
    public String[] getArrayDeviceNameAndComponentValue(String value) {
        String[] arr = value.split("state\":");
        String[] arr1 = arr[0].split("\\{\"ietf");
        String[] result = new String[2];
        result[0] = arr1[0].concat("\"ietf" + arr1[1]+ "state\"}]}}");
        result[1] = arr[1].substring(0, arr[1].lastIndexOf("}}]}}"));
        return result;
    }
    
    public String getDeviceName(String value) {
        try {
            JsonNode jsonTree = new ObjectMapper().readTree(value);
            return getJsonNodeValue(jsonTree, DEVICE_ID);
        } catch (JsonProcessingException e) {
            System.out.println("Error when reading JSON message: '" + value + "'" );
            return null;
        }
    }
}
