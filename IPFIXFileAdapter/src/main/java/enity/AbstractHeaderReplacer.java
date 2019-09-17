package enity;

public abstract class AbstractHeaderReplacer {
    public static final String HASH_KEY = "###";
    protected String[] headers;
    
    public AbstractHeaderReplacer(String[] headers) {
        this.headers = headers;
    }
    
    public StringBuilder replaceHeaders(StringBuilder value) {
        String result = value.toString();
        for (String s : headers) {
            String[] split = s.split(HASH_KEY);
            String str1 = split[0];
            String str2 = split[1];
            result = result.replaceAll(str2, str1);
        }
        return new StringBuilder(result);
    }
}
