package enity;

public class InterfaceHeader extends AbstractHeaderReplacer {
    public static final String NAME = "interface";

    public InterfaceHeader() {
        super(new String[] { "Name" + HASH_KEY + "interface_name",
                "Administrator State" + HASH_KEY + "interface_admin-status",
                "Operation State" + HASH_KEY + "interface_oper-status", });
    }
}
