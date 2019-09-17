package enity;

public class HardWareHeader extends AbstractHeaderReplacer {
    public static final String NAME = "component";
    
    public HardWareHeader() {
        super(new String[]{
            "Name" + HASH_KEY + "component_name",
            "NearEnd" + HASH_KEY + "component_bbf-hardware-rpf:rpf_performance_intervals-15min_current_near-end_dpu-extracted-energy",
            "FarEnd" + HASH_KEY + "component_bbf-hardware-rpf:rpf_performance_intervals-15min_current_far-end_pse-injected-energy",
        });
    }
}

