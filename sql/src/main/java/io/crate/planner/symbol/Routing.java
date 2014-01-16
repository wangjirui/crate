package io.crate.planner.symbol;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Routing extends Symbol {

    public static final SymbolFactory<Routing> FACTORY = new SymbolFactory<Routing>() {
        @Override
        public Routing newInstance() {
            return new Routing();
        }
    };

    public Routing() {

    }

    private Map<String, Map<String, Integer>> locations;

    public Routing(Map<String, Map<String, Integer>> locations) {
        this.locations = locations;
    }

    public Map<String, Map<String, Integer>> locations() {
        return locations;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.ROUTING;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitRouting(this, context);
    }

    public boolean hasLocations() {
        return locations != null && locations().size() > 0;
    }

    public Set<String> nodes() {
        if (hasLocations()) {
            return locations.keySet();
        }
        return ImmutableSet.of();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int numLocations = in.readVInt();
        locations = new HashMap<>(numLocations);

        String nodeId;
        int numInner;
        Map<String, Integer> innerMap;
        for (int i = 0; i < numLocations; i++) {
            nodeId = in.readString();
            numInner = in.readVInt();
            innerMap = new HashMap<>(numInner);

            locations.put(nodeId, innerMap);
            for (int j = 0; j < numInner; j++) {
                innerMap.put(in.readString(), in.readVInt());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(locations.size());

        for (Map.Entry<String, Map<String, Integer>> entry : locations.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue().size());

            for (Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()) {
                out.writeString(innerEntry.getKey());
                out.writeVInt(innerEntry.getValue());
            }
        }
    }
}
