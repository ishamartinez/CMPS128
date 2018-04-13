package util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import distributed.VersionVector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KeyValueStore {

    @JsonIgnore
    private final ObjectMapper mapper;

    private final Map<String, Value> keyStore;

    public KeyValueStore() {

        mapper = new ObjectMapper();
        keyStore = Collections.synchronizedMap(new HashMap<>());
    }

    // Puts value only if it is a winning value
    public void put(String key, Value value) {

        synchronized (keyStore) {

            Value storedValue = keyStore.get(key);
            boolean newValue = true;

            if(storedValue != null) newValue = winningValue(storedValue, value);
            if(newValue) keyStore.put(key, value);
        }
    }

    public Value get(String key) {

        return keyStore.get(key);
    }

    public boolean containsKey(String key) {

        return keyStore.containsKey(key);
    }

    public void merge(KeyValueStore otherStore) {

        // Merge tables
        synchronized (keyStore) {

            // Loop through other and put all keys in keyStore
            Iterator<String> iterator;
            iterator = otherStore.keyStore.keySet().iterator();

            while(iterator.hasNext()) {

                String key = iterator.next();
                keyStore.put(key, otherStore.keyStore.get(key));
            }
        }
    }

    // If the the new value happens after or has a greater timestamp/id it is a winning value
    private boolean winningValue(Value oldValue, Value newValue) {

        VersionVector oldVector, newVector;
        oldVector = mapper.objectify(oldValue.version, VersionVector.class);
        newVector = mapper.objectify(newValue.version, VersionVector.class);

        oldVector.sync(newVector);
        newVector.sync(oldVector);

        if(!oldVector.equals(newVector)) {

            int comparison = newVector.compareTo(oldVector);
            if(comparison > 0) {

                return true;

            } else if(comparison == 0) {

                comparison = newValue.compareTo(oldValue);
                if(comparison > 0)
                    return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {

        synchronized (keyStore) {

            return mapper.stringify(this);
        }
    }
}
