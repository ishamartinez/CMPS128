package distributed;

import com.fasterxml.jackson.annotation.JsonIgnore;
import util.ObjectMapper;

import java.util.*;

public class VersionVector implements Comparable<VersionVector> {

    @JsonIgnore
    private final ObjectMapper mapper;

    private final Map<String, Integer> versionMap;

    public VersionVector() {

        mapper = new ObjectMapper();
        versionMap = Collections.synchronizedMap(new LinkedHashMap<String, Integer>());
    }

    // Insert a key into the vector only if it is new
    public void insert(String key) {

        versionMap.putIfAbsent(key, 0);
    }

    public void insertAll(String[] keys) {

        for(String key : keys) {

            this.insert(key);
        }
    }

    // Increment version of corresponding key
    public void increment(String key) {

        synchronized (versionMap) {

            int val = versionMap.get(key) + 1;
            versionMap.put(key, val);
        }
    }

    // Increments and returns json string based on jackson mapper
    public String incrementAndStringify(String key) {

        String mappedValue;

        synchronized (versionMap) {

            this.increment(key);
            mappedValue = mapper.stringify(this);
        }

        return mappedValue;
    }

    // Syncs this vector with another
    public void sync(VersionVector otherVector) {

        Iterator<String> iterator;
        iterator = otherVector.versionMap.keySet().iterator();

        while(iterator.hasNext()) {

            String key = iterator.next();
            if(!versionMap.containsKey(key))
                this.insert(key);
        }
    }

    // Syncs this vector with another and returns the difference
    public Set<String> syncDiff(VersionVector otherVector) {

        Set<String> newKeys = new HashSet<>();
        Set<String> otherKeys = otherVector.versionMap.keySet();

        for(String key : otherKeys) {

            if(!versionMap.containsKey(key)) {

                this.insert(key);
                newKeys.add(key);
            }
        }

        return newKeys;
    }

    // Merge vector based on max value
    public void merge(VersionVector otherVector) {

        Map<String, Integer> otherMap = otherVector.versionMap;
        Set<Map.Entry<String, Integer>> entries = versionMap.entrySet();

        synchronized (versionMap) {

            for(Map.Entry<String, Integer> entry : entries) {

                String key = entry.getKey();
                Integer value = entry.getValue();

                if(otherMap.containsKey(key)) {

                    Integer otherValue = otherMap.get(key);
                    entry.setValue(Math.max(value, otherValue));
                }
            }
        }
    }

    @Override
    // Returns -1 less than, 0 concurrent, 1 greater than
    public int compareTo(VersionVector otherVector) {

        Map<String, Integer> otherMap = otherVector.versionMap;
        Set<Map.Entry<String, Integer>> entries = versionMap.entrySet();

        boolean greaterThan, lessThan;
        greaterThan = lessThan = true;

        synchronized(versionMap) {

            for(Map.Entry<String, Integer> entry : entries) {

                String key = entry.getKey();
                Integer value = entry.getValue();

                if(otherMap.containsKey(key)) {

                    Integer otherValue = otherMap.get(key);
                    greaterThan = greaterThan && (value >= otherValue);
                    lessThan = lessThan && (value <= otherValue);
                }

                if(!greaterThan && !lessThan) break;
            }
        }

        if(greaterThan) return 1;
        else if(lessThan) return -1;

        return 0;
    }

    @Override
    // Returns self as json based on jackson mapper
    public String toString() {

        synchronized (versionMap) {

            return mapper.stringify(this);
        }
    }

    @Override
    public boolean equals(Object object) {

        if(!(object instanceof VersionVector)) return false;

        VersionVector versionVector = (VersionVector) object;
        return versionMap.equals(versionVector.versionMap);
    }
}
