package util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Value implements Comparable<Value> {

    public final String value, version, id;
    public final long timeStamp;

    @JsonCreator
    public Value(@JsonProperty("value") String value,
                 @JsonProperty("version") String version,
                 @JsonProperty("timeStamp") long timeStamp,
                 @JsonProperty("id") String id) {

        this.value = value;
        this.version = version;
        this.timeStamp = timeStamp;
        this.id = id;
    }

    @Override
    public boolean equals(Object object) {

        if (!(object instanceof Value)) return false;

        Value value = (Value) object;
        return this.value.equals(value.value) &&
               this.version.equals(value.version) &&
               this.timeStamp == value.timeStamp;
    }

    @Override
    public int compareTo(Value otherValue) {

        if(timeStamp > otherValue.timeStamp) {

            return 1;

        } else if (timeStamp < otherValue.timeStamp) {

            return -1;

        } else {

            return id.compareTo(otherValue.id);
        }
    }
}
