package util;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;

// Wrapper for jackson mapper - encapsulates the try - catches
public class ObjectMapper extends com.fasterxml.jackson.databind.ObjectMapper {

    public ObjectMapper() {

        super();
        this.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
    }

    public String stringify(Object object) {

        String mappedObject = null;

        try {

            mappedObject = writeValueAsString(object);

        } catch(IOException e) {

            e.printStackTrace();
            System.exit(1);
        }

        return mappedObject;
    }

    public <T> T objectify(String content, Class<T> type) {

        T mappedString = null;

        try {

            mappedString = readValue(content, type);

        } catch(IOException e) {

            e.printStackTrace();
            System.exit(1);
        }

        return mappedString;
    }

    public <T> T objectify(String content, TypeReference typeReference) {

        T mappedString = null;

        try {

            mappedString = readValue(content, typeReference);

        } catch(IOException e) {

            e.printStackTrace();
            System.exit(1);
        }

        return mappedString;
    }
}
