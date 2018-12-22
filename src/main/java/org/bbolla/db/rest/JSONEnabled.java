package org.bbolla.db.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import org.bbolla.db.utils.JsonUtils;

/**
 * @author bbolla on 12/21/18
 */
public abstract class JSONEnabled<T> {

    public String json() {
        return JsonUtils.serialize(this);
    }

    public static <T> T fromJson(String input) {
        return JsonUtils.deserialize(input, new TypeReference<T>(){});
    }

}
