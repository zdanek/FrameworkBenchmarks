/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Base helper class for Java modules which use the event bus.<p>
 * You don't have to use this class but it contains some useful functionality.<p>
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public abstract class BusModBase extends AbstractVerticle {

    private Logger logger;
    private JsonObject config;

    /**
     * Start the busmod
     */
    public void start() {
        config = config();
        logger = LoggerFactory.getLogger(getClass());
    }

    protected void sendOK(Message<JsonObject> message) {
        sendOK(message, null);
    }

    protected void sendStatus(String status, Message<JsonObject> message) {
        sendStatus(status, message, null);
    }

    protected void sendStatus(String status, Message<JsonObject> message, JsonObject json) {
        if (json == null) {
            json = new JsonObject();
        }
        json.put("status", status);
        message.reply(json);
    }

    protected void sendOK(Message<JsonObject> message, JsonObject json) {
        sendStatus("ok", message, json);
    }

    protected void sendError(Message<JsonObject> message, String error) {
        sendError(message, error, null);
    }

    protected void sendError(Message<JsonObject> message, String error, Exception e) {
        logger.error(error, e);
        JsonObject json = new JsonObject().put("status", "error").put("message", error);
        message.reply(json);
    }

    protected String getMandatoryString(String field, Message<JsonObject> message) {
        String val = message.body().getString(field);
        if (val == null) {
            sendError(message, field + " must be specified");
        }
        return val;
    }

    protected JsonObject getMandatoryObject(String field, Message<JsonObject> message) {
        JsonObject val = message.body().getJsonObject(field);
        if (val == null) {
            sendError(message, field + " must be specified");
        }
        return val;
    }

    protected boolean getOptionalBooleanConfig(String fieldName, boolean defaultValue) {
        Boolean b = config.getBoolean(fieldName);
        return b == null ? defaultValue : b.booleanValue();
    }

    protected String getOptionalStringConfig(String fieldName, String defaultValue) {
        String s = config.getString(fieldName);
        return s == null ? defaultValue : s;
    }

    protected int getOptionalIntConfig(String fieldName, int defaultValue) {
        Number i = config.getInteger(fieldName);
        return i == null ? defaultValue : i.intValue();
    }

    protected long getOptionalLongConfig(String fieldName, long defaultValue) {
        Number l = config.getInteger(fieldName);
        return l == null ? defaultValue : l.longValue();
    }

    protected JsonObject getOptionalObjectConfig(String fieldName, JsonObject defaultValue) {
        JsonObject o = config.getJsonObject(fieldName);
        return o == null ? defaultValue : o;
    }

    protected JsonArray getOptionalArrayConfig(String fieldName, JsonArray defaultValue) {
        JsonArray a = config.getJsonArray(fieldName);
        return a == null ? defaultValue : a;
    }

    protected boolean getMandatoryBooleanConfig(String fieldName) {
        Boolean b = config.getBoolean(fieldName);
        if (b == null) {
            throw new IllegalArgumentException(fieldName + " must be specified in config for busmod");
        }
        return b;
    }

    protected String getMandatoryStringConfig(String fieldName) {
        String s = config.getString(fieldName);
        if (s == null) {
            throw new IllegalArgumentException(fieldName + " must be specified in config for busmod");
        }
        return s;
    }

    protected int getMandatoryIntConfig(String fieldName) {
        Number i = config.getInteger(fieldName);
        if (i == null) {
            throw new IllegalArgumentException(fieldName + " must be specified in config for busmod");
        }
        return i.intValue();
    }

    protected long getMandatoryLongConfig(String fieldName) {
        Number l = config.getInteger(fieldName);
        if (l == null) {
            throw new IllegalArgumentException(fieldName + " must be specified in config for busmod");
        }
        return l.longValue();
    }

}
