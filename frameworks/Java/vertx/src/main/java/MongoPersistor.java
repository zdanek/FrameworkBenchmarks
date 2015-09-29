import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;

import java.util.List;
import java.util.Optional;

/**
 * Based on  <a href="https://github.com/vert-x/mod-mongo-persistor/blob/master/src/main/java/org/vertx/mods/MongoPersistor.java">MogoPersistor</a>
 */
public class MongoPersistor extends BusModBase implements Handler<Message<JsonObject>> {

    private static Logger LOG = LoggerFactory.getLogger(MongoPersistor.class);

    protected String address;
    protected String host;
    protected int port;
    protected String dbName;
    protected String username;
    protected String password;
    protected ReadPreference readPreference;
    protected boolean autoConnectRetry;
    protected int socketTimeout;
    protected boolean useSSL;

    private MongoClient mongoClient;
    private boolean useMongoTypes;

    @Override
    public void start() {
        super.start();
        LOG.info("MongoPersistor is starting");
        address = getOptionalStringConfig("address", "vertx.mongopersistor");

        host = getOptionalStringConfig("host", "localhost");
        LOG.info(String.format("using server [%s]", host));

        port = getOptionalIntConfig("port", 27017);
        dbName = getOptionalStringConfig("db_name", "default_db");
        username = getOptionalStringConfig("username", null);
        password = getOptionalStringConfig("password", null);
        readPreference = ReadPreference.valueOf(getOptionalStringConfig("read_preference", "primary"));
        int poolSize = getOptionalIntConfig("pool_size", 10);
        autoConnectRetry = getOptionalBooleanConfig("auto_connect_retry", true);
        socketTimeout = getOptionalIntConfig("socket_timeout", 60000);
        useSSL = getOptionalBooleanConfig("use_ssl", false);
        useMongoTypes = getOptionalBooleanConfig("use_mongo_types", false);
        System.out.println("config is" + config());

        mongoClient = MongoClient.createShared(vertx, config());

        vertx.eventBus().consumer(address, this);
    }

    @Override
    public void stop() {
        Optional.ofNullable(mongoClient).ifPresent(MongoClient::close);
    }

    @Override
    public void handle(Message<JsonObject> message) {
        String action = message.body().getString("action");

        if (action == null) {
            sendError(message, "action must be specified");
            return;
        }

        try {

            // Note actions should not be in camel case, but should use underscores
            // I have kept the version with camel case so as not to break compatibility

            switch (action) {
/*                case "save":
                    doSave(message);
                    break;*/
                case "update":
                    doUpdate(message);
                    break;
                case "find":
                    doFind(message);
                    break;
                case "findone":
                    doFindOne(message);
                    break;
 /*               // no need for a backwards compatible "findAndModify" since this feature was added after
                case "find_and_modify":
                    doFindAndModify(message);
                    break;
                case "delete":
                    doDelete(message);
                    break;
                case "count":
                    doCount(message);
                    break;
                case "getCollections":
                case "get_collections":
                    getCollections(message);
                    break;
                case "dropCollection":
                case "drop_collection":
                    dropCollection(message);
                    break;
                case "collectionStats":
                case "collection_stats":
                    getCollectionStats(message);
                    break;
                case "aggregate":
                    doAggregation(message);
                    break;
                case "command":
                    runCommand(message);
                    break;*/
                default:
                    sendError(message, "Invalid action: " + action);
            }
        } catch (MongoException e) {
            sendError(message, e.getMessage(), e);
        }
    }

    private void doUpdate(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            return;
        }
        JsonObject criteriaJson = getMandatoryObject("criteria", message);
        if (criteriaJson == null) {
            return;
        }

        JsonObject objNewJson = getMandatoryObject("objNew", message);
        if (objNewJson == null) {
            return;
        }
        JsonObject update = new JsonObject().put("$set", objNewJson);

        mongoClient.update(collection, criteriaJson, update, res -> {

            if (res.succeeded()) {
                JsonObject reply = new JsonObject();
//                reply.put("number", Integer.parseInt(res.result().toString()));
                if (message.replyAddress() != null) {
                    sendOK(message, reply);
                }
            } else {
                LOG.error("Error updating document", res.cause());
                sendError(message, res.cause().getMessage(), res.cause());
            }
        });


/*
        Boolean upsert = message.body().getBoolean("upsert", false);
        Boolean multi = message.body().getBoolean("multi", false);

        DBCollection coll = db.getCollection(collection);
        WriteConcern writeConcern = WriteConcern.valueOf(getOptionalStringConfig("writeConcern", ""));
        // Backwards compatibility
        if (writeConcern == null) {
            writeConcern = WriteConcern.valueOf(getOptionalStringConfig("write_concern", ""));
        }

        if (writeConcern == null) {
            writeConcern = db.getWriteConcern();
        }
        WriteResult res = coll.update(criteria, objNew, upsert, multi, writeConcern);*/

    }

    private void doFindOne(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            return;
        }
        JsonObject matcher = message.body().getJsonObject("matcher");
        JsonObject keys = message.body().getJsonObject("keys");

        if (matcher == null) {
            matcher = new JsonObject();
        }

        mongoClient.findOne(collection, matcher, keys, (AsyncResult<JsonObject> res) -> {

            if (res.succeeded()) {
                sendOK(message, new JsonObject().put("result", res.result()));
            } else {
                LOG.error("Error retrieving one document", res.cause());
                sendError(message, res.cause().getMessage(), res.cause());
            }

        });
    }

    private void doFind(Message<JsonObject> message) {

        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            return;
        }

        FindOptions options = new FindOptions();

        Integer limit = message.body().getInteger("limit");
        Integer skip = message.body().getInteger("skip");
        Object sort = message.body().getValue("sort");

        options.setLimit(limit == null ? FindOptions.DEFAULT_LIMIT : limit)
                .setSkip(skip == null ? FindOptions.DEFAULT_SKIP : skip)
                .setSort(sortObjectToJsonObject(sort));


        final Integer batchSize = message.body().containsKey("batch_size") ? message.body().getInteger("batch_size") : 100;
        Integer respTimeout = message.body().getInteger("timeout");
        final Integer timeout = respTimeout == null || respTimeout < 0 ? 10000 : respTimeout;

        JsonObject matcher = message.body().getJsonObject("matcher");
        JsonObject keys = message.body().getJsonObject("keys");
        options.setFields(keys);

        Object hint = message.body().getValue("hint");

        if (matcher == null) {
            matcher = new JsonObject();
        }
        mongoClient.findWithOptions(collection, matcher, options, res -> {
            if (res.failed()) {
                sendError(message, "Error finding documents", res.cause());
            } else {
                sendBatch(message, res.result(), batchSize, timeout);
            }
        });




        /*



        if (sort != null) {
        }*/
        if (hint != null) {
            throw new UnsupportedOperationException("Hint is currently not supported");
            /*if (hint instanceof JsonObject) {
                cursor.hint(jsonToDBObject((JsonObject) hint));
            } else if (hint instanceof String) {
                cursor.hint((String) hint);
            } else {
                throw new IllegalArgumentException("Cannot handle type " + hint.getClass().getSimpleName());
            }*/
        }
    }

    private JsonObject sortObjectToJsonObject(Object sortObj) {
        if (sortObj == null) {
            return null;
        }
        if (sortObj instanceof JsonObject) {
            return (JsonObject) sortObj;
        } else if (sortObj instanceof JsonArray) {
            JsonArray sortJsonObjects = (JsonArray) sortObj;
            JsonObject sortDBObject = new JsonObject();
            for (Object curSortObj : sortJsonObjects) {
                if (!(curSortObj instanceof JsonObject)) {
                    throw new IllegalArgumentException("Cannot handle type "
                            + curSortObj.getClass().getSimpleName());
                }

                sortDBObject.mergeIn((JsonObject) curSortObj);
            }

            return sortDBObject;
        } else {
            throw new IllegalArgumentException("Cannot handle type " + sortObj.getClass().getSimpleName());
        }
    }

    private void sendBatch(Message<JsonObject> message, final List<JsonObject> results, final int max, final int timeout) {
        int count = 0;
        JsonArray resultsToSend = new JsonArray();

        if (resultsToSend.size() <= max) {
            JsonObject reply = createBatchMessage("ok", new JsonArray(results));
            message.reply(reply);
        } else {
            sendError(message, "result size > max", new RuntimeException("result size > max. batching responding is not implemented."));
        }
/*
        while (results.hasNext() && count < max) {
            DBObject obj = results.next();
            JsonObject m = dbObjectToJsonObject(obj);
            results.add(m);
            count++;
        }
        if (results.hasNext()) {
            JsonObject reply = createBatchMessage("more-exist", results);

            // If the user doesn't reply within timeout, close the results
            final long timerID = vertx.setTimer(timeout, new Handler<Long>() {
                @Override
                public void handle(Long timerID) {
                    container.logger().warn("Closing DB results on timeout");
                    try {
                        results.close();
                    } catch (Exception ignore) {
                    }
                }
            });


            message.reply(reply, new Handler<Message<JsonObject>>() {
                @Override
                public void handle(Message<JsonObject> msg) {
                    vertx.cancelTimer(timerID);
                    // Get the next batch
                    sendBatch(msg, results, max, timeout);
                }
            });

        } else {
            JsonObject reply = createBatchMessage("ok", results);
            message.reply(reply);
            results.close();
        }*/
    }

    private JsonObject createBatchMessage(String status, JsonArray results) {
        JsonObject reply = new JsonObject();
        reply.put("results", results);
        reply.put("status", status);
        reply.put("number", results.size());
        return reply;
    }
}
