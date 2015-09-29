import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;

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

        LOG.info(String.format("using server [%s]", address));
        host = getOptionalStringConfig("host", "localhost");

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
//                    doFind(message);
                    break;
                case "findone":
//                    doFindOne(message);
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
                reply.put("number", Integer.parseInt(res.result().toString()));
                sendOK(message, reply);
            } else {
                LOG.error("Error updating document", res.cause());
                sendError(message, res.cause().getMessage());
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
/*
    private void doFindOne(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            return;
        }
        JsonObject matcher = message.body().getObject("matcher");
        JsonObject keys = message.body().getObject("keys");
        DBCollection coll = db.getCollection(collection);
        DBObject res;
        if (matcher == null) {
            res = keys != null ? coll.findOne(null, jsonToDBObject(keys)) : coll.findOne();
        } else {
            res = keys != null ? coll.findOne(jsonToDBObject(matcher), jsonToDBObject(keys)) : coll.findOne(jsonToDBObject(matcher));
        }
        JsonObject reply = new JsonObject();
        if (res != null) {
            JsonObject m = new JsonObject(res.toMap());
            reply.putObject("result", m);
        }
        sendOK(message, reply);
    }

    private void doFind(Message<JsonObject> message) {
        String collection = getMandatoryString("collection", message);
        if (collection == null) {
            return;
        }
        Integer limit = (Integer) message.body().getNumber("limit");
        if (limit == null) {
            limit = -1;
        }
        Integer skip = (Integer) message.body().getNumber("skip");
        if (skip == null) {
            skip = -1;
        }
        Integer batchSize = (Integer) message.body().getNumber("batch_size");
        if (batchSize == null) {
            batchSize = 100;
        }
        Integer timeout = (Integer) message.body().getNumber("timeout");
        if (timeout == null || timeout < 0) {
            timeout = 10000; // 10 seconds
        }
        JsonObject matcher = message.body().getObject("matcher");
        JsonObject keys = message.body().getObject("keys");

        Object hint = message.body().getField("hint");
        Object sort = message.body().getField("sort");
        DBCollection coll = db.getCollection(collection);
        DBCursor cursor;
        if (matcher != null) {
            cursor = (keys == null) ?
                    coll.find(jsonToDBObject(matcher)) :
                    coll.find(jsonToDBObject(matcher), jsonToDBObject(keys));
        } else {
            cursor = coll.find();
        }
        if (skip != -1) {
            cursor.skip(skip);
        }
        if (limit != -1) {
            cursor.limit(limit);
        }
        if (sort != null) {
            cursor.sort(sortObjectToDBObject(sort));
        }
        if (hint != null) {
            if (hint instanceof JsonObject) {
                cursor.hint(jsonToDBObject((JsonObject) hint));
            } else if (hint instanceof String) {
                cursor.hint((String) hint);
            } else {
                throw new IllegalArgumentException("Cannot handle type " + hint.getClass().getSimpleName());
            }
        }
        sendBatch(message, cursor, batchSize, timeout);
    }
*/
}
