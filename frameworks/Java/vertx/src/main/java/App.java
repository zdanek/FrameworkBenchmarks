import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class App {

    public static void main(String[] args) {

        JsonObject config = new JsonObject()
                .put("address", "hello.persistor")
                .put("db_name", "hello_world")
                .put("host", "127.0.0.1")
                .put("port", 27017)
                .put("maxPoolSize", 100);

        System.out.println("Starting vertx app");

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(MongoPersistor.class.getName(), new DeploymentOptions().setConfig(config), res ->{
            Logger log = LoggerFactory.getLogger(App.class);
            if (res.succeeded()) {
                log.info("Deployed successfuly MongoPersistor");
                log.info("Now deploying WebServer");
                vertx.deployVerticle(WebServer.class.getName());
            } else {
                log.error("Error deploying MongoPersistor", res.cause());
            }
        });

    }
}
