package dk.trustworks.clientmanager;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.trustworks.clientmanager.handlers.*;
import dk.trustworks.clientmanager.service.*;
import dk.trustworks.framework.persistence.Helper;
import dk.trustworks.framework.service.ServiceRegistry;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.util.Headers;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.Options;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by hans on 16/03/15.
 */
public class ClientApplication {

    private static final Logger log = LogManager.getLogger();

    public static void main(String[] args) throws Exception {
        new ClientApplication(Integer.parseInt(args[0]));
    }

    public ClientApplication(int port) throws Exception {
        log.info("ClientManager on port " + port);
        Class.forName("org.mariadb.jdbc.Driver");
        Properties properties = new Properties();
        try (InputStream in = Helper.class.getResourceAsStream("server.properties")) {
            properties.load(in);
        }
        final ObjectMapper objectMapper = new ObjectMapper();

        ServiceRegistry serviceRegistry = ServiceRegistry.getInstance();

        serviceRegistry.registerService("taskworkerconstraintuuid", new TaskWorkerConstraintService());
        serviceRegistry.registerService("taskuuid", new TaskService());
        serviceRegistry.registerService("projectuuid", new ProjectService());
        serviceRegistry.registerService("clientuuid", new ClientService());
        serviceRegistry.registerService("useruuid", new UserService());

        Undertow.builder()
                .addHttpListener(port, properties.getProperty("web.host"))
                .setBufferSize(1024 * 16)
                .setIoThreads(Runtime.getRuntime().availableProcessors() * 2) //this seems slightly faster in some configurations
                .setSocketOption(Options.BACKLOG, 10000)
                .setServerOption(UndertowOptions.ALWAYS_SET_KEEP_ALIVE, false) //don't send a keep-alive header for HTTP/1.1 requests, as it is not required
                .setServerOption(UndertowOptions.ALWAYS_SET_DATE, true)
                .setHandler(Handlers.header(Handlers.path()
                        .addPrefixPath("/api/clients", new ClientHandler())
                        .addPrefixPath("/api/clientdatas", new ClientDataHandler())
                        .addPrefixPath("/api/projects", new ProjectHandler())
                        .addPrefixPath("/api/tasks", new TaskHandler())
                        .addPrefixPath("/api/taskworkerconstraints", new TaskWorkerConstraintHandler())
                        .addPrefixPath("/api/taskworkerconstraintbudgets", new TaskWorkerConstraintBudgetHandler())
                        , Headers.SERVER_STRING, "U-tow"))
                        .setWorkerThreads(200)
                        .build()
                        .start();

        registerInZookeeper(properties.getProperty("zookeeper.host"), port);
    }

    private static void registerInZookeeper(String zooHost, int port) throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(zooHost+":2181", new RetryNTimes(5, 1000));
        curatorFramework.start();

        ServiceInstance serviceInstance = ServiceInstance.builder()
                .uriSpec(new UriSpec("{scheme}://{address}:{port}"))
                .address("localhost")
                .port(port)
                .name("clientservice")
                .build();

        ServiceDiscoveryBuilder.builder(Object.class)
                .basePath("trustworks")
                .client(curatorFramework)
                .thisInstance(serviceInstance)
                .build()
                .start();
    }
}
