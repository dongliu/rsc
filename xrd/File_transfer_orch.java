
package xrd;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;
import org.eclipse.jetty.util.RolloverFileOutputStream;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * This service starts an embedded jetty server to serve the service orchestration
 * 
 *
 * @author Dong Liu
 *
 *
 */
public class File_transfer_orch {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int max_thread = 200;
        int max_idle = 60000;
        int port = Integer.parseInt(System.getProperty("jetty.port", "8080"));
        String host = System.getProperty("jetty.host", "localhost");
//        String securityConfig = System.getProperty("security", "./src/realm.properties");
//        String clientRealm = System.getProperty("realm", "./src/realm.client");
        String serviceName = System.getProperty("name", "service");
        String log = System.getProperty("logStd", "yes");
        String logBase = System.getProperty("log", "./");
        int max_connection = Integer.parseInt(System.getProperty("client.mc", "8"));
        int low_resource_connection = 8;
        
        Logger logger = Log.getLogger(File_transfer_orch.class);

        QueuedThreadPool _pool = new QueuedThreadPool();
        _pool.setMaxThreads(max_thread);
        _pool.setMinThreads(10);
        _pool.setDaemon(true);

        HttpClient _httpClient = new HttpClient();
        _httpClient.setThreadPool(_pool);
        _httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        _httpClient.setMaxConnectionsPerAddress(max_connection);
        _httpClient.setTimeout(300000); // 5 minutes
        _httpClient.setMaxRetries(1);

       
        // a thread pool executor for sending new images to target
        ThreadPoolExecutor _sendExecutor = new ThreadPoolExecutor(5, max_connection, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
       
        Server _server = new Server();
        _server.setThreadPool(_pool);
        _server.addBean(_httpClient);
        _server.setGracefulShutdown(500);
        _server.setAttribute("host", host);
        _server.setAttribute("httpclient", _httpClient);
//        _server.setAttribute("submitted", _submittedTask);
//        _server.setAttribute("executed", _executedTask);
//        _server.setAttribute("pollingexecutor", _pollingExecutor);
        _server.setAttribute("sendexecutor", _sendExecutor);
//        _server.setAttribute("pool", _pool);

        SelectChannelConnector _connector = new SelectChannelConnector();
        _connector.setPort(port);
        _connector.setHost(host);
        _connector.setMaxIdleTime(max_idle);
        _connector.setLowResourceMaxIdleTime(max_idle / 6);
        _connector.setLowResourcesConnections(low_resource_connection);
        _server.setConnectors(new Connector[] { _connector });

        // server security start
//        LoginService loginService = new HashLoginService("transfer", securityConfig);
//        _server.addBean(loginService);
//
//        Constraint constraint = new Constraint();
//        constraint.setName(Constraint.__DIGEST_AUTH);
//        constraint.setRoles(new String[]{"user","admin"});
//        constraint.setAuthenticate(true);
//
//        ConstraintMapping cm = new ConstraintMapping();
//        cm.setConstraint(constraint);
//        cm.setPathSpec("/*");
//
//        Set<String> knownRoles = new HashSet<String>();
//        knownRoles.add("user");
//        knownRoles.add("admin");
//
//        ConstraintSecurityHandler security = new ConstraintSecurityHandler();
//        security.setConstraintMappings(Collections.singletonList(cm), knownRoles);
//        security.setAuthenticator(new DigestAuthenticator());
//        security.setLoginService(loginService);
//        security.setStrict(true);
        // server security end

        // client security start
//        HashRealmResolver realmResolver = new HashRealmResolver();
//        ClientRealmUtil.addRealms(realmResolver, clientRealm);
//        _httpClient.setRealmResolver(realmResolver);
//        // client security end

        HandlerCollection handlers = new HandlerCollection();

        ContextHandlerCollection contexts = new ContextHandlerCollection();

        RequestLogHandler requestLogHandler = new RequestLogHandler();

        handlers.setHandlers(new Handler[] { contexts, requestLogHandler });
        _server.setHandler(handlers);

        NCSARequestLog requestLog = new NCSARequestLog(logBase + "yyyy_mm_dd." + serviceName + ".request.log");
        requestLog.setExtended(false);
        requestLogHandler.setRequestLog(requestLog);

//        ServletContextHandler root = new ServletContextHandler(contexts, "/", new SessionHandler(), security, new ServletHandler(), null);
        ServletContextHandler root = new ServletContextHandler(contexts, "/", new SessionHandler(), null, new ServletHandler(), null);

        // add servlets to the server
        ServletHolder orchServlet = new ServletHolder(new OrchServlet());
        orchServlet.setInitOrder(1);
        root.addServlet(orchServlet, "/_orch/*");

        ServletHolder usageServlet = new ServletHolder(new UsageServlet());
        Map<String, String> usageServletConfig = new HashMap<String, String>();
        usageServletConfig.put("name", serviceName);
        usageServletConfig.put("host", host);
        usageServlet.setInitParameters(usageServletConfig);
        usageServlet.setInitOrder(1);

        root.addServlet(usageServlet, "/_echo");

                
        FilterHolder gzipFilter = new FilterHolder(new GzipFilter());
        gzipFilter.setInitParameter("minGzipSize", "256");
        root.getServletHandler().addFilterWithMapping(gzipFilter, "/*", FilterMapping.ALL);

        if (log.startsWith("y") || log.startsWith("Y") || log.startsWith("t") || log.startsWith("T")) {
            RolloverFileOutputStream logFile = new RolloverFileOutputStream(logBase + "yyyy_mm_dd." + serviceName + ".stderrout.log", false, 90, TimeZone.getDefault());
            PrintStream _log = new PrintStream(logFile);

            logger.info("Redirecting stderr stdout to " + logFile.getFilename());
            System.setErr(_log);
            System.setOut(_log);
        }

        _server.start();
        _server.join();

    }

}
