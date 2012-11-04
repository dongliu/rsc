package xrd;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.ajax.JSON;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;



/**
 * This servlet is corresponding to the OrchServlet. The difference is it polls the directory directly in file system
 * not through another service. 
 * 
 * 
 * @author Dong Liu
 *
 */
public class WatchServlet extends HttpServlet {
    
    private static final long serialVersionUID = 8091114217447045544L;
    private ServletContext _servletContext;
    private ContextHandler _contextHandler;
    private Server _server;
    private HttpClient _client;
//    private ThreadPoolExecutor _sendExecutor;
    private Logger logger;
    
    private String host;
    private int port;
    
    private File _resourceBase;
    
    private String _newline = System.getProperty("line.separator");


    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        
        if (!request.getContentType().toLowerCase().contains("json")) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need json message");
            return;
        }
        
        // check request json
        Map requestMap = (Map) JSON.parse(request.getReader());

        if (requestMap == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need json message body");
            return;
        }
        
        // check scan id
        if (!requestMap.containsKey("scan_id")) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need scan id");
            return;
        }
        final String scan_id = (String) requestMap.get("scan_id");
  
        // check destination url
        if (!requestMap.containsKey("target_url")) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need target url");
            return;
        }
        final String target_url = (String) requestMap.get("target_url");
        
        // check map size
        if (!requestMap.containsKey("map_size")) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need map size");
            return;
        }
        int size = ((Long)requestMap.get("map_size")).intValue();
        
        // check period
        if (!requestMap.containsKey("period")) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need period");
            return;
        }
        int period = ((Long)requestMap.get("period")).intValue();
        
        
        logger.info("A pooling task of " + size + " images arrived at " + System.currentTimeMillis());
        
        // check the total map size         
        
        // create the polling task, and submit it
        ScheduledThreadPoolExecutor pollingExecutor = new ScheduledThreadPoolExecutor(1);
        Watch task = new Watch(_resourceBase, scan_id, host, port, target_url, _client, pollingExecutor, size);
        pollingExecutor.scheduleAtFixedRate(task, 1, period, TimeUnit.SECONDS);
        logger.debug("task sheduled to run every " + period + " seconds after " + 1 + " second.");
        
        // generate response
        response.setContentType("text/plain; charset=UTF-8");
        String message = "A polling tasked is running now." + _newline;
        byte[] data = message.getBytes("UTF-8");
        response.setContentLength(data.length);
        response.getOutputStream().write(data);
        return;
        
        
    }

    @Override
    public void init() throws ServletException {
        _servletContext = getServletContext();
        ContextHandler.Context scontext = ContextHandler.getCurrentContext();
        if (scontext == null)
            _contextHandler = ((ContextHandler.Context) _servletContext).getContextHandler();
        else
            _contextHandler = ContextHandler.getCurrentContext().getContextHandler();

        _server = _contextHandler.getServer();
        _client = (HttpClient) _server.getAttribute("httpclient");
        host = (String) _server.getAttribute("host");
        port = ((Integer) _server.getAttribute("port")).intValue();
        
        _resourceBase = (File) _server.getAttribute("resourceBase");
        
        logger = Log.getLogger(WatchServlet.class); 
    }
    
    
    @Override
    public void destroy() {
        _client.destroy();
        super.destroy();
    }
}

