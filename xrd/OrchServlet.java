package xrd;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
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

public class OrchServlet extends HttpServlet {
	
	private static final long serialVersionUID = 8091114217447045544L;
	private ServletContext _servletContext;
	private ContextHandler _contextHandler;
	private Server _server;
	private HttpClient _client;
	private ThreadPoolExecutor _sendExecutor;
	private Logger logger;
	
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
        // check source url
        if (!requestMap.containsKey("source_url")) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need json source url");
            return;
        }

        final String source_url = (String) requestMap.get("source_url");
  
        // check destination url
        if (!requestMap.containsKey("target_url")) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need json scan url");
            return;
        }

        final String target_url = (String) requestMap.get("target_url");
        
        // check map size
        if (!requestMap.containsKey("map_size")) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need json map size");
            return;
        }
        
        
        int size = ((Long)requestMap.get("map_size")).intValue();
        
        // check period
        if (!requestMap.containsKey("period")) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need json period");
            return;
        }
        
        
        int period = ((Long)requestMap.get("period")).intValue();
        
        
        logger.info("A pooling task of " + size + " images arrived at " + System.currentTimeMillis());
        
        // check the total map size         
        
        // create the polling task, and submit it
        ScheduledThreadPoolExecutor pollingExecutor = new ScheduledThreadPoolExecutor(1);
        Polling task = new Polling(source_url, target_url, _client, pollingExecutor, _sendExecutor, size);
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
		_sendExecutor = (ThreadPoolExecutor) _server.getAttribute("sendexecutor");
		
		logger = Log.getLogger(OrchServlet.class); 
	}
    @Override
    public void destroy() {
        _client.destroy();
        super.destroy();
    }
}
