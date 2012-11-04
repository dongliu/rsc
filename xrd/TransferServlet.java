package xrd;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Dispatcher;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.URIUtil;

/**
 * Functionality interfaces
 *
 * <p>
 * GET /_transfers
 *
 * retrieve the list of transfers in queue
 * </p>
 *
 * <p>
 * Get /_transfers/history
 *
 * retrieve all the started transfers
 * </p>
 * <p>
 * DELETE /_transfers/{transfer:id}
 *
 * remove the {transfer:id} from the queue
 * </p>
 *
 *
 *
 * @author Dong Liu
 *
 */
public class TransferServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private ServletContext _servletContext;
    private ContextHandler _contextHandler;
    private Server _server;

//    private ThreadPoolExecutor _transferExecutor;
    private ConcurrentHashMap<File, Transfer> _submitted;
    private LinkedBlockingQueue<Transfer> _executed;

    private String _newline = System.getProperty("line.separator");

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String pathInContext = getPathInContext(request);
        String[] path = pathInContext.split(URIUtil.SLASH);
        int numberLayer = path.length;
        if (numberLayer > 3) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "URL not understood.");
            return;
        }

        // handle /_transfer or /_transfer/
        if (numberLayer == 2) {
            sendSubmittedTransfer(request, response);
            return;
        }

        // handle /_transfer/history
        if (numberLayer == 3) {
            String last = path[2];
            if (last.equalsIgnoreCase("history")) {
                sendTransferHistory(request, response);
                return;
            } else {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "URL not understood.");
                return;
            }
        }

    }

    private void sendTransferHistory(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        byte[] data = null;
        String list = getListHistory(_executed);
        data = list.getBytes("UTF-8");
        response.setContentType("application/json; charset=UTF-8");
        response.setContentLength(data.length);
        response.getOutputStream().write(data);

    }

    
    private String getListHistory(LinkedBlockingQueue<Transfer> executed) {
        StringBuilder buf = new StringBuilder(4096);
        buf.append("{\"history\":[");
        if (!executed.isEmpty()) {
            Transfer[] tasks = (Transfer[]) executed.toArray(new Transfer[0]);
            for (int i = 0; i < tasks.length; i++) {
                if (i != 0)
                    buf.append(",");
                buf.append("{\"description\":");
                buf.append(tasks[i].getJson());
                buf.append(",\"info\":");
                buf.append(tasks[i].getInfo());
                buf.append("}");
            }
        }
        buf.append("]}" + _newline);
        return buf.toString();
    }

    private void sendSubmittedTransfer(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        byte[] data = null;
        String list = getListSubmitted(_submitted);
        data = list.getBytes("UTF-8");
        response.setContentType("application/json; charset=UTF-8");
        response.setContentLength(data.length);
        response.getOutputStream().write(data);
    }


    private String getListSubmitted(ConcurrentHashMap<File, Transfer> submitted) {
        StringBuilder buf = new StringBuilder(4096);
        buf.append("{\"submitted\":[");
        if (!submitted.isEmpty()) {
            Transfer[] tasks =  (Transfer[]) submitted.values().toArray();
            for (int i = 0; i < tasks.length; i++) {
                if (i != 0)
                    buf.append(",");
                buf.append("{\"description\":");
                buf.append(tasks[i].getJson());
                buf.append(",\"progress\":");
                buf.append(tasks[i].getProgress());
                buf.append("}");
            }
        }
        buf.append("]}" + _newline);
        return buf.toString();
    }

    private String getPathInContext(HttpServletRequest request) {
        String servletPath = null;
        String pathInfo = null;
        Boolean included = request.getAttribute(Dispatcher.INCLUDE_REQUEST_URI) != null;
        if (included != null && included.booleanValue()) {
            servletPath = (String) request.getAttribute(Dispatcher.INCLUDE_SERVLET_PATH);
            pathInfo = (String) request.getAttribute(Dispatcher.INCLUDE_PATH_INFO);
            if (servletPath == null) {
                servletPath = request.getServletPath();
                pathInfo = request.getPathInfo();
            }
        } else {
            included = Boolean.FALSE;
            servletPath = request.getServletPath();
            pathInfo = request.getPathInfo();
        }

        return URIUtil.addPaths(servletPath, pathInfo);
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
        _submitted = (ConcurrentHashMap<File, Transfer>) _server.getAttribute("submitted");
        _executed = (LinkedBlockingQueue<Transfer>) _server.getAttribute("executed");

    }

}
