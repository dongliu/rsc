package xrd;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Dispatcher;
import org.eclipse.jetty.util.URIUtil;

/**
 * GET /_all_scans : a list of all the scans in JSON
 *
 * @author Dong Liu
 *
 */
public class UsageServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

    private String _serviceName;
    private String _host;

    private String _newline = System.getProperty("line.separator");

    public void init() throws UnavailableException {

        String name = getInitParameter("name");
        if (name != null)
            _serviceName = name;

        String host = getInitParameter("host");
        if (host != null)
            _host = host;


    }

    @Override
    protected void doGet(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
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

        String pathInContext = URIUtil.addPaths(servletPath, pathInfo);

        if (pathInContext.endsWith("_usage")) {
            response.setContentType("text/plain; charset=UTF-8");
            String usage = "This is the service for science studio XRD scans. You can try /_scans to get all the scans. \n" + "If you know a scanID, try /_scans/scanID for more information. " + _newline;
            byte[] data = usage.getBytes("UTF-8");
            response.setContentLength(data.length);
            response.getOutputStream().write(data);
            return;
        }

        if (pathInContext.endsWith("_echo")) {
            response.setContentType("application/json; charset=UTF-8");
            String echo = "{\"" + _serviceName + "_time\":" + System.currentTimeMillis() + "," + "\"host\": \"" + _host + "\"}" + _newline;
            byte[] data = echo.getBytes("UTF-8");
            response.setContentLength(data.length);
            response.getOutputStream().write(data);
            return;

        }

    }
}
