package xrd;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpExchange;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeaderValues;
import org.eclipse.jetty.http.HttpHeaders;
import org.eclipse.jetty.http.HttpMethods;
import org.eclipse.jetty.http.HttpSchemes;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpVersions;
import org.eclipse.jetty.io.Buffer;
import org.eclipse.jetty.server.Dispatcher;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.IO;
import org.eclipse.jetty.util.URIUtil;
import org.eclipse.jetty.util.ajax.JSON;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

/**
 * 
 * Functionality interfaces
 * <p>
 * GET /_scans the list of scans available on the service
 * </p>
 * <p>
 * GET /_scans/{scanID} the list of images in the scan. The client can specify the {@code type} of images in the
 * parameter. The client can also get the progress of a scan being transferred by including a {@code progress}
 * parameter.
 * </p>
 * <p>
 * GET /_scans/{scanID} gives the list of images in a short format. This short format message is also cached in the
 * .cache directory in service base. The cache is expired by a request and a out-of-date timestamp.
 * </p>
 * <p>
 * GET /_scans/{scanID}/md5sum returns the md5 signatures of the images in the scan.
 * </p>
 * <p>
 * GET /_scans/{scanID}/md5sum-c returns the md5sum check result of the scan. It is in fact a comparison of the result
 * of GET /_scans/{scanID}/md5sum and GET original scan_url/md5sum. If some files do not pass the check, then another
 * fetching might be triggered by POST /_scans/{scanID} with the "patch" : "md5" option.
 * </p>
 * <p>
 * GET /_scans/{scanID}/{imageName} the image
 * </p>
 * <p>
 * POST /_scans The CLS side service or others (for testing) inform the service about the scan of scanID. The message
 * might contain information about how the images are named. Or it can indicate that all images with certain prefix
 * and/or suffix are expected to be transferred.
 * </p>
 * <p>
 * PUT /_scans/{scanID}/{imageName} add an image to an existed scan 
 * An image can be added by a json message indicating the image source or by the request including an image in body 
 * </p>
 * 
 * 
 * @author Dong Liu
 * 
 */
public class ScansServlet extends HttpServlet {

	private static final long serialVersionUID = 3450234358152487164L;
	private ServletContext _servletContext;
	private ContextHandler _contextHandler;
	private Server _server;
	private HttpClient _client;
	private ThreadPoolExecutor _decodeExecutor;
	private ThreadPoolExecutor _transferExecutor;
	private Logger logger;

	private ConcurrentHashMap<File, Transfer> _submittedTask;
	private LinkedBlockingQueue<Transfer> _executedTask;

	private String _imageType = "spe,tif";

	private String _newline = System.getProperty("line.separator");

	private File _resourceBase;
	private File _cacheBase;
	private static int magic = 4;

	private static int buffer = 2 * 8192;

	public void init() throws UnavailableException {
		_servletContext = getServletContext();
		ContextHandler.Context scontext = ContextHandler.getCurrentContext();
		if (scontext == null)
			_contextHandler = ((ContextHandler.Context) _servletContext).getContextHandler();
		else
			_contextHandler = ContextHandler.getCurrentContext().getContextHandler();

		_server = _contextHandler.getServer();
		_client = (HttpClient) _server.getAttribute("httpclient");
		_decodeExecutor = (ThreadPoolExecutor) _server.getAttribute("decodeexecutor");
		_transferExecutor = (ThreadPoolExecutor) _server.getAttribute("transferexecutor");
		
		logger = Log.getLogger(ScansServlet.class);

		_submittedTask = (ConcurrentHashMap<File, Transfer>) _server.getAttribute("submitted");
		_executedTask = (LinkedBlockingQueue<Transfer>) _server.getAttribute("executed");

//		String rb = getInitParameter("resourceBase");
//		String cb = getInitParameter("cacheBase");

//		if (rb != null) {
//			try {
//				_resourceBase = new File(rb);
//				_cacheBase = new File(cb);
//			} catch (Exception e) {
//				throw new UnavailableException(e.toString());
//			}
//		}
		
		_resourceBase = (File) _server.getAttribute("resourceBase");

		String imageType = getInitParameter("type");
		if (imageType != null) {
			_imageType = imageType;
		}

		String magicParameter = getInitParameter("magic");
		if (magicParameter != null)
			magic = Integer.parseInt(getInitParameter("magic"));


		logger.debug("resource base = " + _resourceBase.getAbsolutePath());

	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		String pathInContext = getPathInContext(request);
		int numberLayer = pathInContext.split(URIUtil.SLASH).length;
		
		if (numberLayer >= 4) {
			String forwardPath = pathInContext.substring(pathInContext.indexOf(URIUtil.SLASH, 1));
			request.getRequestDispatcher(forwardPath).forward(request, response);
			return;
		}

		// handle /_scans or /_scans/
		if (numberLayer == 2) {
			sendAllScans(request, response, _resourceBase);
			return;
		}

		// handle /_scans/scanName or /_scans/scanName/
		if (numberLayer == 3) {
			String path = pathInContext.substring(pathInContext.indexOf(URIUtil.SLASH, 1));
			File resource = new File(_resourceBase, path);
			// handle /_scans/scanName?md5sum or /_scans/scanName?md5sum-c
			if (request.getParameter("md5sum")!=null) {
				sendMd5(request, response, resource, _imageType);
				return;
			}
			if (request.getParameter("md5sum-c")!=null) {
				try {
					sendMd5Check(request, response, resource, _imageType);
				} catch (InterruptedException e) {
					response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
					logger.warn(e);
				}
				return;
			}
			if (request.getParameter("type") == null) {
				sendAllItems(request, response, resource);
				return;
			} else {
				String type = request.getParameter("type");
				sendAllItems(request, response, resource, type);
				return;
			}
		}

	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
			IOException {
	    
	    if (!request.getContentType().toLowerCase().contains("json")) {
	        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need json message");
            return;
	    }

		// Create a new scan with an id.
		// The id might be the same as that in the request message or be created based on that.
		Map requestMap = (Map) JSON.parse(request.getReader());

		if (requestMap == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "need json message body");
			return;
		}

		if (!requestMap.containsKey("scan_url")) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Request does not contain scan_url");
			return;
		}

		final String scan_url = (String) requestMap.get("scan_url");
		String scan = null;

		try {
			// handle path with multiple hierarchies
			String[] splits = new URL(scan_url).getPath().split(URIUtil.SLASH);
			scan = splits[splits.length - 1];
		} catch (MalformedURLException e) {
			logger.warn(Log.EXCEPTION, e);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		final Long arrived = new Long(System.currentTimeMillis());
		File scanDir = null;
		final String patch = (String) requestMap.get("patch");
		try {
			scanDir = new File(_resourceBase, scan);
		} catch (NullPointerException e) {
			logger.warn(Log.EXCEPTION, e);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "no scan path in request");
			return;
		}

		String resourceURL = URIUtil.encodePath(new URL("http", request.getServerName(), request.getServerPort(),
				URIUtil.SLASH + "_scans" + URIUtil.SLASH + scan + URIUtil.SLASH).toString());

		if (scanDir.exists()) {
			logger.debug("The directory " + scanDir.toString() + " exists.");
			if (patch == null || patch.equalsIgnoreCase("new") || patch.equalsIgnoreCase("md5")) {
				response.sendError(HttpServletResponse.SC_CONFLICT, scanDir.toString()
						+ " exists, and patch needs to be specified.");
				return;
			} else {
				response.setStatus(HttpStatus.OK_200);
			}

		} else {
			if (scanDir.mkdir()) {
				// write the origin json file 
				File origin = new File(scanDir, ".json");
				if (origin.createNewFile()) {
					Map originMap = new HashMap();
					originMap.put("origin_url", scan_url);
					if (requestMap.containsKey("note"))
						originMap.put("note", requestMap.get("note"));
					String originJson = JSON.toString(originMap);
					FileWriter writer = new FileWriter(origin, false); 
					writer.write(originJson);
					writer.close();
				}
				response.setStatus(HttpStatus.CREATED_201);
				response.setHeader(HttpHeaders.LOCATION, resourceURL);
			} else {
				logger.debug("The directory " + scanDir.toString() + " cannot be created.");
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "The directory " + scanDir.toString() + " cannot be created.");
				return;
			}

		}

		Transfer task = null;

		if (requestMap.containsKey("image_suffix") && requestMap.containsKey("image_prefix")) {
			if (requestMap.containsKey("image_start") && requestMap.containsKey("image_end")
					&& requestMap.containsKey("digit_number")) {
				long image_start = ((Long) requestMap.get("image_start")).longValue();
				long image_end = ((Long) requestMap.get("image_end")).longValue();
				final int digit_number = ((Long) requestMap.get("digit_number")).intValue();
				if (image_end < image_start) {
					response.sendError(HttpServletResponse.SC_BAD_REQUEST, "image number wrong.");
					return;
				}
				if (digit_number != 0 && ("" + image_end).length() > digit_number) {
					response.sendError(HttpServletResponse.SC_BAD_REQUEST, "image number format wrong.");
					return;
				}
				task = Transfer.createTransfer(arrived, requestMap, scanDir, _client, _decodeExecutor, _submittedTask,
						_executedTask);

			} else if (requestMap.containsKey("image_all")) {
				String all = (String) requestMap.get("image_all");
				if (getBoolean(all)) {
					task = Transfer.createTransfer(arrived, requestMap, scanDir, _client, _decodeExecutor,
							_submittedTask, _executedTask);
				} else {
					logger.debug("image_all not true in the JSON.");
					response.sendError(HttpStatus.BAD_REQUEST_400, "Cannot understand request.");
				}
			} else {
				logger.debug("Missing image number related entries in the JSON.");
				response.sendError(HttpStatus.BAD_REQUEST_400, "Missing image number related information.");
			}
		} else {
			logger.debug("Missing image_prefix and/or image_suffix in the JSON.");
			response.sendError(HttpStatus.BAD_REQUEST_400, "Missing image_prefix and/or image_suffix information.");
		}

		if (task == null) {
			response.sendError(HttpStatus.BAD_REQUEST_400,
					"Transfer task rejected because a similar task is being executed.");
		} else {
			try {
				// inform the client if the executor's threads and job queue are used up
				_transferExecutor.execute(task);
			} catch (RejectedExecutionException e) {
				response.sendError(HttpStatus.BAD_REQUEST_400, "Transfer task rejected because the queue is full.");
			}
		}

		response.setContentType("application/json; charset=UTF-8");

		Map responseMap = new HashMap();
		responseMap.put("original_url", scan_url);
		responseMap.put("scan_url", resourceURL);
		responseMap.put("transfer_id", arrived);
		String message = JSON.toString(responseMap) + _newline;
		byte[] data = message.getBytes("UTF-8");
		response.setContentLength(data.length);
		response.getOutputStream().write(data);
		return;
	}

	@Override
	protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String pathInContext = getPathInContext(request);

		// handle /_scans/{scanName}/{imageName}

		int numberLayer = pathInContext.split("/").length;
		if (numberLayer < 4) {
			logger.debug("Request uri = " + pathInContext);
			response.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		String scan = pathInContext.split("/")[numberLayer - 2];
		String resource = pathInContext.split("/")[numberLayer - 1];

		File scanDir = null;
		File resourceFile = null;
		try {
			scanDir = new File(_resourceBase, scan);
			resourceFile = new File(scanDir, resource);
		} catch (NullPointerException e) {
			logger.debug("no scan path in request");
			response.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		if (!scanDir.exists()) {
		    scanDir.mkdir();
		}
		
		String contentType = request.getContentType().toLowerCase();
		
		if (contentType.contains("json")) {
			Map requestMap = (Map) JSON.parse(request.getReader());

			if (!requestMap.containsKey("update") || !getBoolean((String) requestMap.get("update"))) {
				if (resourceFile.exists()) {
					logger.debug("The resource " + resourceFile.toString() + " already existed.");

					response.setContentType("text/plain; charset=UTF-8");
					response.sendError(HttpServletResponse.SC_CONFLICT, resource + " already existed.");
					return;
				}
			}

			if (!requestMap.containsKey("resource_url")) {
				logger.debug("Request does not contain scan_url or processing option.");
				response.sendError(HttpServletResponse.SC_BAD_REQUEST);
				return;
			}

			String resource_url = (String) requestMap.get("resource_url");

			long size = 0;

			if (requestMap.containsKey("size")) {
				size = ((Long) requestMap.get("size")).longValue();
			}

			if (size != 0) {
				scheduleRetrieve(resource_url, scanDir, resourceFile, size);
			} else {
				scheduleRetrieve(resource_url, scanDir, resourceFile);
			}			
		} else if (contentType.contains("image")) {
			// write the image
			InputStream input =  request.getInputStream();
			OutputStream output = new FileOutputStream(resourceFile);
			IO.copy(input, output);
			IO.close(input);
			IO.close(output);
		}
		response.setStatus(HttpStatus.CREATED_201);
		response.setContentType("application/json; charset=UTF-8");	
		Map responseMap = new HashMap();
		responseMap.put("resource_url", pathInContext);
		String message = JSON.toString(responseMap) + _newline;
		byte[] data = message.getBytes("UTF-8");
		response.setContentLength(data.length);
		response.getOutputStream().write(data);
	}

	private void scheduleRetrieve(String resourceUrl, File scanDir, final File resourceFile)
			throws FileNotFoundException {
//		logger.info("****************************************");
//		logger.info("start retrieving" + resourceFile.toString());
//		logger.info("****************************************");
//		final long start = System.currentTimeMillis();

		final BufferedOutputStream target = new BufferedOutputStream(new FileOutputStream(resourceFile, true), buffer);

		HttpExchange exchange = new HttpExchange() {
			private volatile int _responseStatus;

			protected void onRetry() {
				logger.warn(this.toString() + " retries to " + this.getMethod() + " " + this.getRequestURI());
			}

			protected void onResponseStatus(Buffer version, int status, Buffer reason) throws IOException {
				_responseStatus = status;
				super.onResponseStatus(version, status, reason);
			}

			protected void onResponseContent(Buffer content) {

				if (_responseStatus == HttpStatus.OK_200) {
					try {
						content.writeTo(target);
					} catch (IOException e) {
						logger.warn(e);
					}
				}
			}

			protected void onResponseComplete() throws IOException {
				target.close();
				logger.info("****************************************");
				logger.info(resourceFile.toString() + " finished at " + System.currentTimeMillis());
				logger.info("****************************************");
				super.onResponseComplete();
			}

		};
		exchange.setMethod("GET");
		exchange.setURL(resourceUrl);
		try {
			_client.send(exchange);
		} catch (IOException e) {
			logger.warn(Log.EXCEPTION, e);
		}
	}

	private void scheduleRetrieve(String resourceUrl, final File scanDir, final File resourceFile, long filesize,
			final int magic) throws FileNotFoundException {

		logger.info("****************************************");
		logger.info("start transfering" + resourceFile.toString());
		logger.info("****************************************");
		final long start = System.currentTimeMillis();

		// one random access file has problem of IO exceptions. try to use four regular files and then join them.

		final FileOutputStream target = new FileOutputStream(resourceFile, true);
		final AtomicInteger count = new AtomicInteger(magic);

		for (int i = 0; i < magic; i++) {

			final File part = new File(scanDir, resourceFile.getName() + ".part" + i + ".gz");
			final BufferedOutputStream fileStream = new BufferedOutputStream(new FileOutputStream(part), buffer);
			final ConcurrentHashMap<String, Integer> statusMap = new ConcurrentHashMap<String, Integer>();
			final ConcurrentHashMap<String, String> encodingMap = new ConcurrentHashMap<String, String>();
			final ConcurrentHashMap<String, String> rangeMap = new ConcurrentHashMap<String, String>();

			HttpExchange exchange = new HttpExchange() {

				private final HttpFields _responseFields = new HttpFields();
				private volatile int _responseStatus;

				private int _attempt = 0;

				protected void onConnectionFailed(Throwable x) {
					super.onConnectionFailed(x);
					if (_attempt < _client.maxRetries()) {
						try {
							this.reset();
							_client.send(this);
						} catch (IOException e) {
							this.onException(x);
						} finally {
							_attempt = _attempt + 1;
							logger.warn("resend " + _attempt + " times, " + this.toString());
						}
					}
				}

				protected void onRetry() throws IOException {
					logger.warn(this.toString() + " retries to " + this.getMethod() + " " + this.getRequestURI());
					super.onRetry();
				}

				protected void onResponseHeader(Buffer name, Buffer value) throws IOException {
					_responseFields.add(name, value);
					super.onResponseHeader(name, value);
				}

				protected void onResponseStatus(Buffer version, int status, Buffer reason) throws IOException {
					_responseStatus = status;
					super.onResponseStatus(version, status, reason);
				}

				protected void onResponseContent(Buffer content) throws IOException {
					if (_responseStatus == HttpStatus.PARTIAL_CONTENT_206) {
						content.writeTo(fileStream);
					}
				}

				protected void onResponseComplete() throws IOException {
					if (_responseStatus == HttpStatus.PARTIAL_CONTENT_206) {
						fileStream.flush();
						fileStream.close();
					}
					statusMap.putIfAbsent(part.getName(), new Integer(_responseStatus));
					if (_responseFields.containsKey(HttpHeaders.CONTENT_RANGE_BUFFER)) {
						rangeMap.putIfAbsent(part.getName(),
								_responseFields.getStringField(HttpHeaders.CONTENT_RANGE_BUFFER));
					}
					if (_responseFields.containsKey(HttpHeaders.CONTENT_ENCODING_BUFFER)) {
						encodingMap.putIfAbsent(part.getName(),
								_responseFields.getStringField(HttpHeaders.CONTENT_ENCODING_BUFFER));
					}
					logger.debug("The status is " + _responseStatus + " range is "
								+ _responseFields.getStringField(HttpHeaders.CONTENT_RANGE_BUFFER) + " encoding is "
								+ _responseFields.getStringField(HttpHeaders.CONTENT_ENCODING_BUFFER));

					if (count.decrementAndGet() == 0) { // all exchanges done
						logger.info("****************************************");
						logger.info(resourceFile.toString() + " parts requests finished in "
								+ (System.currentTimeMillis() - start) + " milliseconds. ");
						logger.info("****************************************");

						Enumeration<Integer> status = statusMap.elements();
						boolean fail = false;
						while (status.hasMoreElements()) {
							if (status.nextElement().intValue() != 206) {
								fail = true;
								break;
							}
						}

						Enumeration<String> encodings = encodingMap.elements();
						boolean gzipEncoding = true;
						while (encodings.hasMoreElements()) {
							String encoding = encodings.nextElement();
							if (encoding == null || encoding.indexOf(HttpHeaderValues.GZIP) == -1) {
								gzipEncoding = false;
								break;
							}
						}
						if (!gzipEncoding) {
							logger.warn("Not all the parts are in Gzip encoding.");
						}

						if (gzipEncoding && !fail) {
							for (int i = 0; i < magic; i++) {
								File part = new File(scanDir, resourceFile.getName() + ".part" + i + ".gz");
								FileInputStream partStream = null;
								GZIPInputStream gis = null;
								try {
									partStream = new FileInputStream(part);
									gis = new GZIPInputStream(partStream);
									IO.copy(gis, target);
								} finally {
									gis.close();
									partStream.close();
								}
							}

							for (int i = 0; i < magic; i++) {
								File part = new File(scanDir, resourceFile.getName() + ".part" + i);
								part.delete();
							}

						}

						target.close();

						if (fail) {
							resourceFile.delete();
							logger.info("****************************************");
							logger.info(resourceFile.toString() + " failed in " + (System.currentTimeMillis() - start)
									+ " milliseconds. ");
							logger.info("****************************************");

						} else {

							logger.info("****************************************");
							logger.info(resourceFile.toString() + " finished in " + (System.currentTimeMillis() - start)
									+ " milliseconds. ");
							logger.info("****************************************");
						}
					}
					super.onResponseComplete();
				}

			};
			exchange.setMethod("GET");
			exchange.setURL(resourceUrl);
			exchange.addRequestHeader(HttpHeaders.RANGE, rangesSpecifier(filesize, i, magic));
			exchange.addRequestHeader(HttpHeaders.ACCEPT_ENCODING_BUFFER, HttpHeaderValues.GZIP_BUFFER);
			try {
				_client.send(exchange);
			} catch (IOException e) {
				logger.warn(Log.EXCEPTION, e);
			}
		}

	}

	private String rangesSpecifier(long filesize, int sequence, int number) {
		long rangeSize = filesize / number;
		if (sequence == (number - 1)) {
			return "bytes=" + (rangeSize * sequence) + "-";
		} else {
			return "bytes=" + (rangeSize * sequence) + "-" + (rangeSize * (sequence + 1) - 1);
		}
	}

	private void scheduleRetrieve(String resourceUrl, File scanDir, File resourceFile, long filesize)
			throws FileNotFoundException {
		scheduleRetrieve(resourceUrl, scanDir, resourceFile, filesize, magic);
	}

	private Boolean getBoolean(String string) {
		return (string.startsWith("t") || string.startsWith("T") || string.startsWith("y") || string.startsWith("Y") || string
				.startsWith("1"));
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

	protected void sendAllScans(HttpServletRequest request, HttpServletResponse response, File resource)
			throws IOException {
		byte[] data = null;
		String dir = getListScan(resource);
		if (dir == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No resource");
			return;
		}

		data = dir.getBytes("UTF-8");
		response.setContentType("application/json; charset=UTF-8");
		response.setContentLength(data.length);
		response.getOutputStream().write(data);
	}

	private String getListScan(File resource) throws IOException {

		String[] ls = resource.list(new ScanFilter());
		if (ls == null)
			return null;
		Arrays.sort(ls);

		StringBuilder buf = new StringBuilder(4096);
		// buf.append("{\"num_scans\": " + ls.length + ", \"resource\": \""+
		// host + ":" + resource.getAbsolutePath() + "\" , \"scans\": [");
		buf.append("{\"num_scans\": " + ls.length + ", \"resource\": \"" + resource.getAbsolutePath()
				+ "\" , \"scans\": [");
		for (int i = 0; i < ls.length; i++) {
			if (i != 0)
				buf.append(",");
			buf.append("{\"id\": \"" + ls[i] + "\", \"lastModified\": "
					+ (long) (new File(resource, ls[i]).lastModified()) / 1000 + "}");
		}
		buf.append("]}" + _newline);
		return buf.toString();
	}

	private void sendMd5Check(HttpServletRequest request, HttpServletResponse response, File resource, String type) throws IOException, InterruptedException {
		// check resource against a the md5 representation of the origin scan
		File originJson = new File(resource, ".json");
		if (!originJson.exists() || !originJson.isFile()) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No resource");
			return;
		}
		
		// get the md5 representation from the origin url
		
		Map jsonMap = (Map) JSON.parse(new FileReader(originJson));
		
		String origin_url = (String) jsonMap.get("origin_url");
		
		if (origin_url == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No resource");
			return;
		}
		
		ContentExchange getMd5 = new ContentExchange(false);
        getMd5.setURL(origin_url + "md5sum");
        getMd5.setScheme(HttpSchemes.HTTP_BUFFER);
        getMd5.setVersion(HttpVersions.HTTP_1_1_ORDINAL);
        getMd5.setMethod(HttpMethods.GET);
        _client.send(getMd5);
        int exchangeStatus = getMd5.waitForDone();
        int responseStatus = getMd5.getResponseStatus();

        if (exchangeStatus != HttpExchange.STATUS_COMPLETED) {
            logger.warn(getMd5.toString() + " cannot complete with a status " + exchangeStatus);
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "Cannot get md5sum");
            return;
        }

        if (responseStatus != HttpStatus.OK_200) {
            logger.warn(getMd5.toString() + " status code " + responseStatus);
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "Cannot get md5sum");
            return;
        }
		
        byte[] md5Byte = getMd5.getResponseContentBytes();
		
		ArrayList<String> command = new ArrayList<String>();
		command.add("md5sum");
		command.add("-c");
		ProcessBuilder pBuilder = new ProcessBuilder(command);
		pBuilder.directory(resource);
		Process p = pBuilder.start();
		OutputStream os = p.getOutputStream();
		InputStream is = p.getInputStream();
		InputStream es = p.getErrorStream();
		os.write(md5Byte);
		
		response.setContentType("text/plain; charset=UTF-8");
		IO.copyThread(is, response.getOutputStream());
		
		InputStreamReader esReader = new InputStreamReader(es, "UTF-8");
		StringWriter esWriter=new StringWriter();
		IO.copyThread(esReader, esWriter);
		
		if (p.exitValue() != 0) {
			logger.warn(esWriter.toString());
		}
		
	}

	private void sendMd5(HttpServletRequest request, HttpServletResponse response, File resource, String type)
			throws IOException, ServletException {
		// check .cache directory
		File cacheDir = getCacheDir(_cacheBase);

		if (cacheDir == null) {
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}

		// TODO change to tee output stream
		
		// get md5 from cached response, update it if needed
		String md5 = null;
		try {
			md5 = getMeta(cacheDir, resource, request, response, type, ".md5");
		} catch (InterruptedException e) {
			logger.warn(e);
		}

		byte[] data = null;
		if (md5 != null) {
			data = md5.getBytes("UTF-8");
			response.setContentType("text/plain; charset=UTF-8");
			response.setContentLength(data.length);
			response.getOutputStream().write(data);
		} 

	}

	protected void sendAllItems(HttpServletRequest request, HttpServletResponse response, File resource, String type)
			throws IOException, ServletException {
		byte[] data = null;
		// for details request, send the list directly
		if (request.getParameter("details") != null) {
			String dir = getListImage(resource, type);
			if (dir == null) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND, "No resouce");
				return;
			}

			data = dir.getBytes("UTF-8");
			response.setContentType("application/json; charset=UTF-8");
			response.setContentLength(data.length);
			response.getOutputStream().write(data);
			return;
		}

		// check .cache directory
		File cacheDir = getCacheDir(_cacheBase);

		if (cacheDir == null) {
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}

		// get json from cached response, update it if needed
		String json = null;
		try {
			json = getMeta(cacheDir, resource, request, response, type, ".json");
		} catch (InterruptedException e) {
			logger.warn(e);
		}

		if (json != null) {
			data = json.getBytes("UTF-8");
			response.setContentType("application/json; charset=UTF-8");
			response.setContentLength(data.length);
			response.getOutputStream().write(data);
		}
	}

	// TODO we really need a TEE here
	private String getMeta(File cacheDir, File resource, HttpServletRequest request, HttpServletResponse response,
			String imageType, String metaType) throws ServletException, IOException, InterruptedException {
		File cache = new File(cacheDir, resource.getName() + metaType);
		if (cache.exists() && cache.lastModified() >= resource.lastModified() && cache.length() > 0) {
			// forward to to the file
			String forwardPath = URIUtil.SLASH + cacheDir.getName() + URIUtil.SLASH + cache.getName();
			request.getRequestDispatcher(forwardPath).forward(request, response);
			return null;
		}

		if (!cache.exists() && !cache.createNewFile()) {
			logger.warn("cannot create " + cache.getName());
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return null;
		}

		// generate the representation, and cache it
		String[] ls = resource.list(new ImageFilter(imageType));
		if (ls == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, "No resouce");
			return null;
		}
		Arrays.sort(ls);

		String meta = null;
		if (metaType.equalsIgnoreCase(".json")) {
			// If the first is not valid for the template then forward to details response
			if (!ScanNameUtil.isValid(ls[0])) {
				String forwardPath = getPathInContext(request) + "?details";
				request.getRequestDispatcher(forwardPath).forward(request, response);
				return null;
			}

			ArrayList<ImageSet> setList = new ArrayList<ImageSet>();

			try {
				setList = compressSet(ls);
			} catch (NumberFormatException e) {
				logger.warn("The scan has an image named not in the predefined format.");
				String forwardPath = getPathInContext(request) + "?details";
				request.getRequestDispatcher(forwardPath).forward(request, response);
				return null;
			}

			StringBuilder buf = new StringBuilder(4096);
			buf.append("{\"num_sets\": " + setList.size() + ", \"sets\": [");

			Iterator<ImageSet> i = setList.iterator();
			while (i.hasNext()) {
				ImageSet set = i.next();
				buf.append("{\"name\":\"" + set.getName() + "\",\"start\":" + set.getStart().intValue() + ",\"end\":"
						+ set.getEnd().intValue() + ",\"type\":\"" + set.getType() + "\"");
				if (!set.missingEmpty()) {
					buf.append(",\"missing\":[");
					ArrayList<Integer> missings = set.getMissing();
					Iterator<Integer> m = missings.iterator();
					while (m.hasNext()) {
						buf.append(m.next().intValue());
						if (m.hasNext())
							buf.append(",");
					}
					buf.append("]");
				}
				buf.append("}");
				if (i.hasNext())
					buf.append(",");
			}

			buf.append("]}" + _newline);
			meta = buf.toString();
		}
		if (metaType.equalsIgnoreCase(".md5")) {
			ArrayList<String> command = new ArrayList<String>();
			command.add("md5sum");
			command.addAll(Arrays.asList(ls));
			ProcessBuilder pBuilder = new ProcessBuilder();
			pBuilder.directory(resource);
			pBuilder.redirectErrorStream(true);
			pBuilder.command(command);
			Process p = pBuilder.start();
			InputStream is = p.getInputStream();
			meta = IO.toString(is, "UTF-8");
			if (p.waitFor() != 0) {
				logger.warn(meta);
				meta = null;
			}
		}
		if (meta != null) {
			FileWriter writer = new FileWriter(cache, false); // write the new cache
			writer.write(meta);
			writer.close();
		} else {
			cache.delete();
		}
		return meta;
	}

	private File getCacheDir(File _cacheBase) {
		File cacheDir = new File(_cacheBase, ".cache");
		if (!cacheDir.exists() || !cacheDir.isDirectory()) {
			logger.debug("create the cache dir.");
			if (!cacheDir.mkdir()) {
				logger.warn("failed to create the cache dir.");
				return null;
			}
		}
		return cacheDir;
	}

	protected void sendAllItems(HttpServletRequest request, HttpServletResponse response, File resource)
			throws IOException, ServletException {
		sendAllItems(request, response, resource, _imageType);
	}

	private String getListImage(File resource, String type) throws IOException {
		String[] ls = resource.list(new ImageFilter(type));
		if (ls == null)
			return null;
		Arrays.sort(ls);

		StringBuilder buf = new StringBuilder(4096);
		buf.append("{\"num_images\": " + ls.length + ", \"images\": [");
		for (int i = 0; i < ls.length; i++) {
			if (i != 0)
				buf.append(",");
			buf.append("{\"id\": \"" + ls[i] + "\", \"size\":" + (new File(resource, ls[i])).length() +"}");
		}
		buf.append("]}" + _newline);
		return buf.toString();
	}

	// Make the parser more robust by ScanNameUtil
	private ArrayList<ImageSet> compressSet(String[] ls) throws NumberFormatException {
		int length = ls.length;
		String[][] tokens = new String[length][];
		for (int i = 0; i < length; i++) {
			tokens[i] = ScanNameUtil.parse(ls[i]);
		}

		ArrayList<ImageSet> list = new ArrayList<ImageSet>();

		int startIndex = 0;
		for (int i = 0; i < length; i++) {
			if ((i == length - 1) || !tokens[i + 1][0].equals(tokens[i][0])) {
				// a new name found or that is all
				ImageSet set = new ImageSet();
				set.setName(tokens[i][0]);
				set.setType(tokens[i][2]);
				set.setNumDigit(tokens[i][1].length());
				int setLength = i - startIndex + 1;
				int[] numbers = new int[setLength];
				for (int j = 0; j < setLength; j++) {
					numbers[j] = Integer.parseInt(tokens[startIndex + j][1]);
				}
				Arrays.sort(numbers);
				set.setStart(numbers[0]);
				set.setEnd(numbers[setLength - 1]);
				if ((numbers[setLength - 1] - numbers[0] + 1) != setLength) {
					// some are missing
					for (int n = 0; n < setLength - 1; n++) {
						int diff = numbers[n + 1] - numbers[n];
						if (diff > 1) {
							set.addMissing(numbers[n] + 1, numbers[n + 1] - 1);
						}
					}
				}
				list.add(set);
				startIndex = i + 1;
			}

		}

		return list;

	}

	/**
	 * Filter image directories, hide those starting with "."
	 * 
	 */
	private class ScanFilter implements FilenameFilter {

		public boolean accept(File dir, String name) {
			if (new File(dir, name).isDirectory()) {
				return (!name.startsWith("."));
			} else {
				return false;
			}

		}

	}

	private class ImageSet {

		private String name;
		private Integer start;
		private Integer end;
		private ArrayList<Integer> missing;
		private String type;
		private Integer numDigit; // the number of digits in the file name %d

		public ImageSet() {
			this.name = null;
			this.start = null;
			this.end = null;
			this.type = null;
			this.numDigit = null;
			this.missing = new ArrayList<Integer>();
		}

		public String getName() {
			return name;
		}

		public Integer getStart() {
			return start;
		}

		public Integer getEnd() {
			return end;
		}

		public ArrayList<Integer> getMissing() {
			return missing;
		}

		public String getType() {
			return type;
		}

		public void setName(String name) {
			this.name = name;
		}
		public void setStart(int start) {
			this.start = new Integer(start);
		}

		public void setEnd(int end) {
			this.end = new Integer(end);
		}

		public boolean addMissing(Integer number) {
			return this.missing.add(number);
		}

		public boolean missingEmpty() {
			return this.missing.isEmpty();
		}

		public void setType(String type) {
			this.type = type;
		}

		public void addMissing(int start, int end) {
			for (int i = start; i <= end; i++) {
				this.addMissing(new Integer(i));
			}
		}

		public Integer getNumDigit() {
			return numDigit;
		}

		public void setNumDigit(int numDigit) {
			this.numDigit = new Integer(numDigit);
		}

	}

}
