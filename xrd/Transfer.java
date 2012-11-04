package xrd;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

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
import org.eclipse.jetty.util.IO;
import org.eclipse.jetty.util.URIUtil;
import org.eclipse.jetty.util.ajax.JSON;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

public class Transfer implements Runnable {

    private final String scan_url;
    private final String image_prefix;
    private final String image_suffix;
    private final Long image_start;
    private final Long image_end;
    private final Long digit_number;
    private final String patch;
    private final String image_all;
    private final Long arrived;
    private Long started;
    private Long finished;
    private final File scan_dir;
    private HttpClient _client;
    private ThreadPoolExecutor _decodeExecutor;
    private final String json;
    private int total;
    private int cancelled;
    private AtomicInteger renaming_success;
    private AtomicInteger decoding_renaming;
    private AtomicInteger status; // 0: init 1: transferring 2: transfer finished 3: decoding and renaming finished
    
    private ConcurrentHashMap<File, Transfer> _submittedTask;
    private LinkedBlockingQueue<Transfer> _executedTask;
    private Logger logger = Log.getLogger(Transfer.class);

    public static Transfer createTransfer(Long arrived, Map request, File scan_dir, HttpClient client, ThreadPoolExecutor executor, ConcurrentHashMap<File, Transfer> submitted, LinkedBlockingQueue<Transfer> executed){
    		Transfer task = new Transfer(arrived, request, scan_dir, client, executor, submitted, executed);
    		if (task.equals(submitted.putIfAbsent(scan_dir, task)))
    			return task;
    		else
    			return null;
    }
    
    private Transfer(Long arrived, Map request, File scan_dir, HttpClient client, ThreadPoolExecutor executor, ConcurrentHashMap<File, Transfer> submitted, LinkedBlockingQueue<Transfer> executed) {
        this.arrived = arrived;
        this.scan_dir = scan_dir;
        this.scan_url = request.containsKey("scan_url")?(String)request.get("scan_url"):null;
        this.image_prefix = request.containsKey("image_prefix")?(String)request.get("image_prefix"):null;
        this.image_suffix = request.containsKey("image_suffix")?(String)request.get("image_suffix"):null;
        this.patch = request.containsKey("patch")?(String)request.get("patch"):null;
        this.image_all = request.containsKey("image_all")?(String)request.get("image_all"):null;
        this.image_start = request.containsKey("image_start")?(Long)request.get("image_start"):null;
        this.image_end = request.containsKey("image_end")?(Long)request.get("image_end"):null;
        this.digit_number = request.containsKey("digit_number")?(Long)request.get("digit_number"):new Long(0);
        this._client = client;
        this._decodeExecutor = executor;
        this._submittedTask = submitted;
        this._executedTask = executed;
        this.json = JSON.toString(request);
        this.total = 0;
        this.cancelled = 0;
        this.renaming_success = new AtomicInteger(0);
        this.status = new AtomicInteger(0);
    }



    public void run() {
    	incrementStatus();
        if (this.image_all == null) {
            try {
                retrieve(scan_url, scan_dir, image_suffix, image_prefix, image_start.intValue(), image_end.intValue(), digit_number.intValue(), patch);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        } else {
            try {
                retrieve(scan_url, scan_dir, image_suffix, image_prefix, digit_number.intValue(), patch);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String getScan_url() {
        return scan_url;
    }

    public String getImage_prefix() {
        return image_prefix;
    }

    public String getImage_suffix() {
        return image_suffix;
    }

    public Long getImage_start() {
        return image_start;
    }

    public Long getImage_end() {
        return image_end;
    }

    public Long getDigit_number() {
        return digit_number;
    }

    public String getPatch() {
        return patch;
    }

    public String getImage_all() {
        return image_all;
    }


    public String getJson() {
        return json;
    }

    public String getProgress() {
        Map json = new HashMap();
        json.put("arrived", arrived);
        json.put("started", started);
        json.put("total", new Integer(total));
        json.put("cancelled", new Integer(cancelled));
        json.put("success", new Integer(renaming_success.get()));
        return JSON.toString(json);
    }
    
    public String getInfo() {
        Map json = new HashMap();
        json.put("arrived", arrived);
        json.put("started", started);
        json.put("finished", finished);
        json.put("total", new Integer(total));
        json.put("cancelled", new Integer(cancelled));
        json.put("success", new Integer(renaming_success.get()));
        return JSON.toString(json);
    }

    /**
     * Fetch the files specified by {@code imageSuffice}, {@code imagePrefix},
     * {@code imageStart} and {@code imageEnd} from {@code scanUrl} according to
     * the option {@code patch}. If {@code patch} is "overwrite" then the
     * existing file
     * with the same name will be overwritten. That is, the old one is deleted
     * when the new one is transferred. If {@code patch} is "new" then only the
     * files not existing in {@code scanDir} will be transferred.
     *
     * @param scanUrl
     * @param scanDir
     * @param imageSuffix
     * @param imagePrefix
     * @param imageStart
     * @param imageEnd
     * @param digitNumber
     * @param patch
     * @throws FileNotFoundException
     */
    private void retrieve(final String scanUrl, final File scanDir,
            String imageSuffix, String imagePrefix, int imageStart,
            int imageEnd, int digitNumber, String patch)
            throws FileNotFoundException {
        logger.info("****************************************");
        logger.info("start transfering images from " + scanUrl + " at " + System.currentTimeMillis());
        logger.info("****************************************");
        // create the list of url's and let _client to send the GET tasks
        int number = imageEnd - imageStart + 1; // number of images
        this.total = number;
        ArrayList<String> urls = new ArrayList<String>((int) number);

        // clean the url list for new patch
        if (patch != null && patch.equalsIgnoreCase("new")) {
            String[] ls = scanDir.list(new ImageFilter(imageSuffix));
            Arrays.sort(ls);
            String filename;
            if (digitNumber != 0) {
                NumberFormat nf = NumberFormat.getInstance();
                nf.setMinimumIntegerDigits(digitNumber);
                nf.setMaximumIntegerDigits(digitNumber);
                nf.setGroupingUsed(false);

                for (long i = imageStart; i <= imageEnd; i++) {
                    String filename_foxmas = imagePrefix + i + imageSuffix;
                    filename = imagePrefix + nf.format(i) + imageSuffix;
                    if (Arrays.binarySearch(ls, filename_foxmas) < 0)
                        urls.add(URIUtil.encodePath(URIUtil.addPaths(scanUrl, filename)));
                }
            } else {
                for (long i = imageStart; i <= imageEnd; i++) {
                    filename = imagePrefix + i + imageSuffix;
                    if (Arrays.binarySearch(ls, filename) < 0)
                        urls.add(URIUtil.encodePath(URIUtil.addPaths(scanUrl, filename)));
                }
            }
        } else {
            // TODO patch based on md5
            if (digitNumber != 0) {
                NumberFormat nf = NumberFormat.getInstance();
                nf.setMinimumIntegerDigits(digitNumber);
                nf.setMaximumIntegerDigits(digitNumber);
                nf.setGroupingUsed(false);

                for (long i = imageStart; i <= imageEnd; i++) {
                    urls.add(URIUtil.encodePath(URIUtil.addPaths(scanUrl, imagePrefix + nf.format(i) + imageSuffix)));
                }
            } else {
                for (long i = imageStart; i <= imageEnd; i++) {
                    urls.add(URIUtil.encodePath(URIUtil.addPaths(scanUrl, imagePrefix + i + imageSuffix)));
                }
            }
        }

        retriveUrlList(urls, scanDir, imagePrefix, imageSuffix, digitNumber);
    }

    /**
     * Try to get all the images in {@code scanUrl}. This is done by first
     * sending a {@code GET scanURL} to
     * get the list of images, then issue GET's for each item in the list with
     * the name pattern {@code imagePrefix.*imageSuffic}. 
     *
     * @param scanUrl
     * @param scanDir
     * @param imageSuffix
     * @param imagePrefix
     * @param patch
     * @throws InterruptedException
     * @throws IOException
     */
    private void retrieve(String scanUrl, File scanDir, String imageSuffix,
            String imagePrefix, int digitNumber, String patch)
            throws InterruptedException, IOException {
        logger.info("****************************************");
        logger.info("start transfering images from " + scanUrl + " at " + System.currentTimeMillis());
        logger.info("****************************************");
        ContentExchange getImages = new ContentExchange(false);
        // the detail parameter tells the other side to give a representation of detailed list
        getImages.setURL(scanUrl + "?type=" + imageSuffix + "&details");
        getImages.setScheme(HttpSchemes.HTTP_BUFFER);
        getImages.setVersion(HttpVersions.HTTP_1_1_ORDINAL);
        getImages.setMethod(HttpMethods.GET);
        _client.send(getImages);
        int exchangeStatus = getImages.waitForDone();
        int responseStatus = getImages.getResponseStatus();

        if (exchangeStatus != HttpExchange.STATUS_COMPLETED) {
            logger.warn(getImages.toString() + " cannot complete with a status " + exchangeStatus);
            return;
        }

        if (responseStatus != HttpStatus.OK_200) {
            logger.warn(getImages.toString() + " status code " + responseStatus);
            return;
        }

        // Got the list of images and compose the url list
        ArrayList<String> urls = new ArrayList<String>();
        Map imagesMap = (Map) JSON.parse(getImages.getResponseContent());
        Object[] images = (Object[]) imagesMap.get("images");

        this.total = images.length;

        // clean the url list for new patch
        if (patch != null && patch.equalsIgnoreCase("new")) {
            String[] ls = scanDir.list(new ImageFilter(imageSuffix));
            Arrays.sort(ls);
            for (Object image : images) {
                String id = (String) ((Map) image).get("id");
                if (id.startsWith(imagePrefix) && Arrays.binarySearch(ls, id) < 0) {
                    urls.add(URIUtil.encodePath(URIUtil.addPaths(scanUrl, id)));
                }
            }

        } else {
        	// TODO patch based on md5
            for (Object image : images) {
                String id = (String) ((Map) image).get("id");
                if (id.startsWith(imagePrefix)) {
                    urls.add(URIUtil.encodePath(URIUtil.addPaths(scanUrl, id)));
                }
            }
        }

        retriveUrlList(urls, scanDir, imagePrefix, imageSuffix, digitNumber);
    }



    /**
     * Retrieve all the files listed in {@code urls} and write them to {@code
     * scanDir}.
     *
     * @param urls
     * @param scanDir
     * @param imagePrefix
     * @param imageSuffix
     */
    private void retriveUrlList(ArrayList<String> urls, final File scanDir,
            final String imagePrefix, final String imageSuffix,
            final int digitNumber) {
    	 this.cancelled = this.total - urls.size();
         if (urls.isEmpty()) {
             setFinished(new Long(System.currentTimeMillis()));
             incrementStatus(); 
             _executedTask.offer(_submittedTask.remove(scanDir));
             logger.info("****************************************");
             logger.info("No image needs to be transferred from " + this.scan_url + " at " + System.currentTimeMillis());
             logger.info("****************************************");
             return;
         }
    	
        final ConcurrentHashMap<String, Integer> statusMap = new ConcurrentHashMap<String, Integer>();
        final int n = urls.size();
        for (final String url : urls) {
            // use HttpExchange instead of CachedExchange or ContentExchang to get better performance
            HttpExchange exchange = new HttpExchange() {
                private final HttpFields _responseFields = new HttpFields();
                private volatile int _responseStatus;
                private File _tmpFile;
                private FileOutputStream _fileStream;
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

                protected void onResponseHeader(Buffer name, Buffer value)
                        throws IOException {
                    _responseFields.add(name, value);
                    super.onResponseHeader(name, value);
                }

                protected void onResponseStatus(Buffer version, int status,
                        Buffer reason) throws IOException {
                    _responseStatus = status;
                    if (status == HttpStatus.OK_200) {
                        
                        String original = url.substring(url.lastIndexOf("/") + 1);
                        // do not change the name format 
                        // format the file's name to foxmas style if the digit number is not zero
                        // AND the service knows about the image prefix
                        /*String foxmasName = null;
                        if (digitNumber != 0 && !imagePrefix.endsWith("")) {
                            foxmasName = original.replaceFirst(imageSuffix + "$", "");
                            foxmasName = foxmasName.replaceFirst("^" + imagePrefix, "");
                            int number = Integer.parseInt(foxmasName);
                            foxmasName = imagePrefix + number + imageSuffix;
                        } else {
                            foxmasName = original;
                        }

                        _tmpFile = new File(scanDir, foxmasName + ".tmp");*/
                        _tmpFile = new File(scanDir, original + ".tmp");
                        _fileStream = new FileOutputStream(_tmpFile);
                    } 
                    super.onResponseStatus(version, status, reason);
                }

                protected void onResponseContent(Buffer content) {
                    if (_responseStatus == HttpStatus.OK_200) {
                        try {
                            content.writeTo(_fileStream);
                        } catch (IOException e) {
                            logger.warn(e);
                        }
                    } else {
                        logger.warn(this.toString() + " " + this.getMethod() + " " + this.getRequestURI() + " results " + _responseStatus);
                    }
                }

                protected void onResponseComplete() throws IOException {
                    if (_responseStatus == HttpStatus.OK_200) {
                        _fileStream.close();
                        final String encoding = _responseFields.getStringField(HttpHeaders.CONTENT_ENCODING_BUFFER);
                        final File target = new File(_tmpFile.getPath().replaceFirst(".tmp$", ""));
                        // create a job and submitted to the executor
                        Runnable decode = new Runnable() {

                            public void run() {
                                if (encoding != null && encoding.indexOf(HttpHeaderValues.GZIP) != -1) {

                                    // decompress the file and remove the tmp file
                                    FileOutputStream targetos = null;
                                    FileInputStream inStream = null;
                                    GZIPInputStream gis = null;
                                    try {
                                        targetos = new FileOutputStream(target);
                                        inStream = new FileInputStream(_tmpFile);
                                        gis = new GZIPInputStream(inStream);
                                        IO.copy(gis, targetos);
                                        addSuccess();
                                    } catch (IOException e) {
                                        logger.warn(e);
                                    } finally {
                                        IO.close(gis);
                                        IO.close(inStream);
                                        IO.close(targetos);
                                        if (!_tmpFile.delete()) {
                                            logger.warn(_tmpFile.getPath() + " is not deleted.");
                                        }
                                    }
                                } else {
                                    // rename the tmp file
                                    if (_tmpFile.renameTo(target)) {
                                        addSuccess();
                                    } else {
                                        logger.warn(_tmpFile.getName() + " is not renamed to " + target.getName());
                                    }
                                }
                                if(addEncoding() == total) {
                                	incrementStatus();
                                	logger.info("****************************************");
                                    logger.info("" + n + " files decoding and renaming finished at " + System.currentTimeMillis());
                                    logger.info("" + renaming_success.get() + " files decoded and renamed successfully.");
                                    logger.info("****************************************");
                                	_executedTask.offer(_submittedTask.remove(scanDir));
                                }
                            }
                        };

                        _decodeExecutor.execute(decode);

                    }

                    statusMap.putIfAbsent(url, new Integer(_responseStatus));

                    if (statusMap.size() == n) {
                        setFinished(new Long(System.currentTimeMillis()));
                        incrementStatus();
                        logger.info("****************************************");
                        logger.info("" + n + " files transfer finished at " + System.currentTimeMillis());
                        Enumeration<Integer> status = statusMap.elements();
                        int success = 0;
                        while (status.hasMoreElements()) {
                            success = success + (status.nextElement().intValue() == 200
                                    ? 1 : 0);
                        }
                        logger.info("" + success + " files transfer succeeded.");
                        logger.info("" + (n - success) + " files failed.");
                        logger.info("****************************************");
                    }
                    super.onResponseComplete();
                }

            };
            exchange.setMethod("GET");
            exchange.addRequestHeader(HttpHeaders.ACCEPT_ENCODING_BUFFER, HttpHeaderValues.GZIP_BUFFER);
            exchange.setURL(url);
            try {
                _client.send(exchange);
            } catch (IOException e) {
                logger.warn(Log.EXCEPTION, e);
            }
        }

    }

    public int getTotal() {
        return total;
    }

    public int getSuccess() {
        return renaming_success.get();
    }

    private int addSuccess() {
        return renaming_success.addAndGet(1);
    }

    private int addEncoding() {
        return decoding_renaming.addAndGet(1);
    }

    public Long getStart() {
        return arrived;
    }

    public Long getStarted() {
        return started;
    }

    public Long getFinished() {
        return finished;
    }

    public void setStarted(Long started) {
        this.started = started;
    }

    public void setFinished(Long finished) {
        this.finished = finished;
    }
    
    public int getStatus() {
    	return this.status.get();
    }

    public void incrementStatus() {
    	this.status.addAndGet(1);
    }
}
