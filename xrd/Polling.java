package xrd;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpExchange;
import org.eclipse.jetty.http.HttpMethods;
import org.eclipse.jetty.http.HttpSchemes;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpVersions;
import org.eclipse.jetty.util.URIUtil;
import org.eclipse.jetty.util.ajax.JSON;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

public class Polling implements Runnable {
    
    private final String source_url;
    private final String target_url;

    
    private ConcurrentHashMap<String, Long> watchingImage;
    private ArrayList<String> knownImage;
    
    private HttpClient _client; 
    private ScheduledThreadPoolExecutor pollingExecutor;
    private ThreadPoolExecutor _sendExecutor;
    private int size;
    private Logger logger = Log.getLogger(Polling.class);
    
    


    public Polling(String source_url, String target_url, HttpClient _client,
			ScheduledThreadPoolExecutor _pollingExecutor,
			ThreadPoolExecutor _sendExecutor, int size) {
		this.source_url = source_url;
		this.target_url = target_url;
		this._client = _client;
		this.pollingExecutor = _pollingExecutor;
		this._sendExecutor = _sendExecutor;
		this.size = size;
		this.watchingImage = new ConcurrentHashMap<String, Long>();
		this.knownImage = new ArrayList<String>();
	}

    public void run() {
        logger.debug("************************************************");
        logger.debug("poll the image list from " + this.source_url + " at " + System.currentTimeMillis());
        logger.debug("************************************************");
        ContentExchange getImages = new ContentExchange(false);
        getImages.setURL(source_url+"?details");
        getImages.setScheme(HttpSchemes.HTTP_BUFFER);
        getImages.setVersion(HttpVersions.HTTP_1_1_ORDINAL);
        getImages.setMethod(HttpMethods.GET);
        try {
            _client.send(getImages);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        int exchangeStatus = HttpExchange.STATUS_START;
        try {
            exchangeStatus = getImages.waitForDone();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
        ArrayList<String> ids = new ArrayList<String>();
        Map imagesMap = null;
        try {
            imagesMap = (Map) JSON.parse(getImages.getResponseContent());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        Object[] images = (Object[]) imagesMap.get("images");

        for (Object image : images) {
            String id = (String) ((Map) image).get("id");
            Long size = (Long) ((Map) image).get("size");
            if (knownImage.contains(id)) {
                if (watchingImage.containsKey(id)) {
                    // under watching 
                    // update the current size
                    if (watchingImage.put(id, size).longValue() == size.longValue()) {
                        // the size has not changed from the last poll
                        ids.add(id);
                        // stop watching
                        watchingImage.remove(id);
                        logger.debug("remove a watch for " + id);
                    }
                }
            } else {
                knownImage.add(id);
                // start watching
                watchingImage.put(id, size);
                logger.debug("add a watch for " + id);
            }
       }
        // submit urls to send executor in a loop
        for (String id : ids) {
            String from = URIUtil.encodePath(URIUtil.addPaths(source_url, id));
            String to = URIUtil.encodePath(URIUtil.addPaths(target_url, id));
            Retrieve task = new Retrieve(from, to, _client);
            _sendExecutor.execute(task);
        }

        if (knownImage.size() == size && watchingImage.size() == 0) {
            logger.info("stop polling now. ");
            pollingExecutor.shutdown();
        }

    }

}
