package xrd;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpMethods;
import org.eclipse.jetty.io.ByteArrayBuffer;
import org.eclipse.jetty.util.URIUtil;
import org.eclipse.jetty.util.ajax.JSON;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;



public class Watch implements Runnable {
    
    private final File base;
    private final String target_url;
    private final String scanId;

    
    private ConcurrentHashMap<String, Long> watchingImage;
    private ArrayList<String> knownImage;
    
    private HttpClient _client; 
    private ScheduledThreadPoolExecutor pollingExecutor;
//    private ThreadPoolExecutor _sendExecutor;
    private int size;
    private Logger logger = Log.getLogger(Watch.class);
    
    private String _imageType = "spe,tif";
    private String host;
    private int port;
    
    


    public Watch(File base, String scanId, String host, int port, String target_url, HttpClient _client,
            ScheduledThreadPoolExecutor _pollingExecutor,
            int size) {
        this.base = base;
        this.scanId = scanId;
        this.host = host;
        this.port = port;
        this.target_url = target_url;
        this._client = _client;
        this.pollingExecutor = _pollingExecutor;
        this.size = size;
        this.watchingImage = new ConcurrentHashMap<String, Long>();
        this.knownImage = new ArrayList<String>();
    }



    public void run() {
        
        logger.debug("************************************************");
        logger.debug("poll the image list at " + scanId + " at " + System.currentTimeMillis());
        logger.debug("************************************************");
        
        // TODO check if the scan exist, and cancel the task after 2*mapsize*period seconds
        String[] ls = (new File(base, scanId)).list(new ImageFilter(_imageType));
        if (ls == null) {
            return;
        }
        Arrays.sort(ls);

        ArrayList<String> ids = new ArrayList<String>();
        for (String id : ls) {
            if (knownImage.contains(id)) {
                if (watchingImage.containsKey(id)) {
                    // under watching 
                    // update the current size
                    long size = new Long((new File(base, id)).length());
                    if (watchingImage.put(id, new Long(size)).longValue() == size) {
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
                watchingImage.put(id, new Long((new File(base, id)).length()));
                logger.debug("add a watch for " + id);
            }
       }
        // submit urls to send executor in a loop
        for (String id : ids) {
            String target = URIUtil.encodePath(URIUtil.addPaths(target_url, id));
            ContentExchange putImage = new ContentExchange();
            putImage.setURL(target);
            putImage.setMethod(HttpMethods.PUT);
            putImage.setRequestContentType("application/json");
            
            
            Map<String, String> requestMap = new HashMap<String, String>();
            try {
                requestMap.put("resource_url", (new URL("http", host, port, URIUtil.SLASH+"_scans"+URIUtil.SLASH+ scanId +URIUtil.SLASH+id)).toString());
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
            String message = JSON.toString(requestMap);
            putImage.setRequestContent(new ByteArrayBuffer(message));
            logger.info("************************************************");
            logger.info("start transferring " + id + " at " + System.currentTimeMillis());
            logger.info("************************************************");
            try {
                _client.send(putImage);
            } catch (IOException e) {
                e.printStackTrace();
            }            
            
        }

        if (knownImage.size() == size && watchingImage.size() == 0) {
            logger.info("stop polling now. ");
            pollingExecutor.shutdown();
        }

    }

}
