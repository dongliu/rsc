package xrd;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpExchange;
import org.eclipse.jetty.http.HttpHeaders;
import org.eclipse.jetty.http.HttpMethods;
import org.eclipse.jetty.http.HttpSchemes;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpVersions;
import org.eclipse.jetty.io.Buffer;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

public class Retrieve implements Runnable {
	
	private final String source_url;
    private final String target_url;
	private HttpClient _client; 
	private Logger logger = Log.getLogger(Retrieve.class);

	
	
	
	public Retrieve(String source_url, String target_url, HttpClient _client) {
		super();
		this.source_url = source_url;
		this.target_url = target_url;
		this._client = _client;
	}




	@Override
	public void run() {
        logger.info("start retreive " + source_url + " at " + System.currentTimeMillis());
        final ContentExchange sendImage = new ContentExchange(false){
            @Override
            protected synchronized void onResponseComplete() throws IOException {
                super.onRequestComplete();
                logger.info("finish put " + target_url + " at " + System.currentTimeMillis());
            }
        };
        
        ContentExchange retrieveImage = new ContentExchange() {
            @Override
            protected synchronized void onResponseHeader(Buffer name, Buffer value) throws IOException
            {
                super.onResponseHeader(name, value);
                int header = HttpHeaders.CACHE.getOrdinal(name);
                switch (header)
                {
                    case HttpHeaders.CONTENT_LENGTH_ORDINAL:
                        sendImage.setRequestHeader(HttpHeaders.CONTENT_LENGTH_BUFFER, value);
                        break;
                    case HttpHeaders.CONTENT_TYPE_ORDINAL:
                        sendImage.setRequestHeader(HttpHeaders.CONTENT_TYPE_BUFFER, value);
                        break;
                }
            }
        };
        retrieveImage.setURL(source_url);
        retrieveImage.setScheme(HttpSchemes.HTTP_BUFFER);
        retrieveImage.setVersion(HttpVersions.HTTP_1_1);
        retrieveImage.setMethod(HttpMethods.GET);
        try {
            _client.send(retrieveImage);

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            int exchangeStatus = retrieveImage.waitForDone();
            if (exchangeStatus != HttpExchange.STATUS_COMPLETED) {
                logger.warn(retrieveImage.toString() + " cannot complete with a status " + exchangeStatus);
                return;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
                
        int responseStatus = retrieveImage.getResponseStatus();
        if (responseStatus != HttpStatus.OK_200) {
            logger.warn(retrieveImage.toString() + " status code " + responseStatus);
            return;
        }
     
        sendImage.setURL(target_url);
        sendImage.setScheme(HttpSchemes.HTTP_BUFFER);
        sendImage.setVersion(HttpVersions.HTTP_1_1);
        sendImage.setMethod(HttpMethods.PUT);
        sendImage.setRequestContentSource(new ByteArrayInputStream(retrieveImage.getResponseContentBytes()));         
        try {
            _client.send(sendImage);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        
	}	

}
