package xrd;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.eclipse.jetty.client.security.HashRealmResolver;
import org.eclipse.jetty.client.security.Realm;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.resource.Resource;

public class ClientRealmUtil {

    /**
     * @param realmResolver
     * @param realms
     *
     * Based on loadUsers() of HashLoginService.
     * @throws IOException
     *
     */
	
    static void addRealms(HashRealmResolver realmResolver, String realms) throws IOException {
        if (realms==null)
            return;
        Resource realmResource = Resource.newResource(realms);
        Logger logger = Log.getRootLogger();

        if (logger.isDebugEnabled()) 
        	logger.debug("Load realms from " + realms);
        Properties properties = new Properties();
        properties.load(realmResource.getInputStream());

        for (Map.Entry<Object, Object> entry : properties.entrySet())
        {
            String username = ((String) entry.getKey()).trim();
            String credentials = ((String) entry.getValue()).trim();
            String realm = null;
            int c = credentials.indexOf('@');
            if (c > 0) {
                realm = credentials.substring(c + 1).trim();
                credentials = credentials.substring(0, c).trim();
            }

            if (username != null && username.length() > 0 && credentials != null && credentials.length() > 0 && realm != null && realm.length() > 0){
                final String pricipal = username;
                final String id = realm;
                final String cred = credentials;
                realmResolver.addSecurityRealm(new Realm() {

                    public String getPrincipal() {
                        return pricipal;
                    }

                    public String getId() {
                        return id;
                    }

                    public String getCredentials() {
                        return cred;
                    }
                });
            }
        }

    }

}
