package xrd;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Filter image according to the type
 *
 * 2012-02-01, 11:24:59 AM 
 * use "all" for imageSuffix to select all files 
 * 
 * 2012-03-14, 2:36:45 PM
 * hide all the files which names start with "."
 * 
 */
public class ImageFilter implements FilenameFilter {
    private String type;

    public ImageFilter(String type) {
        super();
        this.type = type;
    }

    public boolean accept(File dir, String name) {
    	if(name.startsWith("."))
    		return false;
        if (new File(dir, name).isFile()) {
        	if (type.equalsIgnoreCase("all")) 
        		return true;
            return type.contains(name.substring(name.lastIndexOf(".") + 1).toLowerCase());
        } else {
            return false;
        }

    }

}
