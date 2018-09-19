/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.utilities;

import edu.si.damsTools.DamsTools;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class StringUtils {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    //returns the filename without the extension (removes everything after LAST '.'
    public static String getExtensionlessFileName(String fileName) {
        
        if (fileName.contains(".")) {
            return fileName.substring(0, fileName.lastIndexOf(".")) ;
        }
        return fileName;
    }
    
    public static String scrubString(String inputString) {
        
        if (inputString == null) {
            return "";
        }
        String newString;
        
        // remove & for easy insert into db
        //newString = inputString.replaceAll("&", "and");
	
        //substitute any 'right' apostrophes to a pair of single quotes
        newString = inputString.replaceAll("\u2019", "'");
       
        //substitute 'em and en dash' for regular dash 
        newString = newString.replaceAll("\u2012", "-");
        newString = newString.replaceAll("\u2013", "-");
        
        //substitute curly double quotes for regular double quotes
        newString = newString.replaceAll("\u201c", "\"");
        newString = newString.replaceAll("\u201d", "\"");
        
        newString = newString.replaceAll("\r", "");
        
	//double any single quotes
	newString = newString.replaceAll("'", "''");
        

        // remove leading and trailing spaces
        newString = newString.trim();
                  
        
        return newString;
        
    }
    
    public static String truncateByByteSize(String inputString, int len) {
        byte[] utf8 = inputString.getBytes();
        if (utf8.length < len) len = utf8.length;
        int n16 = 0;
        int advance = 1;
        int i = 0;
        while (i < len) {
            advance = 1;
            if ((utf8[i] & 0x80) == 0) i += 1;
            else if ((utf8[i] & 0xE0) == 0xC0) i += 2;
            else if ((utf8[i] & 0xF0) == 0xE0) i += 3;
            else { i += 4; advance = 2; }
            
            if (i <= len) n16 += advance;
        }
        
        return inputString.substring(0,n16);
  }

}
