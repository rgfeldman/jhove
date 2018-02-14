/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.utilities;

/**
 *
 * @author rfeldman
 */
public class StringUtils {
    
    //returns the filename without the extension (removes everything after LAST '.'
    public static String getExtensionlessFileName(String fileName) {
        
        if (fileName.contains(".")) {
            return fileName.substring(0, fileName.lastIndexOf(".")) ;
        }
        return fileName;
    }
    
    public static String scrubString(String inputString) {
          
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
        
        newString = newString.replaceAll("\r\n", "\n");
        
	//double any single quotes
	newString = newString.replaceAll("'", "''");
        
        // remove leading and trailing spaces
        newString = newString.trim();
        
        return newString;
        
    }
}
