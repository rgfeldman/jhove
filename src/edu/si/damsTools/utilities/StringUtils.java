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
}
