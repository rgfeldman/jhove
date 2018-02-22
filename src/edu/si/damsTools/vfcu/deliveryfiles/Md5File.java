/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.deliveryfiles;

import edu.si.damsTools.DamsTools;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

/**
 * Class : Md5File.java
 * Purpose: This Class holds the  methods relating to Md5Files that are delivered to VFCU from the Unit/Vendor
 * @author rfeldman
 */
public class Md5File extends DeliveryFile {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final HashMap<String,String> contentsMap;
   
    public Md5File(Path sourceNameAndPath) {
        super(sourceNameAndPath);
        contentsMap = new HashMap<>();
    }
   
    public HashMap<String,String> getContentsMap() {
        return this.contentsMap;
    }

    public boolean populateContentsHashMap() {
    
        LineIterator lt = null;
        try (FileInputStream fin= new FileInputStream(super.sourceNameAndPath.toString())) {
            
            lt = IOUtils.lineIterator(fin, "utf-8");
            
            while(lt.hasNext()) {
                
                try {
                    String line=lt.nextLine();
                    
                    String pattern = "(\\S+)\\s+(\\S.*)";
                    Pattern r = Pattern.compile(pattern);
                
                    Matcher m = r.matcher(line);
                
                    if (m.matches()) {
                        boolean excludeFile = excludeMediaFiles(m.group(2).trim());
                            if (excludeFile) {
                            logger.log(Level.FINEST, "File Excluded: " + m.group(2).trim()); 
                            continue;
                        }
                        contentsMap.put(m.group(1).toLowerCase(),m.group(2).trim());

                    }
                } catch (Exception e) {
                    logger.log(Level.FINEST, "Error reading line from md5 file " + super.sourceNameAndPath.toString() + e);
                    return false;
                }
            }
        
        } catch (Exception e) {
            logger.log(Level.FINEST, "Error reading md5 file " + super.sourceNameAndPath.toString() + e);
            return false;
        } finally {
            if (lt!= null) {
                lt.close();
            }
        }
        return true;        
    }
    
    public boolean excludeMediaFiles(String fileName) {
        //Skip the line if he filename is Thumbs.Db.  It is a temp windows generated file Do not even add it to Database
        if (fileName.equals("Thumbs.db")) {
            return true;
        }
        if (fileName.equals(".DS_Store")) {
            return true;
        }
        
        //skip any filenames that end with .md5 extension
        return fileName.endsWith(".md5");
    }
}
