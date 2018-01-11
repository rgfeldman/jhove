/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.deliveryfiles;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.database.VfcuActivityLog;
import edu.si.damsTools.vfcu.database.VfcuMd5File;
//import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

/**
 *
 * @author rfeldman
 */
public class Md5File extends DeliveryFile {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Path sourceNameAndPath;
    private HashMap<String,String> contentsMap;
   
    public Md5File(Path sourceNameAndPath) {
        super(sourceNameAndPath);
        contentsMap = new HashMap<>();
    }
   
    public HashMap<String,String> getContentsMap() {
        return this.contentsMap;
    }

    public boolean populateContentsHashMap() {
    
        LineIterator lt = null;
        try (FileInputStream fin= new FileInputStream(this.sourceNameAndPath.toString())) {
            
            lt = IOUtils.lineIterator(fin, "utf-8");
            
            while(lt.hasNext()) {
                
                try {
                    String line=lt.nextLine();
                    
                    String pattern = "(\\S+)\\s+(\\S.*)";
                    Pattern r = Pattern.compile(pattern);
                
                    Matcher m = r.matcher(line);
                
                    if (m.matches()) {
                        contentsMap.put(m.group(1).toLowerCase(),m.group(2).trim());

                    }
                } catch (Exception e) {
                    logger.log(Level.FINEST, "Error reading line from md5 file " + sourceNameAndPath.toString() + e);
                    return false;
                }
            }
        
        } catch (Exception e) {
            logger.log(Level.FINEST, "Error reading md5 file " + sourceNameAndPath.toString() + e);
            return false;
        } finally {
            if (lt!= null) {
                lt.close();
            }
        }
        return true;
        
    }
    
}
