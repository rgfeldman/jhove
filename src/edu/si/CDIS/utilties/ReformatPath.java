/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.utilties;

import edu.si.CDIS.CDIS;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class ReformatPath {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    public String reformatPathMS (String pathName) {
        
        logger.log(Level.FINER, "PathName to reformat: " + pathName );
        
        try {
            if (pathName.contains("/")) {
                pathName = pathName.replace("/", "\\");
            }
            if (pathName.contains("\\\\")) {
                pathName = pathName.replace("\\\\", "\\"); 
            }
        
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to reformat fileName", e);
        }
        
        //need to add an extra '\' if it the '\' starts the pathname, it is likely start with the server name.  In MSDOS, server name is preceded with '\\'
        if (pathName.startsWith("\\")) {
            pathName = "\\" + pathName;
        }
        
        logger.log(Level.FINER, "Reformatted pathName: " + pathName );
        
        return pathName;
        
    }
}
