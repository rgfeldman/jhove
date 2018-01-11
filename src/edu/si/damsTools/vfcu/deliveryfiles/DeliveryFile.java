/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.deliveryfiles;

/**
 *
 * @author rfeldman
 */
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.files.xferType.XferType;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeliveryFile {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    Path sourceNameAndPath;

    public DeliveryFile(Path sourceNameAndPath) {
        this.sourceNameAndPath = sourceNameAndPath;
    }
    
    public String getFileName() {
        return this.sourceNameAndPath.getFileName().toString();
    }
    
    public String getLocalPathEnding() {
        return sourceNameAndPath.toString().substring(DamsTools.getProperty("vendorBaseDir").length());
    }
    
    public boolean transferToDAMSStaging(XferType xferType, boolean createMissingDir) {

        String destinationDir = DamsTools.getProperty("vfcuStagingForCDIS") + '/' + getLocalPathEnding();
        String dest = destinationDir + "/" + getFileName();
        logger.log(Level.FINEST, "Source: " + getFileName());
        logger.log(Level.FINEST, "Destination " + dest);
        
        Path source      = sourceNameAndPath;
        Path destination = Paths.get(dest);
       
        try {
            if (createMissingDir) {
           
                Path newDirectory = Paths.get(destinationDir);  
                logger.log(Level.FINEST, "New Folder  " + destinationDir);
             
                Files.createDirectories(newDirectory);
            }
           
            boolean fileXfered = xferType.xferFile(source, destination);
                if (!fileXfered) {
                return false;
            }
            
            //Now that we have moved the file, our file of reference is the file we just moved.  point file to the destination area
            this.sourceNameAndPath = destination;
                
        }
        catch(Exception e) {
             logger.log(Level.FINEST, "Unable to transfer to staging", e);
        }
        
        return true;
    }

}
