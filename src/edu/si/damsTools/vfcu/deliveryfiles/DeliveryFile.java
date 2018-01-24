/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.deliveryfiles;

/**
 * Class : DeliveryFile.java
 * Purpose: This Class holds the common methods of all types of files that are delivered to VFCU from the Unit/Vendor
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

        String localPathEnding = "";

        logger.log(Level.FINEST, "sourceNameAndPath: " + sourceNameAndPath.toString());

        //The sourceName and path can have NO localPathEnding.  This is valid condition, and in that case the localPathEnding is an empty string
        //This occurs when VFCU is pointed directly at a particular directory.  We check to make sure this is not an empty string first or the substr may fail.
        if (sourceNameAndPath.toString().length() > DamsTools.getProperty("vendorBaseDir").length() + getFileName().length() +1 )  {
            localPathEnding = sourceNameAndPath.toString().substring(DamsTools.getProperty("vendorBaseDir").length() +1, 
                sourceNameAndPath.toString().length() - getFileName().length() -1 );
        }   
        return localPathEnding;
    }
    
    /*
    /* Method: transferToVfcuStaging
    /* Purpose: Transfers any file from VFCU source area to Vfcu Staging 
    */
    public boolean transferToVfcuStaging(XferType xferType, boolean createMissingDir) {

        String destinationDir = DamsTools.getProperty("vfcuStaging");
        if (! getLocalPathEnding().equals("")) {
            destinationDir = DamsTools.getProperty("vfcuStaging") + '/' + getLocalPathEnding();
        }
      
        String destNameWithPath = destinationDir + "/" + getFileName();
        
        logger.log(Level.FINEST, "Source: " + getFileName());
        logger.log(Level.FINEST, "Destination " + destNameWithPath);
        
        Path source      = sourceNameAndPath;
        Path destination = Paths.get(destNameWithPath);
       
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
