/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.deliveryfiles;

import com.twmacinta.util.MD5;

import edu.si.damsTools.vfcu.utilities.JhoveConnection;
import java.io.File;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;


import edu.si.damsTools.DamsTools;


/**
 *
 * @author rfeldman
 */
public class MediaFile extends DeliveryFile {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String md5Hash;
    private String mediaFileDate;
    private String mediaFileSize;
    boolean jhoveValidated = false;

    public MediaFile (Path sourceNameAndPath) {
        super(sourceNameAndPath);
    }
    
    public String getMd5Hash() {
        return this.md5Hash;
    }
    
    public String getMediaFileDate() {
        return this.mediaFileDate;
    }
    
    public String getMediaFileSize() {
        return this.mediaFileSize;
    }
    
    public boolean retJhoveValidated() {
        return this.jhoveValidated;
    }

    public boolean populateAttributes () {
        
        Path mediaFile      = super.sourceNameAndPath;
        
        try {
            BasicFileAttributes attr = Files.readAttributes(mediaFile, BasicFileAttributes.class);    
            this.mediaFileDate = attr.lastModifiedTime().toString().substring(0,10);    
            this.mediaFileSize = Long.toString(attr.size());
            
            //uses java fast md5
            MD5 md5 = new MD5();
            md5Hash = md5.asHex(md5.getHash(new File(mediaFile.toString())));

            logger.log(Level.FINER, "FAST Md5 hash value: " + this.md5Hash);
            
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain file attributes", e );
                return false;
        }
        return true;
    }
    
    boolean zeroByteChecksumVldt() {
        return !(mediaFileSize == null || mediaFileSize.equals("0") || md5Hash.equals("d41d8cd98f00b204e9800998ecf8427e"));
    }
     
    
    public String validate() {
                        
        //make sure the checksum is not equivalent to a zero-byte file
        boolean validFileSize = zeroByteChecksumVldt();
        if (!validFileSize) {
            return "ZBF";
        }
 
        JhoveConnection jhoveConnection = new JhoveConnection();
        boolean jhoveCheckRequired = jhoveConnection.populateRequiredData(super.sourceNameAndPath.toString());  
        // perform jhove validation if needed 
        if (jhoveCheckRequired) {          
            
            boolean jhoveValidationSuccess = jhoveConnection.jhoveValidate(super.sourceNameAndPath.toString());
            
            if (!jhoveValidationSuccess) {
                return "JHV";
            }
            jhoveValidated = true;
        }
            
        return null;
       
    }
}
