/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.tms;

import edu.si.damsTools.DamsTools;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet; 
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;
import org.im4java.core.ConvertCmd;
import org.im4java.core.IMOperation;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;

import edu.si.damsTools.cdis.database.CdisMap;

/**
 *
 * @author rfeldman
 */
public class Thumbnail {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
 
    private String damsLocation;
    int fileSize;
    byte[] bytes;
 
    /*  Method :        generate
        Arguments:      
        Description:    Finds the physical image and generates the thumbnail sized image 
        RFeldman 3/2015
    */
    public boolean generate(Integer mapId) {
        
        fileSize = 0;
        bytes = null;
        
        //Get uoiid from mapId
        CdisMap cdisMap = new CdisMap();
        cdisMap.setCdisMapId(mapId);
        cdisMap.populateMapInfo();
        
        //Get image location from DAMS
        boolean locationFound = false;
        if (DamsTools.getProperty("damsImageSize") != null && DamsTools.getProperty("damsImageSize").equals("thumb")) {
            locationFound = getDamsThumbNameLocation (cdisMap.getDamsUoiid());
        }
        else {  
            locationFound = getDamsScreenNameLocation (cdisMap.getDamsUoiid());   
        }    
        
        if (! locationFound) {
            logger.log(Level.FINER, "Not updating thumbnail, image could not be located from database");
            return false;
        }
        
        String sourceImageFile = DamsTools.getProperty("damsRepo") + "/" + this.damsLocation; 
        logger.log(Level.FINER, "Image Location in DAMS repository: " + sourceImageFile);
        
        String tempImage = resizeImage(sourceImageFile, cdisMap.getDamsUoiid());
        if (tempImage == null) {
            logger.log(Level.FINER, "Not updating thumbnail, unable to resize image");
        }
        
        //Capture the image as a binary stream
        try (InputStream is = new BufferedInputStream(new FileInputStream(tempImage)) ) {
            
            this.bytes = IOUtils.toByteArray(is);
            
            this.fileSize = this.bytes.length; 
                    
            logger.log(Level.FINER, "Found DAMS file: " + tempImage + " Size: " + fileSize ); 
            
        } catch(Exception e) {
            logger.log(Level.FINER, "Error, could not obtain thumbnail from binary stream ", e ); 
            return false;
            
	} finally {
            try {
                File thumbFile = new File (tempImage);
                thumbFile.delete();
                
            } catch(Exception e) {
                e.printStackTrace();
            } 
        }
        
        if (! (fileSize > 1 && bytes != null)) {
            logger.log(Level.FINER, "Error: Unable to detect thumbnail image, not updating: " + mapId);
            return false;
        }
        
        logger.log(Level.FINER, "Updating Thumbnail for MapId: " + mapId);
        CdisCisIdentifierMap cdisCisIdentifierMap = new CdisCisIdentifierMap();
        cdisCisIdentifierMap.setCdisMapId(mapId);
        cdisCisIdentifierMap.setCisIdentifierCd("rnd");
        cdisCisIdentifierMap.populateCisIdentifierValueForCdisMapIdType();
        boolean thumbUpdated = update(Integer.parseInt(cdisCisIdentifierMap.getCisIdentifierValue() ) );
            
        return thumbUpdated;
        
    }    
    
    private String resizeImage(String sourceImageFile, String uoiId) {
        
        if (DamsTools.getProperty("damsImageSize") != null && DamsTools.getProperty("damsImageSize").equals("thumb")) {
            
            return sourceImageFile;
            
        }
        else {
            ConvertCmd cmd=new ConvertCmd();
            IMOperation opGenThumbnail = new IMOperation();
        
            //get the current image file specified as the DAMS screen location
            opGenThumbnail.addImage(sourceImageFile);
        
            //Set the resolution and resizing parameters
            //We have to do an extra resize up because these steps are done sequentially
            //and when we resample it reduces the size too much and we ended up with some blurred images....
            opGenThumbnail.resize(9600,9600);
            opGenThumbnail.resample(72);
            opGenThumbnail.resize(192,192);
            opGenThumbnail.units("PixelsPerInch");
            opGenThumbnail.autoOrient();
            opGenThumbnail.unsharp(0.0,1.0);
            opGenThumbnail.colorspace("RGB");
 
            String tempImage = uoiId + ".jpg";
            opGenThumbnail.addImage(tempImage);
        
            // Capture the image as a binary stream
            try {        
                // execute all the above imagemagick commands
                cmd.run(opGenThumbnail);  
            
            } catch(Exception e) {
                logger.log(Level.FINER, "Error, could not obtain thumbnail from DAMS ", e ); 
                return null;
            }
        
            return tempImage;
        }
        
    }
    
    /*  Method :        update
        Arguments:      
        Description:    Updates the CIS database with the thumbnail image
        RFeldman 3/2015
    */
    private boolean update(Integer renditionID) {
       
        //Input the binary stream into the update statement for the table...and execute
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement("update MediaRenditions set ThumbBLOB = ?, ThumbBlobSize = ? " +
                " where RenditionID in (SELECT RenditionID from MediaRenditions where RenditionID =  ? ) ") ) {									   
			
            pStmt.setBytes(1, this.bytes);
            pStmt.setInt(2, this.fileSize);
            pStmt.setInt(3, renditionID);
        
            int recordsUpdated = pStmt.executeUpdate();
            
            if ((recordsUpdated) != 1 ) {
                logger.log(Level.FINER, "ERROR: Thumbnail creation has failed for renditionID: " + renditionID);
                return false;
            }
            else {
                logger.log(Level.FINER, "Thumbnail successfully updated for renditionID: " + renditionID);  
            }
                    
        }catch(Exception e) {
                logger.log(Level.FINER, "ERROR: Thumbnail creation has failed for renditionID: " + renditionID);
		e.printStackTrace();
                return false;
        }       
        return true;                                                  
    }
    
    /*  Method :        getDamsScreenNameLocation
        Arguments:      
        Description:    Obtains the location and name of the screen-size medium resoltion image from the DAMS databasae 
        RFeldman 3/2015
    */
    private boolean getDamsScreenNameLocation (String uoiId) {
        
        String sql = "SELECT o.object_name_location " + 
                     "FROM towner.uois u, " +
                     "     towner.object_stacks o " +
                     "WHERE u.uoi_id = '" + uoiId + "'" +
                     "AND   u.screen_res_obj_id = o.object_id ";
           
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
        
            if(rs.next()) {
                this.damsLocation = rs.getString(1);
            }
            else {
                logger.log(Level.FINEST,"Error: Unable to obtain image location for uoiid: " + uoiId);
                return false;
            }
        } catch(Exception e) {
		e.printStackTrace();
                return false;
	}     
        return true;
        
    }
    
    private boolean getDamsThumbNameLocation (String uoiId) {
        
        String sql = "SELECT o.object_name_location " + 
                     "FROM towner.uois u " +
                     "INNER JOIN towner.object_stacks os " +
                     "ON u.thumb_nail_obj_id = os.object_id " +
                     "WHERE u.uoi_id = '" + uoiId + "'";
                         
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
        
            if(rs.next()) {
                this.damsLocation = rs.getString(1);
            }
            else {
                logger.log(Level.FINEST,"Error: Unable to obtain image location for uoiid: " + uoiId);
                return false;
            }
        } catch(Exception e) {
		e.printStackTrace();
                return false;
	}     
        return true;
        
    }
    
}
