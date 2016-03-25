/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.CDIS;
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

import edu.si.CDIS.Database.CDISMap;

/**
 *
 * @author rfeldman
 */
public class Thumbnail {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
 
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
        CDISMap cdisMap = new CDISMap();
        cdisMap.setCdisMapId(mapId);
        cdisMap.populateMapInfo();
        
        //Get RenditionID from mapId
        
        boolean locationFound = getDamsNameLocation (cdisMap.getDamsUoiid());   
        if (! locationFound) {
            logger.log(Level.FINER, "Not updating thumbnail, image could not be located from database");
            return false;
        }
 
        String imageFile = "\\\\smb.si-osmisilon1.si.edu\\prodartesiarepo\\" + this.damsLocation; 
        
        logger.log(Level.FINER, "Need to Obtain imageLocation " + imageFile);
        
        ConvertCmd cmd=new ConvertCmd();
        IMOperation opGenThumbnail = new IMOperation();
        
        //get the current image file specified as the DAMS screen location
        opGenThumbnail.addImage(imageFile);
        
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
 
        //save the thumbnail with the new parameters
        String thumbImageName = cdisMap.getDamsUoiid() + ".jpg";
        opGenThumbnail.addImage(thumbImageName);
        
        // Capture the image as a binary stream
        try {        
            // execute all the above imagemagick commands
            cmd.run(opGenThumbnail);  
            
        } catch(Exception e) {
            logger.log(Level.FINER, "Error, could not obtain thumbnail from DAMS ", e ); 
            return false;
        }
       
        //Capture the image as a binary stream
        try (InputStream is = new BufferedInputStream(new FileInputStream(thumbImageName)) ) {
            
            this.bytes = IOUtils.toByteArray(is);
            
            this.fileSize = this.bytes.length; 
                    
            logger.log(Level.FINER, "Found DAMS file: " + thumbImageName + " Size: " + fileSize ); 
            
        } catch(Exception e) {
            logger.log(Level.FINER, "Error, could not obtain thumbnail from binary stream ", e ); 
            return false;
            
	} finally {
            try {
                File thumbFile = new File (thumbImageName);
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
        boolean thumbUpdated = update(Integer.parseInt(cdisMap.getCisUniqueMediaId()) );
            
        if (! thumbUpdated ) {
            return false;
        }  
        
        return true;
        
    }    
    
    /*  Method :        update
        Arguments:      
        Description:    Updates the CIS database with the thumbnail image
        RFeldman 3/2015
    */
    private boolean update(Integer renditionID) {
       
        //Input the binary stream into the update statement for the table...and execute
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement("update MediaRenditions set ThumbBLOB = ?, ThumbBlobSize = ? " +
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
    
    /*  Method :        getDamsLocation
        Arguments:      
        Description:    Obtains the location and name of the screen-size medium resoltion image from the DAMS databasae 
        RFeldman 3/2015
    */
    private boolean getDamsNameLocation (String uoiId) {
        
        String sql = "SELECT o.object_name_location " + 
                     "FROM towner.uois u, " +
                     "     towner.object_stacks o " +
                     "WHERE u.uoi_id = '" + uoiId + "'" +
                     "AND   u.screen_res_obj_id = o.object_id ";
           
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
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
