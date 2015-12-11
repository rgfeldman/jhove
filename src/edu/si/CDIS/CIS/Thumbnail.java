/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.CIS.Database.CDISTable;

import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.LinkedHashMap;

import org.apache.commons.io.IOUtils;
import org.im4java.core.ConvertCmd;
import org.im4java.core.IMOperation;



public class Thumbnail {

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private byte[] bytes;
    private String damsImageNameLocation;
    private int fileSize;
    private String uoiid;
    
    private LinkedHashMap <Integer,String> thumbnailsToSync;  
    
    private void addthumbnailsToSync (Integer renditionID, String UOIID) {
        this.thumbnailsToSync.put(renditionID, UOIID); 
    }
     
    /*  Method :        getDamsLocation
        Arguments:      
        Description:    Obtains the location and name of the screen-size medium resoltion image from the DAMS databasae 
        RFeldman 3/2015
    */
    private boolean getDamsNameLocation () {
        
        String sql = "select o.object_name_location from uois u, object_stacks o" +
        " where u.uoi_id = '" + this.uoiid + "'" +
        " and u.screen_res_obj_id = o.object_id ";
           
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
        
            if(rs.next()) {
                this.damsImageNameLocation = rs.getString(1);
            }
            else {
                logger.log(Level.FINEST,"Error: Unable to obtain image location for uoiid: " + this.uoiid);
                return false;
            }
        } catch(Exception e) {
		e.printStackTrace();
                return false;
	}     
        return true;
        
    }
    
    /*  Method :        generate
        Arguments:      
        Description:    Finds the physical image and generates the thumbnail sized image 
        RFeldman 3/2015
    */
    public boolean generate(String uoiid, Integer renditionId) {
        
        this.uoiid = uoiid;
        String imageFile = null;
        this.fileSize = 0;
        this.bytes = null;
        
        boolean locationFound = getDamsNameLocation ();   
        if (! locationFound) {
            logger.log(Level.FINER, "Not updating thumbnail, image could not be located from database");
            return false;
        }
 
        imageFile = "\\\\smb.si-osmisilon1.si.edu\\prodartesiarepo\\" + this.damsImageNameLocation; 
        
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
        String thumbImageName = uoiid + ".jpg";
        opGenThumbnail.addImage(thumbImageName);
        
        // Capture the image as a binary stream
        try {        
            // execute all the above imagemagick commands
            cmd.run(opGenThumbnail);  
            
        } catch(Exception e) {
                e.printStackTrace();
        }
       
        //Capture the image as a binary stream
        try (InputStream is = new BufferedInputStream(new FileInputStream(thumbImageName)) ) {
            
            bytes = IOUtils.toByteArray(is);
            
            fileSize = bytes.length; 
                    
            logger.log(Level.FINER, "Found DAMS file: " + thumbImageName + " Size: " + fileSize ); 
            
        } catch(Exception e) {
            e.printStackTrace();
            
	} finally {
            try {
                File thumbFile = new File (thumbImageName);
                thumbFile.delete();
                
            } catch(Exception e) {
                e.printStackTrace();
            } 
        }
        
        if (fileSize > 1 && bytes != null) {
            logger.log(Level.FINER, "Updating Thumbnail for RenditionID: " + renditionId);
            boolean thumbUpdated = update(renditionId);
            
            if ( thumbUpdated ) {
                //update the thumbnail sync date in the CDIS table
                CDISTable cdisTbl = new CDISTable();
                cdisTbl.setRenditionId(renditionId);
                cdisTbl.updateThumbnailSyncDate();
            }  
        }
        else {
            logger.log(Level.FINER, "Error: Unable to detect thumbnail image, not updating: " + renditionId);
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
    
    /*  Method :        populateRenditionsToUpdate
        Arguments:      
        Description:    Populate a list of thumnails that need to be generated/updated 
        RFeldman 3/2015
    */
    private void populateRenditionsToUpdate () {
        String sqlTypeArr[] = null;
        String sql = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("retrieveRenditionIds")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
                
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql); 
             ResultSet rs = pStmt.executeQuery() ) {
           
            // For each record in the sql query, add it to the unlinked rendition List
            while (rs.next()) {   
                addthumbnailsToSync(rs.getInt("RenditionID"), rs.getString("uoiid"));
                logger.log(Level.FINER,"Adding CIS renditionID for Thumbnail update: " + rs.getInt("RenditionID") );
            }
            int numRecords = this.thumbnailsToSync.size();
        
            logger.log(Level.FINER,"Number of records in DAMS that are unsynced: {0}", numRecords);
            
        } catch (Exception e) {
            e.printStackTrace();
        }    
        return;  
    }
    
    /*  Method :        sync
        Arguments:      
        Description:    Thumbnail sync driver 
        RFeldman 3/2015
    */
    public void sync () {
        
        this.thumbnailsToSync = new LinkedHashMap <Integer, String>();
        
        //Get a list of RenditionIDs that require syncing from the sql XML file
        populateRenditionsToUpdate ();
        
        //create the thumbnail in TMS from those DAMS images 
        for (Integer key : thumbnailsToSync.keySet()) {
             boolean blobUpdated = generate (thumbnailsToSync.get(key), key);
        }
        
    }
    
}
