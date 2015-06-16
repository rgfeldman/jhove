/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.LinkedHashMap;
import org.apache.commons.io.IOUtils;

import org.im4java.core.ConvertCmd;
import org.im4java.core.IMOperation;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.StatisticsReport;


public class Thumbnail {

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    String damsImageNameLocation;
    Connection damsConn;
    Connection cisConn;
    String uoiid;
    String renditionNumber;
    int fileSize;
    byte[] bytes;
    
    LinkedHashMap <Integer,String> thumbnailsToSync;  
    
    private void addthumbnailsToSync (Integer renditionID, String UOIID) {
        this.thumbnailsToSync.put(renditionID, UOIID); 
    }
     
    /*  Method :        getDamsLocation
        Arguments:      
        Description:    Obtains the location and name of the screen-size medium resoltion image from the DAMS databasae 
        RFeldman 3/2015
    */
    private boolean getDamsNameLocation () {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
      
        String sql = "select o.object_name_location from uois u, object_stacks o" +
        " where u.uoi_id = '" + this.uoiid + "'" +
        " and u.screen_res_obj_id = o.object_id ";
           
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try {
            stmt = damsConn.prepareStatement(sql);
            rs = stmt.executeQuery();
        
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
	} finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
        
    }
    
    /*  Method :        generate
        Arguments:      
        Description:    Finds the physical image and generates the thumbnail sized image 
        RFeldman 3/2015
    */
    public boolean generate(Connection damsConn, Connection cisConn, String uoiid, Integer renditionID) {
        
        this.uoiid = uoiid;
        this.damsConn = damsConn;
        this.cisConn = cisConn;
        
        InputStream is = null;
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
        opGenThumbnail.resize(4800,4800);
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
        try {
           
            is = new BufferedInputStream(new FileInputStream(thumbImageName));
            
            bytes = IOUtils.toByteArray(is);
            
            fileSize = bytes.length; 
                    
            logger.log(Level.FINER, "Found DAMS file: " + thumbImageName + " Size: " + fileSize ); 
            
        } catch(Exception e) {
            e.printStackTrace();
            
	} finally {
            try {
                // We dont need input stream or temporary file any more, remove them.
                is.close();
                File thumbFile = new File (thumbImageName);
                thumbFile.delete();
                
            } catch(Exception e) {
                e.printStackTrace();
            } 
        }
        
        if (fileSize > 1 && bytes != null) {
            logger.log(Level.FINER, "Updating Thumbnail for RenditionID: " + renditionID);
            update(renditionID);
        }
        else {
            logger.log(Level.FINER, "Error: Unable to detect thumbnail image, not updating: " + renditionID);
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
        
        PreparedStatement stmt = null;
        
        //Input the binary stream into the update statement for the table...and execute
        try {
											
            stmt = cisConn.prepareStatement("update MediaRenditions set ThumbBLOB = ?, ThumbBlobSize = ? " +
                " where RenditionID in (SELECT RenditionID from MediaRenditions where RenditionID =  ? ) ");
			
            stmt.setBytes(1, this.bytes);
            stmt.setInt(2, this.fileSize);
            stmt.setInt(3, renditionID);
        
            int recordsUpdated = stmt.executeUpdate();
            
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
    private void populateRenditionsToUpdate (CDIS cdis) {
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String owning_unit_unique_name = null;
        String sqlTypeArr[] = null;
        String sql = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : cdis.xmlSelectHash.keySet()) {     
            
            sqlTypeArr = cdis.xmlSelectHash.get(key);
            
            if (sqlTypeArr[0].equals("retrieveRenditionIds")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
                
        try {
            
            stmt = cisConn.prepareStatement(sql);                                
            rs = stmt.executeQuery();
        
            // For each record in the sql query, add it to the unlinked rendition List
            while (rs.next()) {   
                addthumbnailsToSync(rs.getInt("RenditionID"), rs.getString("uoiid"));
                logger.log(Level.FINER,"Adding CIS renditionID for Thumbnail update: " + rs.getInt("RenditionID") );
            }
            int numRecords = this.thumbnailsToSync.size();
        
            logger.log(Level.FINER,"Number of records in DAMS that are unsynced: {0}", numRecords);
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return;
        
    }
    
    /*  Method :        sync
        Arguments:      
        Description:    Thumbnail sync driver 
        RFeldman 3/2015
    */
    public void sync (CDIS cdis, StatisticsReport statReport) {
        
        this.cisConn = cdis.cisConn;
        this.damsConn = cdis.damsConn;
        
        //Populate the header information in the report file
        statReport.populateHeader(cdis.properties.getProperty("siUnit"), "thumbnailSync"); 
        
        this.thumbnailsToSync = new LinkedHashMap <Integer, String>();
        
        //Get a list of RenditionIDs that require syncing from the sql XML file
        populateRenditionsToUpdate (cdis);
        
        //create the thumbnail in TMS from those DAMS images 
        for (Integer key : thumbnailsToSync.keySet()) {
             boolean blobUpdated = generate (damsConn, cisConn, thumbnailsToSync.get(key), key);
        }
        
        // Get the renditionNumber for the report
        //statReport.writeUpdateStats(this.uoiid, this.renditionNumber, "thumbnailSync", true);
        
    }
    
}
