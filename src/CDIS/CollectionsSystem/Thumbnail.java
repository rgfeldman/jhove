/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS.CollectionsSystem;

import java.io.BufferedReader;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.LinkedHashMap;
import org.apache.commons.io.IOUtils;

import CDIS.CDIS;
import CDIS.CollectionsSystem.Database.TMSRendition;
import CDIS.StatisticsReport;


public class Thumbnail {

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    String damsImageLocation;
    Connection damsConn;
    Connection tmsConn;
    String uoiid;
    String renditionNumber;
    
    LinkedHashMap <Integer,String> thumbnailsToSync;  
    
    private void addthumbnailsToSync (Integer renditionID, String UOIID) {
        this.thumbnailsToSync.put(renditionID, UOIID); 
    }
    
    /*  Method :        getDamsLocation
        Arguments:      
        Description:    Obtains the location of the thumnail image from the DAMS databasae 
        RFeldman 3/2015
    */
    private boolean getDamsLocation () {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        String sql = "select o.object_name_location from uois u, object_stacks o" +
        " where u.uoi_id = '" + this.uoiid + "'" +
        " and u.thumb_nail_obj_id = o.object_id ";
           
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try {
            stmt = damsConn.prepareStatement(sql);
            rs = stmt.executeQuery();
        
            if(rs.next()) {
                this.damsImageLocation = rs.getString(1);
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
    
    /*  Method :        update
        Arguments:      
        Description:    Finds the physical image and Updates the thumbnail image in TMS with the image 
        RFeldman 3/2015
    */
    public boolean update(Connection damsConn, Connection tmsConn, String uoiid, Integer renditionID) {
        
        this.uoiid = uoiid;
        this.damsConn = damsConn;
        
        PreparedStatement stmt = null;
        InputStream is = null;
        String imageFile = null;
        int imageFileSize = 0;
        byte[] bytes = null;
        
        boolean locationFound = getDamsLocation ();
        
        if (! locationFound) {
            logger.log(Level.FINER, "Not updating thumbnail, image could not be located from database");
            return false;
        }
        
        imageFile = "\\\\smb.si-osmisilon1.si.edu\\prodartesiarepo\\" + this.damsImageLocation; 
              
        // Capture the image as a binary stream
        try {
           
            is = new BufferedInputStream(new FileInputStream(imageFile));
            
            bytes = IOUtils.toByteArray(is);
            
            imageFileSize = bytes.length; 
                    
            logger.log(Level.FINER, "Found DAMS file: " + imageFile + " Size: " + imageFileSize ); 
            
        } catch(Exception e) {
            e.printStackTrace();
            
	}
        
        logger.log(Level.FINER, "Updating Thumbnail for RenditionID: " + renditionID);
        
        //Input the binary stream into the update statement for the table...and execute
        try {
											
            stmt = tmsConn.prepareStatement("update MediaRenditions set ThumbBLOB = ?, ThumbBlobSize = ? " +
                    " where RenditionID in (SELECT RenditionID from MediaRenditions where RenditionID =  ? ) ");
			
            stmt.setBytes(1, bytes);
            stmt.setInt(2, imageFileSize);
            stmt.setInt(3, renditionID);
        
            int recordsUpdated = stmt.executeUpdate();
            
            if ((recordsUpdated) != 1 ) {
                logger.log(Level.FINER, "ERROR: Thumbnail creation has failed for renditionID: " + renditionID);
            }
            else {
                logger.log(Level.FINER, "Thumbnail successfully updated for renditionID: " + renditionID);
            }
                    
         }catch(Exception e) {
		e.printStackTrace();
            }
	
        return true;
                                                            
    }
    
    /*  Method :        populateRenditionsToUpdate
        Arguments:      
        Description:    Populate a list of thumnails that need to be updated 
        RFeldman 3/2015
    */
    private void populateRenditionsToUpdate (CDIS cdis_new) {
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String owning_unit_unique_name = null;
        String sqlTypeArr[] = null;
        String sql = null;
        
        //Go through the hash containing the select statements from the XML, and obtain the proper select statement
        for (String key : cdis_new.xmlSelectHash.keySet()) {     
            
            sqlTypeArr = cdis_new.xmlSelectHash.get(key);
            
            if (sqlTypeArr[0].equals("retrieveRenditionIds")) {   
                sql = key;    
                logger.log(Level.FINEST, "SQL: {0}", sql);
            }
        }
                
        try {
            
            stmt = tmsConn.prepareStatement(sql);                                
            rs = stmt.executeQuery();
        
            // For each record in the sql query, add it to the unlinked rendition List
            while (rs.next()) {   
                addthumbnailsToSync(rs.getInt("RenditionID"), rs.getString("uoiid"));
                logger.log(Level.FINER,"Adding TMS renditionID for Thumbnail update: " + rs.getInt("RenditionID") );
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
    public void sync (CDIS cdis_new, StatisticsReport statReport) {
        
        this.tmsConn = cdis_new.tmsConn;
        this.damsConn = cdis_new.damsConn;
        
        //Populate the header information in the report file
        statReport.populateHeader(cdis_new.properties.getProperty("siUnit"), "thumbnailSync"); 
        
        this.thumbnailsToSync = new LinkedHashMap <Integer, String>();
        
        //Get a list of RenditionIDs that require syncing from the sql XML file
        populateRenditionsToUpdate (cdis_new);
        
        //create the thumbnail in TMS from those DAMS images (cdis_new.damsConn, cdis_new.tmsConn, "", tmsRendition)
        for (Integer key : thumbnailsToSync.keySet()) {
             boolean blobUpdated = update (damsConn, tmsConn, thumbnailsToSync.get(key), key);
        }
        
        // Get the renditionNumber for the report
        //statReport.writeUpdateStats(this.uoiid, this.renditionNumber, "thumbnailSync", true);
        
    }
    
}
