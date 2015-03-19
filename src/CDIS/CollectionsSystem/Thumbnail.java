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

import CDIS.CDIS;
import CDIS.CollectionsSystem.Database.TMSRendition;
import CDIS.StatisticsReport;
import java.util.HashMap;
import org.apache.commons.io.IOUtils;



public class Thumbnail {

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    String damsImageLocation;
    Connection damsConn;
    Connection tmsConn;
    String uoiid;
    
    HashMap <Integer,String> thumbnailsToSync;  
    
    private void addthumbnailsToSync (Integer renditionID, String UOIID) {
        this.thumbnailsToSync.put(renditionID, UOIID); 
    }
    
    private boolean getDamsLocation () {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        String sql = "select o.object_name_location from uois u, object_stacks o" +
        " where u.uoi_id = '" + uoiid + "'" +
        " and u.thumb_nail_obj_id = o.object_id ";
           
        try {
            stmt = damsConn.prepareStatement(sql);
            rs = stmt.executeQuery();
        
            while(rs.next()) {
                this.damsImageLocation = rs.getString(1);
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
    
    public boolean create(Connection damsConn, Connection tmsConn, String uoiid, TMSRendition tmsRendition) {
        
        this.uoiid = uoiid;
        this.damsConn = damsConn;
        
        PreparedStatement stmt = null;
        InputStream is = null;
        String imageFile = null;
        int imageFileSize = 0;
        byte[] bytes = null;
        
        getDamsLocation ();
        
        imageFile = "\\\\smb.si-osmisilon1.si.edu\\prodartesiarepo\\" + this.damsImageLocation; 
              
        // Capture the image as a binary stream
        try {
           
            is = new BufferedInputStream(new FileInputStream(imageFile));
            
            bytes = IOUtils.toByteArray(is);
            
            imageFileSize = bytes.length; 
                    
            logger.log(Level.FINER, "Found DAMS file: " + imageFile + "Size: " + imageFileSize );
            
            
        } catch(Exception e) {
            e.printStackTrace();
            
	}
        
        //Input the binary stream into the update statement for the table...and execute
        try {
											
            stmt = tmsConn.prepareStatement("update MediaRenditions set ThumbBLOB = ?, ThumbBlobSize = ? " +
                    " where RenditionID in (SELECT RenditionID from MediaRenditions where RenditionNumber =  ? ) ");
			
            stmt.setBytes(1, bytes);
            stmt.setInt(2, imageFileSize);
            stmt.setString(3, tmsRendition.getRenditionNumber());
        
            int recordsUpdated = stmt.executeUpdate();
            
            if ((recordsUpdated) != 1 ) {
                logger.log(Level.FINER, "ERROR: Thumbnail creation has failed for renditionID: " + tmsRendition.getRenditionId());
            }
                    
         }catch(Exception e) {
		e.printStackTrace();
            }
	
        return true;
                                                            
    }
    
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
                addthumbnailsToSync(rs.getInt("RenditiodID"), rs.getString("uoiid"));
                logger.log(Level.FINER,"Adding TMS rendition for Thumbnail update {0}", rs.getInt("RenditionID") );
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
    
    public void sync (CDIS cdis_new, StatisticsReport statReport) {
        
        this.tmsConn = cdis_new.tmsConn;
        
        //Populate the header information in the report file
        statReport.populateHeader(cdis_new.properties.getProperty("siUnit"), "thumbnailSync"); 
        
        //Get a list of RenditionIDs that require syncing from the sql XML file
        populateRenditionsToUpdate (cdis_new);
        
        //create the thumbnail in TMS from those DAMS images (cdis_new.damsConn, cdis_new.tmsConn, "", tmsRendition)
    
    }
    
}
