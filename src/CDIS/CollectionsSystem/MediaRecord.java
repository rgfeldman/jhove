/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS.CollectionsSystem;

import edu.si.data.DataProvider;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import org.apache.commons.io.IOUtils;
import java.sql.CallableStatement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import CDIS.CollectionsSystem.Database.TMSRendition;
import CDIS.CollectionsSystem.Database.TMSObject;
import CDIS.CDIS;
import CDIS.DAMS.Database.SiAssetMetaData;
import java.text.DateFormat;
import java.util.Date;
import java.util.logging.Logger;


/**
 *
 * @author rfeldman
 */
public class MediaRecord {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Connection damsConn;
    Connection tmsConn;
    
    
    private boolean formatNewRenditionName (CDIS cdis_new, String damsImageFileName, TMSRendition tmsRendition) {
        
        logger.log(Level.FINER, "Dams Image fileName before formatting: {0}", damsImageFileName);
        String tmpRenditionNumber = null;
        
        //NMAAHC wants RenditionNumber with '.'s instead of underscores
        if (cdis_new.properties.getProperty("newRenditionNameFormat").equals ("underscoreToDot")  ) {  
            String damsRenditionNameUnderscoreToDot = damsImageFileName.replaceAll("_", ".");      
            tmsRendition.setRenditionNumber(damsRenditionNameUnderscoreToDot);
        
        } else if (cdis_new.properties.getProperty("newRenditionNameFormat").equals ("none")) {
            tmsRendition.setRenditionNumber(damsImageFileName);
        }
        else if (cdis_new.properties.getProperty("newRenditionNameFormat").equals ("dropACMPrefixSuffix")) {
            if (damsImageFileName.startsWith("ACM-")) {
                tmpRenditionNumber = damsImageFileName.replaceAll("ACM-", "");
            }
            else {
                tmpRenditionNumber = damsImageFileName;
            }
            if (tmpRenditionNumber.substring(7).contains("-")) {
                
                // Chop off everything in the body (after acmboj-) following the last dash....except in the cases where it has the rank number 
                if (! tmpRenditionNumber.substring(tmpRenditionNumber.lastIndexOf("-")).startsWith("r")) { 
                    // Safe to chop off the end, the last dash does not contain a -r 
                    tmpRenditionNumber = tmpRenditionNumber.substring(0, tmpRenditionNumber.lastIndexOf("-"));
                }
            }
            tmsRendition.setRenditionNumber(tmpRenditionNumber);
        }
        else {
            logger.log(Level.FINER, "unable to create Rendition number, Invalid name formatting option: {0}", cdis_new.properties.getProperty("newRenditionNameFormat"));
            return false;
        }
        
        logger.log(Level.FINER, "Formatted name: {0}", tmsRendition.getRenditionNumber());
        
        return true;
        
    }
    
    public boolean create (CDIS cdis_new, SiAssetMetaData siAsst, TMSRendition tmsRendition, TMSObject tmsObject) {
 
        this.tmsConn = cdis_new.tmsConn;    
        this.damsConn = cdis_new.damsConn;

        boolean MediaCreated;
        
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");
        String renditionDate = df1.format(cal.getTime());
        
        //Get the rendition name, and the dimensions
        // if the barcode is set, use the name to get the barcode info,
        //else use the name to get rendition name with the rendition   
        String damsImageFileName = tmsRendition.populateRenditionFromDamsInfo(siAsst.getUoiid(), tmsRendition, damsConn);
        
        // If we are dealing with barcode logic, the name of the rendition that we are mapping to in TMS,
        // and the objectID is populated by an alternate method
        Integer objectId = 0;
        if (cdis_new.properties.getProperty("locateByBarcode").equals("true")) {
            objectId = tmsRendition.getObjectIDFromBarcode(damsImageFileName, tmsConn);

            if (objectId == 0) {
                logger.log(Level.FINER, "Unable to Find obtain object Data for barcode.  Will check to see if we find the object with alternate method");
            }
            else {
                // For NASM, we have to append the timestamp to the renditionName only on barcoded objects for uniqueness
                if (cdis_new.properties.getProperty("appendTimestampToRenditionName").equals("true"))  {
                    DateFormat df = new SimpleDateFormat("kkmmss");
                    //String rankString = 
                    
                    tmsRendition.setRenditionNumber( objectId.toString() + "_" + String.format("%03d", tmsRendition.getRank()) + "_" + df.format(new Date()));
                }
                else {
                    // For barcode objects, the renditionNumber is the objectID plus the rank
                    tmsRendition.setRenditionNumber( objectId.toString() + "_" + tmsRendition.getRank() );
                }
                tmsObject.setObjectID(objectId);
            }
        }
        
        // If we were unable to find object from barcode logic, or if we never stepped into the barcode logic,
        // then we still need to find the objectID and the new renditionNumber
        if (objectId == 0) {
            
            formatNewRenditionName (cdis_new, damsImageFileName, tmsRendition);
            
            boolean objectPopulated = tmsObject.populateObjectFromRenditionNumber(damsImageFileName, cdis_new, tmsConn);
            if (! objectPopulated) {
                // we were unable to populate the object, return with a failure indicator
                logger.log(Level.FINER, "ERROR: Media Creation Failed. Unable to obtain object Data");
                return false;
            }
        }
        
        // Set the primaryRenditionFlag
        tmsRendition.populateIsPrimary(tmsObject.getObjectID(), tmsConn);
        
        logger.log(Level.FINER, "about to create TMS media Record:");
        logger.log(Level.FINER, "ObjectID: " + tmsObject.getObjectID());
        logger.log(Level.FINER, "RenditionNumber: {0}", tmsRendition.getRenditionNumber());
        logger.log(Level.FINER, "Rank: " + tmsRendition.getRank());
        logger.log(Level.FINER, "PixelH: " + tmsRendition.getPixelH());
        logger.log(Level.FINER, "PixelW: " + tmsRendition.getPixelW());
        logger.log(Level.FINER, "IsPrimary: " + tmsRendition.getIsPrimary()); 
        logger.log(Level.FINER, "IDSPath: " + cdis_new.properties.getProperty("IDSPathId")); 
        
        CallableStatement stmt = null;
            
        try {

            // Call stored procedure to create media record in TMS
            stmt = tmsConn.prepareCall("{ call CreateMediaRecords(?,?,?,?,?,?,?,?,?,?)}");
                        
            stmt.setString(1, siAsst.getUoiid());
            stmt.setString(2, siAsst.getOwningUnitUniqueName());
            stmt.setString(3, cdis_new.properties.getProperty("IDSPathId"));
            stmt.setString(4, tmsRendition.getRenditionNumber());
            stmt.setInt(5, tmsObject.getObjectID());
            stmt.setInt(6, tmsRendition.getRank());
            stmt.setInt(7, tmsRendition.getPixelH());
            stmt.setInt(8, tmsRendition.getPixelW());
            
            if (tmsRendition.getIsPrimary() ) {
                stmt.setInt(9, 1);
            }
            else {
                stmt.setInt(9, 0);
            }
            
            stmt.setString(10, renditionDate);
            
            stmt.executeUpdate();
            
        }catch(SQLException sqlex) {
		sqlex.printStackTrace();
                return false;
	}
        finally {
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
                   
        return true;
        
    }
    
   
    
}
