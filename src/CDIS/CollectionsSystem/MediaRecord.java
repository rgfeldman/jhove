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
import java.util.HashMap;
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
import java.util.logging.Logger;


/**
 *
 * @author rfeldman
 */
public class MediaRecord {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Connection damsConn;
    Connection tmsConn;
    
    
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
        
        String damsRenditionName = tmsRendition.populateTMSRendition(siAsst.getUoiid(), tmsRendition, damsConn);
        
        // If we are dealing with barcode logic, the name of the rendition that we are mapping to in TMS,
        // and the objectID is populated by an alternate method
        
        if (cdis_new.properties.getProperty("locateByBarcode").equals("true")) {
            //tmsRendition.setRenditionNumber( ObjectID);
            Integer objectId = tmsRendition.populateTMSRenditionBarcode(damsRenditionName, tmsConn);
            tmsRendition.setRenditionNumber( objectId.toString() + "_01" );
            
            tmsObject.setObjectID(objectId);
            
        }
        else {
            //NMAAHC wants RenditionNumber with '.'s instead of underscores
            String damsRenditionNameUnderscoreToDot = damsRenditionName.replaceAll("_", "."); 
            
            tmsRendition.setRenditionNumber(damsRenditionNameUnderscoreToDot);
            
            boolean objectPopulated = tmsObject.populateObjectFromRenditionNumber(damsRenditionName, tmsConn);
            if (! objectPopulated) {
                // we were unable to populate the object, return with a failure indicator
                logger.log(Level.FINER, "ERROR: Media Creation Failed. Unable to obtain object Data");
                return false;
            }
        }
        
        
        logger.log(Level.FINER, "about to create TMS media Record:");
        logger.log(Level.FINER, "ObjectID: " + tmsObject.getObjectID());
        logger.log(Level.FINER, "RenditionNumber: {0}", tmsRendition.getRenditionNumber());
        
        
        CallableStatement stmt = null;
            
        try {

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
	}
        finally {
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
                   
        return true;
        
    }
    
   
    
}
