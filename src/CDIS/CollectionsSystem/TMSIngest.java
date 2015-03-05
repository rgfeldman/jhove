/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS.CollectionsSystem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import CDIS.CDIS;
import CDIS.CollectionsSystem.Database.CDISTable;
import CDIS.CollectionsSystem.Database.TMSObject;
import CDIS.CollectionsSystem.Database.TMSRendition;
import CDIS.StatisticsReport;
import CDIS.DAMS.Database.SiAssetMetaData;

public class TMSIngest {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    Connection damsConn;
    Connection tmsConn;
    HashMap <String,String> neverLinkedDamsRendtion;  
    
    private void addNeverLinkedDamsRendtion (String UOIID, String uan) {
        this.neverLinkedDamsRendtion.put(UOIID, uan); 
    }
    
    
    private void populateNeverLinkedRenditions (CDIS cdis_new) {
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String uan = null;
        String sql = null;
        String sqlTypeArr[];
        
        for (String key : cdis_new.xmlSelectHash.keySet()) {
            
            sqlTypeArr = cdis_new.xmlSelectHash.get(key);
              
            if (sqlTypeArr[0].equals("DAMSSelectList")) {   
                sql = key;      
            }   
        }
             
        if (sql != null) {           
        
            logger.log(Level.FINER, "SQL: {0}", sql);
            
            try {
                    stmt = damsConn.prepareStatement(sql);                                
                    rs = stmt.executeQuery();
        
                    if (rs.next()) {           
                        logger.log(Level.FINER, "Adding uoi_id to unsynced hash: " + rs.getString("UOI_ID") + " " + rs.getString("OWNING_UNIT_UNIQUE_NAME") );
                        addNeverLinkedDamsRendtion(rs.getString("UOI_ID"), rs.getString("OWNING_UNIT_UNIQUE_NAME"));
                    }   

            } catch (Exception e) {
                    e.printStackTrace();
            } finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            }
            
        }
        
        return;
            
    }
    
    private void linkUANtoFilename (CDIS cdis_new, StatisticsReport statRpt) {
    
        // See if we can find if this uan already exists in TMS
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String uan = null;
        String sql = null;
        String sqlTypeArr[];
        
        for (String key : cdis_new.xmlSelectHash.keySet()) {
            
            sqlTypeArr = cdis_new.xmlSelectHash.get(key);
            if (sqlTypeArr[0].equals("TMSSelectList")) {   
                sql = key;    
              
            }
            
        }
        
        if ( sql != null) {           
        
            //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair 
            for (String UOI_ID : neverLinkedDamsRendtion.keySet()) {
                
                SiAssetMetaData siAsst = new SiAssetMetaData();
                siAsst.setOwningUnitUniqueName(neverLinkedDamsRendtion.get(UOI_ID));
                siAsst.setUoiid(UOI_ID);
                    
                sql = sql.replaceAll("\\?owning_unit_unique_name\\?", siAsst.getOwningUnitUniqueName());
                
                logger.log(Level.FINEST, "SQL: {0}", sql);
                
                try {
                    stmt = tmsConn.prepareStatement(sql);                                
                    rs = stmt.executeQuery();
                       
                    if ( rs.next()) {   
                        TMSRendition tmsRendition = new TMSRendition();
                        TMSObject tmsObject = new TMSObject();
                        
                        MediaRecord mediaRecord = new MediaRecord();
                        boolean mediaCreated = mediaRecord.create(cdis_new, siAsst, tmsRendition, tmsObject);
                            
                        if ( ! mediaCreated ) {
                            logger.log(Level.FINER, "ERROR: Media Creation Failed, no thumbnail to create...returning");
                            return;
                        }
                        
                        //Create the thumbnail image
                        Thumbnail thumbnail = new Thumbnail();
                        boolean thumbCreated = thumbnail.create(damsConn, tmsConn, siAsst.getUoiid(), tmsRendition);
                            
                        if (! thumbCreated) {
                            logger.log(Level.FINER, "Thumbnail creation failed");
                            statRpt.writeUpdateStats(siAsst.getUoiid(), tmsRendition.getRenditionNumber() , "ingestToTMS", false);
                            return;
                        }
                        
                        logger.log(Level.FINER, "Media Creation and thumbnail creation complete");
                        
                        
                        // Create CDIS Object
                        CDISTable cdisTbl = new CDISTable();
                        
                        //Populate cdisTbl Object based on renditionNumber
                        cdisTbl.setRenditionNumber(tmsRendition.getRenditionNumber());
                        cdisTbl.setUOIID(siAsst.getUoiid());
                        cdisTbl.setRenditionId(9999999);
                        cdisTbl.setObjectId (tmsObject.getObjectID());
                        
                        //Insert into cdisTbl
                        boolean recordCreated = cdisTbl.createRecord (cdisTbl, tmsConn);

                        if (! recordCreated) {
                            logger.log(Level.FINER, "Insert to CDIS table failed");
                            statRpt.writeUpdateStats(siAsst.getUoiid(), tmsRendition.getRenditionNumber() , "ingestToTMS", false);
                            return;
                        }
                         
                        statRpt.writeUpdateStats(siAsst.getUoiid(), tmsRendition.getRenditionNumber() , "ingestToTMS", true);
                        
                        // update source_system_id 
                    }
                    else {
                        logger.log(Level.FINER, "Media Already exists: Media does not need to be created");
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
                }
            }
                        
        } 
        else {
            logger.log(Level.FINER, "ERROR: unable to check if TMS Media exists, supporting SQL not provided");
        }
    }
    
    public void ingest (CDIS cdis_new, StatisticsReport statReport) { 
        
        this.damsConn = cdis_new.damsConn;
        this.tmsConn = cdis_new.tmsConn;
       
        logger.log(Level.FINER, "In redesigned Ingest to Collections area");
        
        this.neverLinkedDamsRendtion = new HashMap<String, String>();
        
        // Populate the header for the report file
        statReport.populateHeader(cdis_new.properties.getProperty("siUnit"), "IngestToCollections");
        
        // Get a list of Renditions from DAMS that have no linkages in the Collections system
        populateNeverLinkedRenditions (cdis_new);
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in TMS, if there is not, create it
        linkUANtoFilename (cdis_new, statReport);  
        
    }
}
