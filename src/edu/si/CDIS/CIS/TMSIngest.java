/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.CIS.Database.CDISTable;
import edu.si.CDIS.CIS.Database.TMSObject;
import edu.si.CDIS.CIS.Database.TMSRendition;
import edu.si.CDIS.StatisticsReport;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;

public class TMSIngest {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Connection damsConn;
    Connection tmsConn;
    LinkedHashMap <String,String> neverLinkedDamsRendtion;  
    int successCount;
    int failCount;
    
    private void addNeverLinkedDamsRendtion (String UOIID, String uan) {
        this.neverLinkedDamsRendtion.put(UOIID, uan); 
    }
    
    /*  Method :        populateNeverLinkedImages
        Arguments:      
        Description:    populates the list of never linked dams Images  
        RFeldman 2/2015
    */
    private void populateNeverLinkedImages (CDIS cdis_new) {
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
        
                    while (rs.next()) {           
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
    
    /*  Method :        processUAN
        Arguments:      
        Description:    prcoesses the DAMS uans one at a time from the list  
        RFeldman 2/2015
    */
    private void processUAN (CDIS cdis_new, StatisticsReport statRpt) {
    
        // See if we can find if this uan already exists in TMS
        ResultSet rs = null;
        PreparedStatement stmt = null;
        String uan = null;
        String sql = null;
        String sqlTypeArr[];
        
        for (String key : cdis_new.xmlSelectHash.keySet()) {
            
            sqlTypeArr = cdis_new.xmlSelectHash.get(key);
            if (sqlTypeArr[0].equals("checkForExistingTMSRendition")) {   
                sql = key;    
              
            }
            
        }
        
        if ( sql != null) {           
        
            //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair 
            for (String UOI_ID : neverLinkedDamsRendtion.keySet()) {
                
                SiAssetMetaData siAsst = new SiAssetMetaData();
                siAsst.setOwningUnitUniqueName(neverLinkedDamsRendtion.get(UOI_ID));
                siAsst.setUoiid(UOI_ID);
                    
                logger.log(Level.FINEST, "UOI_ID {0}", UOI_ID);
                
                String currentIterationSql = sql.replaceAll("\\?owning_unit_unique_name\\?", siAsst.getOwningUnitUniqueName());
                
                logger.log(Level.FINEST, "SQL: {0}", currentIterationSql);
                
                try {
                    stmt = tmsConn.prepareStatement(currentIterationSql);                                
                    rs = stmt.executeQuery();
                       
                    if ( rs.next()) {   
                        TMSRendition tmsRendition = new TMSRendition();
                        TMSObject tmsObject = new TMSObject();
                        
                        MediaRecord mediaRecord = new MediaRecord();
                        boolean mediaCreated = mediaRecord.create(cdis_new, siAsst, tmsRendition, tmsObject);
                            
                        if ( ! mediaCreated ) {
                            logger.log(Level.FINER, "ERROR: Media Creation Failed, no thumbnail to create...returning");
                            statRpt.writeUpdateStats(siAsst.getUoiid(), tmsRendition.getRenditionNumber() , "ingestToTMS", false);
                            failCount ++;
                            continue; //Go to the next record in the for-sloop
                        }
                        
                        // Set the renditionID for the rendition just created
                        tmsRendition.populateRenditionIdByRenditionNumber(tmsRendition, tmsConn);
                        logger.log(Level.FINER, "RenditionID for newly created media: " );
                        
                        //Create the thumbnail image
                        Thumbnail thumbnail = new Thumbnail();
                        boolean thumbCreated = thumbnail.update(damsConn, tmsConn, siAsst.getUoiid(), tmsRendition.getRenditionId());
                            
                        if (! thumbCreated) {
                            logger.log(Level.FINER, "Thumbnail creation failed");
                            statRpt.writeUpdateStats(siAsst.getUoiid(), tmsRendition.getRenditionNumber() , "ingestToTMS", false);
                            failCount ++;
                            continue; //Go to the next record in the for-sloop
                        }
                        
                        logger.log(Level.FINER, "Media Creation and thumbnail creation complete");
                        
                        
                        // Create CDIS Object
                        CDISTable cdisTbl = new CDISTable();
                        
                        //Populate cdisTbl Object based on renditionNumber
                        cdisTbl.setRenditionNumber(tmsRendition.getRenditionNumber());
                        cdisTbl.setUOIID(siAsst.getUoiid());
                        cdisTbl.setRenditionId(tmsRendition.getRenditionId());
                        cdisTbl.setObjectId (tmsObject.getObjectID());
                        
                        //Insert into cdisTbl
                        boolean recordCreated = cdisTbl.createRecord (cdisTbl, tmsConn);

                        if (! recordCreated) {
                            logger.log(Level.FINER, "Insert to CDIS table failed");
                            statRpt.writeUpdateStats(siAsst.getUoiid(), tmsRendition.getRenditionNumber() , "ingestToTMS", false);
                            failCount ++;
                            continue;
                        }
                        
                        int rowsUpdated = cdisTbl.updateIDSSyncDate(cdisTbl, tmsConn);
                        
                        if (rowsUpdated == 0) {    
                            logger.log(Level.FINER, "IDS Sync date update failed");
                            statRpt.writeUpdateStats(siAsst.getUoiid(), tmsRendition.getRenditionNumber() , "ingestToTMS", false);
                            failCount ++;
                            continue;
                        }
                        else if (rowsUpdated > 1) {
                            logger.log(Level.FINER, "Warning: Multiple rows may have been IDS path Synced");
                        }
                         
                        statRpt.writeUpdateStats(siAsst.getUoiid(), tmsRendition.getRenditionNumber() , "ingestToTMS", true);
                        this.successCount++;
                        
                    }
                    else {
                        logger.log(Level.FINER, "Media Already exists: Media does not need to be created");
                    }

                } catch (SQLException se) {
                    logger.log(Level.SEVERE, "Fatal Error, check query XML file, CheckForExistingTMSRendition tag for syntax");
                    se.printStackTrace();
                    return;
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
    
    /*  Method :        ingest
        Arguments:      
        Description:    The main driver for the ingest to CIS process 
        RFeldman 2/2015
    */
    public void ingest (CDIS cdis_new, StatisticsReport statReport) { 
        
        this.damsConn = cdis_new.damsConn;
        this.tmsConn = cdis_new.tmsConn;
       
        logger.log(Level.FINER, "In redesigned Ingest to Collections area");
        
        this.neverLinkedDamsRendtion = new LinkedHashMap<String, String>();
        
        // Populate the header for the report file
        statReport.populateHeader(cdis_new.properties.getProperty("siUnit"), "ingestToCIS");
        
        // Get a list of Renditions from DAMS that have no linkages in the Collections system
        populateNeverLinkedImages (cdis_new);
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in TMS, if there is not, create it
        processUAN (cdis_new, statReport);  
        
        statReport.populateStats (0, 0, this.successCount, this.failCount, "ingestToCIS");
        
    }
}
