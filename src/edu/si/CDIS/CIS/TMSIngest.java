/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.CIS.Database.CDISTable;
import edu.si.CDIS.CIS.Database.TMSObject;
import edu.si.CDIS.CIS.Database.MediaRenditions;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;


public class TMSIngest {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private LinkedHashMap <String,String> neverLinkedDamsRendtion;  
    private int successCount;
    private int failCount;
    
    private void addNeverLinkedDamsRendtion (String UOIID, String uan) {
        this.neverLinkedDamsRendtion.put(UOIID, uan); 
    }
    
    /*  Method :        populateNeverLinkedImages
        Arguments:      
        Description:    populates the list of never linked dams Images  
        RFeldman 2/2015
    */
    private void populateNeverLinkedImages () {
        String uan = null;
        String sql = null;
        String sqlTypeArr[];
        
        for (String key : CDIS.getXmlSelectHash().keySet()) {
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
              
            if (sqlTypeArr[0].equals("DAMSSelectList")) {   
                sql = key;      
            }   
        }
             
        if (sql != null) {           
        
            logger.log(Level.FINER, "SQL: {0}", sql);
            
            try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
                 ResultSet rs = pStmt.executeQuery()) {
                                                    
                while (rs.next()) {           
                    logger.log(Level.FINER, "Adding uoi_id to unsynced hash: " + rs.getString("UOI_ID") + " " + rs.getString("OWNING_UNIT_UNIQUE_NAME") );
                    addNeverLinkedDamsRendtion(rs.getString("UOI_ID"), rs.getString("OWNING_UNIT_UNIQUE_NAME"));
                }   
            } catch (Exception e) {
                    e.printStackTrace();
            }         
        }               
    }
    
    /*  Method :        processUAN
        Arguments:      
        Description:    prcoesses the DAMS uans one at a time from the list  
        RFeldman 2/2015
    */
    private void processUAN () { 
        // See if we can find if this uan already exists in TMS
        String uan = null;
        String sql = null;
        String sqlTypeArr[];
        
        for (String key : CDIS.getXmlSelectHash().keySet()) {
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
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
                
                try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(currentIterationSql);
                     ResultSet rs = pStmt.executeQuery()) {
                                                   
                    if ( rs.next()) {   
                        MediaRenditions mediaRendition = new MediaRenditions();
                        TMSObject tmsObject = new TMSObject();
                        
                        MediaRecord mediaRecord = new MediaRecord();
                        boolean mediaCreated = mediaRecord.create(siAsst, mediaRendition, tmsObject);
                            
                        if ( ! mediaCreated ) {
                            logger.log(Level.FINER, "ERROR: Media Creation Failed, no thumbnail to create...returning");
                            failCount ++;
                            continue; //Go to the next record in the for-sloop
                        }
                        
                        // Set the renditionID for the rendition just created
                        mediaRendition.populateIdByRenditionNumber();
                        logger.log(Level.FINER, "Media Creation complete, RenditionID for newly created media: " + mediaRendition.getRenditionNumber() );
                                
                        // Create CDIS Object
                        CDISTable cdisTbl = new CDISTable();
                        
                        //Populate cdisTbl Object based on renditionNumber
                        cdisTbl.setRenditionNumber(mediaRendition.getRenditionNumber());
                        cdisTbl.setUOIID(siAsst.getUoiid());
                        cdisTbl.setUAN(siAsst.getOwningUnitUniqueName());
                        cdisTbl.setRenditionId(mediaRendition.getRenditionId());
                        cdisTbl.setObjectId (tmsObject.getObjectID());
                        
                        //Insert into cdisTbl
                        boolean recordCreated = cdisTbl.createRecord (cdisTbl);

                        if (! recordCreated) {
                            logger.log(Level.FINER, "Insert to CDIS table failed");
                            failCount ++;
                            continue;
                        }
                        
                        //Create the thumbnail image
                        Thumbnail thumbnail = new Thumbnail();
                        boolean thumbCreated = thumbnail.generate(siAsst.getUoiid(), mediaRendition.getRenditionId());
                            
                        if (! thumbCreated) {
                            logger.log(Level.FINER, "Thumbnail creation failed");
                            failCount ++;
                            continue; //Go to the next record in the for-sloop
                        }
                                             
                        int rowsUpdated = cdisTbl.updateIDSSyncDate();
                        
                        if (rowsUpdated == 0) {    
                            logger.log(Level.FINER, "IDS Sync date update failed");
                            failCount ++;
                            continue;
                        }
                        else if (rowsUpdated > 1) {
                            logger.log(Level.FINER, "Warning: Multiple rows may have been IDS path Synced");
                        }
                         
                        // update the SourceSystemID in DAMS with the RenditionNumber
                        rowsUpdated = siAsst.updateDAMSSourceSystemID(siAsst.getUoiid(), mediaRendition.getRenditionNumber() );
                        
                        if (rowsUpdated == 0) {    
                            logger.log(Level.FINER, "Failed in update to SourceSystemID");
                            failCount ++;
                            continue;
                        }
                        else if (rowsUpdated > 1) {
                            logger.log(Level.FINER, "Warning: Multiple rows may have been IDS path Synced");
                        }
                        
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
    public void ingest () { 
       
        logger.log(Level.FINER, "In redesigned Ingest to Collections area");
        
        this.neverLinkedDamsRendtion = new LinkedHashMap<String, String>();
        
        // Get a list of Renditions from DAMS that have no linkages in the Collections system
        populateNeverLinkedImages ();
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in TMS, if there is not, create it
        processUAN ();  
        
    }
}
