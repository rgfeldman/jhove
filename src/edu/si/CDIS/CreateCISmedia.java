/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.CIS.Database.TMSObject;
import edu.si.CDIS.CIS.Database.MediaRenditions;
import edu.si.CDIS.CIS.MediaRecord;
import edu.si.CDIS.CIS.Thumbnail;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.DAMS.Database.Uois;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;


public class CreateCISmedia {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private ArrayList <String> uoiidsToLink;  
    
    
    /*  Method :        populateNeverLinkedImages
        Arguments:      
        Description:    populates the list of never linked dams Images  
        RFeldman 2/2015
    */
    private void populateNeverLinkedImages () {
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
                    logger.log(Level.FINER, "Adding uoi_id to unsynced hash: " + rs.getString("UOI_ID"));
                    uoiidsToLink.add(rs.getString("UOI_ID"));
                }   
            } catch (Exception e) {
                    logger.log(Level.FINER, "Error, unable to obtain list of UOI_IDs to integrate", e);
            }         
        }               
    }
    
    /*  Method :        processUAN
        Arguments:      
        Description:    prcoesses the DAMS uans one at a time from the list  
        RFeldman 2/2015
    */
    private void processUAN () { 

        //loop through the NotLinked RenditionList and obtain the UAN/UOIID pair 
        for (String uoiId : uoiidsToLink) {
                
            // See if we can find if this uan already exists in TMS
            // TO DO
                
            logger.log(Level.FINEST, "UOI_ID to integrate:{0}", uoiId);
                
            Uois uois = new Uois();
            uois.setUoiid(uoiId);
            
            MediaRenditions mediaRendition = new MediaRenditions();
            TMSObject tmsObject = new TMSObject();
                        
            MediaRecord mediaRecord = new MediaRecord();
            
            boolean mediaCreated = mediaRecord.create(uois, mediaRendition, tmsObject);
                            
            if ( ! mediaCreated ) {
                logger.log(Level.FINER, "ERROR: Media Creation Failed, no thumbnail to create...returning");
                continue; //Go to the next record in the for-sloop
            }
                        
            // Set the renditionID for the rendition just created
            mediaRendition.populateIdByRenditionNumber();
            logger.log(Level.FINER, "Media Creation complete, RenditionID for newly created media: " + mediaRendition.getRenditionNumber() );
                                
            /*
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
                            continue;
                        }
                        
                        //Create the thumbnail image
                        Thumbnail thumbnail = new Thumbnail();
                        boolean thumbCreated = thumbnail.generate(siAsst.getUoiid(), mediaRendition.getRenditionId());
                            
                        if (! thumbCreated) {
                            logger.log(Level.FINER, "Thumbnail creation failed");
                            continue; //Go to the next record in the for-sloop
                        }
                                             
                        int rowsUpdated = cdisTbl.updateIDSSyncDate();
                        
                        if (rowsUpdated == 0) {    
                            logger.log(Level.FINER, "IDS Sync date update failed");
                            continue;
                        }
                        else if (rowsUpdated > 1) {
                            logger.log(Level.FINER, "Warning: Multiple rows may have been IDS path Synced");
                        }
                         
                        // update the SourceSystemID in DAMS with the RenditionNumber
                        rowsUpdated = siAsst.updateDAMSSourceSystemID(mediaRendition.getRenditionNumber() );
                        
                        if (rowsUpdated == 0) {    
                            logger.log(Level.FINER, "Failed in update to SourceSystemID");
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
        }*/
            
        }
    }
    
    /*  Method :        ingest
        Arguments:      
        Description:    The main driver for the ingest to CIS process 
        RFeldman 2/2015
    */
    public void createMedia () {
        
        this.uoiidsToLink = new ArrayList<>();
        
        // Get a list of Renditions from DAMS that may need to be brought to the collection system (CIS)
        populateNeverLinkedImages ();
        
        // For all the rows in the hash containing unlinked DAMS assets, See if there is a corresponding row in TMS, if there is not, create it
        processUAN ();  
        
    }
}
