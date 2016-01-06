/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.CIS.Database.Objects;
import edu.si.CDIS.CIS.Database.MediaRenditions;
import edu.si.CDIS.CIS.MediaRecord;
import edu.si.CDIS.DAMS.Database.Uois;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
            Objects tmsObject = new Objects();
                        
            MediaRecord mediaRecord = new MediaRecord();
            
            boolean mediaCreated = mediaRecord.create(uois, mediaRendition, tmsObject);
                            
            if ( ! mediaCreated ) {
                logger.log(Level.FINER, "ERROR: Media Creation Failed, no thumbnail to create...returning");
                continue; //Go to the next record in the for-sloop
            }
                        
            // Set the renditionID for the rendition just created
            mediaRendition.populateIdByRenditionNumber();
            logger.log(Level.FINER, "Media Creation complete, RenditionID for newly created media: " + mediaRendition.getRenditionNumber() );

            LinkDamsToCIS linkDamsToCis = new LinkDamsToCIS();
            linkDamsToCis.createNewLink(Integer.toString(mediaRendition.getRenditionId()), uoiId);
            
        }
    }
    
    /*  Method :        createMedia
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
