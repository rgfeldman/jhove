/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.utilties.DataProvider;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.DAMS.Database.Uois;
import edu.si.CDIS.CIS.Database.MediaFiles;

import edu.si.CDIS.DAMS.Database.SiAssetMetaData;

import java.util.logging.Level;
import java.util.logging.Logger;

// This is the main entrypoint for syncing the image file and image file path in TMS
public class ImageFilePath {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
	
    int successfulUpdateCount;
  
    int idsPathId;
    int pdfPathId;
    String fileType;
    String uan;
    ArrayList<String> RenditionIdToSyncList;
        
    public void sync(CDIS cdis) {
    	
    	logger.log(Level.FINER, "ImageFilePath.sync()");
        //assign the database connections for later use
        this.idsPathId = Integer.parseInt(CDIS.getProperty("IDSPathId"));
        this.pdfPathId = Integer.parseInt(CDIS.getProperty("PDFPathId"));
        
        ArrayList<Integer> RenditionIdToSyncList = new ArrayList<Integer>();

        //Get list of renditions to sync
        boolean receivedList = getNeverSyncedImagePath();
        
        if (receivedList) {
            //loop through the list, and update the pathname
            processRenditionList ();
        }
    }
    
    // Get list of images that require sync file path to be updated
    private boolean getNeverSyncedImagePath () {
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        String sql = "SELECT RenditionID " +
                     "FROM MediaFiles a, " +
                     "MediaRenditions b " +
                     "where a.RenditionID = b.RenditionID " +
                     "and b.isColor = 1 " +
                     "and a.PathID != " + this.idsPathId + " " + 
                     "and a.PathID != " + this.pdfPathId;       
        
        logger.log(Level.FINEST, "IDS syncPath Select: " + sql);
        
        try {
            pStmt = CDIS.getCisConn().prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            while (rs.next()) {
                //We add all unlinked Renditions that are marked as 'For DAMS' to a list
                RenditionIdToSyncList.add(rs.getString(1));
            }

        } catch (Exception e) {
            logger.log(Level.FINEST, "Error obtaining list to sync mediaPath and Name: ");
            return false;
        } finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); } 
        }
        
        return true;
    }
    
   
    
    // That the list of renditions by CDIS_ID and loop through them one at a time, and perform synchronizations
    private void processRenditionList() {
        // For each Rendition Number in list, obtain information from CDIS table
        PreparedStatement pStmt = null;

        try {
            // prepare the sql for obtaining info from CDIS
            pStmt = CDIS.getCisConn().prepareStatement("SELECT dams_uoi_id "
                    + "FROM cdis_map "
                    + "WHERE RenditionID = ? "
                    + "ORDER BY CDIS_ID");
        } catch (Exception e) {
            e.printStackTrace();
        }

        ResultSet rs = null;

        // for each Rendition ID that was identified for sync
        for (Iterator<String> iter = RenditionIdToSyncList.iterator(); iter.hasNext();) {

            try {

                // Reassign object with each loop
                CDISMap cdisMap = new CDISMap();
                   
                // store the current RenditionID in the object, and send to sql statement
                cdisMap.setCisUniqueMediaId(iter.next());       
                pStmt.setString(1, cdisMap.getCisUniqueMediaId());
                   
                rs = DataProvider.executeSelect(CDIS.getCisConn(), pStmt);

                logger.log(Level.FINEST, "Getting information for CDIS_MAP_ID: " + cdisMap.getCisUniqueMediaId());

                // Get the supplemental information for the current RenditionID that we are looping through
                if (rs.next()) {
                    SiAssetMetaData siAsst = new SiAssetMetaData();
                    siAsst.setUoiid(rs.getString(1));
                    siAsst.populateOwningUnitUniqueName();
                
                    // FileName from DAMS
                    Uois uois = new Uois();
                    uois.setUoiid(siAsst.getUoiid());
                    uois.populateName();
                    
                    MediaFiles mediaFiles = new MediaFiles ();
                    mediaFiles.setRenditionId (cdisMap.getCisUniqueMediaId());
                    
                    // check the filename extension
                    if (uois.getName().endsWith(".pdf")) {
                        //Integer.parseInt (cdis.properties.getProperty("PDFPathId"))
                        mediaFiles.setFileName(siAsst.getOwningUnitUniqueName() + ".pdf");
                        mediaFiles.setPathId (this.pdfPathId);
                    }
                    else {
                        mediaFiles.setFileName(siAsst.getOwningUnitUniqueName());
                        mediaFiles.setPathId (this.idsPathId);
                    }
                    
                    mediaFiles.updateFileNameAndPath();
                            
                }
                    
            } catch (Exception e) {
                    e.printStackTrace();
            } finally {
                    try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
                    try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
                    
            }
        }
    }
}
