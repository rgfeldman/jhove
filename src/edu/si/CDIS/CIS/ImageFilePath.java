/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS;

import edu.si.CDIS.utilties.DataProvider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.CIS.Database.CDISTable;
import edu.si.CDIS.StatisticsReport;
import java.util.logging.Level;

// This is the main entrypoint for syncing the image file and image file path in TMS
public class ImageFilePath {
    
    Connection tmsConn;
    Connection damsConn;
    int IDSPathId;
    int successfulUpdateCount;
    
    public void sync(CDIS cdis_new, StatisticsReport StatReport) {
        //assign the database connections for later use
        this.tmsConn = cdis_new.tmsConn;
        this.damsConn = cdis_new.damsConn;
        this.IDSPathId = Integer.parseInt(cdis_new.properties.getProperty("IDSPathId"));
        
        ArrayList<Integer> neverSyncedCDISIdLst = new ArrayList<Integer>();

        //Get list of renditions to sync
        neverSyncedCDISIdLst = getNeverSyncedImagePath();
        
        CDISTable cdisTbl = new CDISTable();
        
        //loop through the list, and update the pathname
        processRenditionList (neverSyncedCDISIdLst, StatReport);
        
        // handle the statistics report, report on the statistics
        StatReport.populateStats (neverSyncedCDISIdLst.size(), 0, this.successfulUpdateCount, "ids");
        
    }
    
    // Get list of images that require sync file path to be updated
    private ArrayList<Integer> getNeverSyncedImagePath () {
        ArrayList<Integer> CDISIdList = new ArrayList<Integer>();
        
        String sql = "select CDIS_ID " +
                     "from CDIS a, " +
                     "MediaFiles b, " +
                     "MediaRenditions c " +
                     "where a.RenditionID = b.RenditionID " +
                     "and b.RenditionID = c.RenditionID " +
                     "and a.SyncIDSPathDate is NULL " +
                     "and a.MetaDataSyncDate is not NULL " +
                     "and c.isColor = 1 " +
                     "and b.PathID != " + this.IDSPathId;
        
        System.out.println("IDS syncPath Select: " + sql);
        
        ResultSet rs;
        
        rs = DataProvider.executeSelect(this.tmsConn, sql);

        try {
            while (rs.next()) {
                CDISIdList.add(rs.getInt(1));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        
        return CDISIdList;
    }
    
   
    // That the list of renditions by CDIS_ID and loop through them one at a time, and perform synchronizations
    private void processRenditionList(ArrayList<Integer> CDISIdList, StatisticsReport StatRpt) {

        // For each Rendition Number in list, obtain information from CDIS table
        PreparedStatement stmt = null;

        try {
            // prepare the sql for obtaining info from CDIS
            stmt = this.tmsConn.prepareStatement("select RenditionID, RenditionNumber, UOIID "
                    + "from CDIS "
                    + "where CDIS_ID = ? "
                    + "order by CDIS_ID");
        } catch (Exception e) {
            e.printStackTrace();
        }

        ResultSet rs = null;

        try {

            // for each Rendition ID that was identified for sync
            for (Iterator<Integer> iter = CDISIdList.iterator(); iter.hasNext();) {

                try {

                    // Reassign object with each loop
                    CDISTable cdisTbl = new CDISTable();

                    // store the current RenditionID in the object, and send to sql statement
                    cdisTbl.setCDIS_ID(iter.next());
                    stmt.setInt(1, cdisTbl.getCDIS_ID());

                    rs = DataProvider.executeSelect(this.tmsConn, stmt);

                    System.out.println("Getting information for CDIS_ID: " + cdisTbl.getCDIS_ID());

                    // Get the supplemental information for the current RenditionID that we are looping through
                    if (rs.next()) {
                        cdisTbl.setRenditionId(rs.getInt("RenditionID"));
                        cdisTbl.setRenditionNumber(rs.getString("RenditionNumber"));
                        cdisTbl.setUOIID(rs.getString("UOIID"));
                    }
                    
                    //get UAN from DAMS
                    String uan = getUAN(cdisTbl);
                    
                    int updateCount = 0;
                    
                    // update the FilePath and FileName with the path and UAN
                    updateCount = updateFilePath(cdisTbl, uan); 
                    
                    // If se updated the filepath successfully, then log the transaction in the CDIS table
                    if (updateCount > 0) { 
                        //update CDIS with the current IDSSyncDate
                        cdisTbl.updateIDSSyncDate(cdisTbl, tmsConn);
                        
                        //Create the IDS report
                        StatRpt.writeUpdateStats(cdisTbl.getUOIID(), cdisTbl.getRenditionNumber(), "idsPath", true);
                        successfulUpdateCount ++;
                        
                    }
                    else {
                        StatRpt.writeUpdateStats(cdisTbl.getUOIID(), cdisTbl.getRenditionNumber(), "idsPath", false);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (rs != null) {
                            rs.close();
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    // This function returns the current UAN from DAMS for the current UOIID
    private String getUAN (CDISTable cdisTbl) {
        
        String uan = null;
        
        String sql = "select OWNING_UNIT_UNIQUE_NAME " +
                    "from SI_ASSET_METADATA " +
                    "where uoi_id = '" + cdisTbl.getUOIID() + "'";
        
        System.out.println("get UAN: " + sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;

        try {
            stmt = damsConn.prepareStatement(sql);                                
            rs = stmt.executeQuery();
        
            if (rs.next()) {
                uan = rs.getString(1);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return uan;
        
    }
    
    //This function updates the filePath and filename in TMS to point to the IDS derivative
    private int updateFilePath (CDISTable cdisTbl, String uan){
        
        String sql = "update MediaFiles " +
                    "set PathID = " + this.IDSPathId + ", " +
                    "FileName = '" + uan + "' " +
                    "where RenditionID = " + cdisTbl.getRenditionId();
        
        System.out.println("IDS update: " + sql);
        
        int updateCount = DataProvider.executeUpdate(this.tmsConn, sql);
        
        return updateCount;
        
    }
    
}
