/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import com.lowagie.text.*;
import com.lowagie.text.rtf.RtfWriter2;
import com.lowagie.text.rtf.style.RtfFont;
import java.io.FileOutputStream;
import java.util.Iterator;

import edu.si.CDIS.DAMS.Database.CDISMap;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;

/**
 *
 * @author rfeldman
 */
public class Report {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Connection damsConn;
    private Double rptDays;
    private String rptHours;
    ArrayList<Integer> completedIds;
    ArrayList<Integer> metaSyncedIds;
    ArrayList<Integer> inProgressIds;
    ArrayList<Integer> failedIds;
    File linkedFile;
    File metaDataSyncFile;
    File errorFile;
    Document document;
    String startTime;
     
    
     private boolean genMetaSyncedUoiidList () {
        
        String sql = "SELECT cdis_map_id FROM cdis_map a " + 
                     "WHERE metadata_sync_dt > (SYSDATE - " + this.rptDays + ")" +
                     "AND integration_complete_dt < (SYSDATE - " + this.rptDays + ")" +
                     "AND NOT EXISTS (" +
                        "SELECT 'X' FROM cdis_error b " +
                        "WHERE a.cdis_map_id = b.cdis_map_id)";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
                
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                    
                while (rs.next()) {
                   metaSyncedIds.add(rs.getInt(1));
                }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain MetaData synced Rows, returning", e);
                return false;
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        return true;
        
    }
     
    
    private boolean genCompletedUoiidList () {
        
        String sql = "SELECT cdis_map_id FROM cdis_map a " + 
                     "WHERE integration_complete_dt > (SYSDATE - " + this.rptDays + ")" +
                     "AND NOT EXISTS (" +
                        "SELECT 'X' FROM cdis_error b " +
                        "WHERE a.cdis_map_id = b.cdis_map_id)";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
                
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                    
                while (rs.next()) {
                   completedIds.add(rs.getInt(1));
                }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain Linked Rows, returning", e);
                return false;
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        return true;
        
    }
    
    private boolean genUoiidListError () {
        
        String sql = "SELECT cdis_map_id FROM cdis_map a " + 
                     "WHERE EXISTS ( " + 
                            "SELECT 'X' from cdis_error b " +
                            "WHERE a.cdis_map_id = b.cdis_map_id " +
                            "AND b.error_dt > (SYSDATE - " + this.rptDays + "))";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                    
                while (rs.next()) {
                    failedIds.add(rs.getInt(1));            
                }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain Errored Rows, returning", e);
                return false;
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        return true;
    }
    
    private boolean create (String siUnit) {
        
        String timeStamp;
        
        //get the date timestamp for use in the report file
        DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmmss");
        timeStamp = df.format(new Date());
       
        String rptFile =  "rpt\\CDISRPT-" + siUnit + "-" + timeStamp + ".rtf";
        
        this.document = new Document();
         
        try {
            
            RtfWriter2.getInstance(document,new FileOutputStream(rptFile));
       
            document.open();
            
            RtfFont title=new RtfFont("Times New Roman",14,Font.BOLD);
            document.add(new Paragraph(siUnit + " CDIS Activity Report - Past " + this.rptHours + " Hours", title));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
            this.rptDays = 1.0;
        }  
        
        
         
        return true;
    }
    
    private void statisticsWrite () {
        
        try {
            RtfFont stats=new RtfFont("Times New Roman",12);
            
            document.add(new Paragraph("\nNumber of Succesful DAMS/CIS Linkages - Completed : " + this.completedIds.size(), stats));
            document.add(new Paragraph("Number of Succesful DAMS/CIS Linkages - In Progress : " + this.inProgressIds.size(), stats));
            document.add(new Paragraph("Number of Successful MetaData Re-Synced Records: " + this.metaSyncedIds.size(), stats));
            document.add(new Paragraph("Number of Failed Records: " + this.failedIds.size(), stats));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
            this.rptDays = 1.0;
        }  
    }
    
    private void completedWrite() {
        
        try {
            document.add(new Paragraph("\nIntegration Successfully Completed List"));
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
            this.rptDays = 1.0;
        }  
            
        for (Iterator<Integer> iter = completedIds.iterator(); iter.hasNext();) {
            
            try {
                RtfFont stats=new RtfFont("Arial",10);
            

                CDISMap cdisMap = new CDISMap();
                cdisMap.setCdisMapId(iter.next());
                       
                boolean returnVal = cdisMap.populateFileName(damsConn);
                
                if ( returnVal ) { 
                    //getCisID
                
                    SiAssetMetaData siAsst = new SiAssetMetaData();
                    //siAsst.setUoiid(cdisMap.getUoiid();
                    //siAsst.populateSourceSystemID(damsConn)(
                
                    //String listing = "FileName: " + cdisMap.getFileName() + "Linked to CisID: " + cdisMap.getCisId() + " Source System ID: " + sourceSystemId; 
                    String listing = "FileName: " + cdisMap.getFileName();
                                
                    document.add(new Paragraph("\n" + listing,stats));
            
                }
                else {
                    logger.log(Level.FINEST, "ERROR in obtaining map data for Report");
                }
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR",e);
                this.rptDays = 1.0;
            }
        }
        
        
    }
    
        private void syncedWrite() {
        String fileName;
        String uoiid;
        Integer cisID;
        
        String ObjectNumber;  //Source System ID
        
        try {
            RtfFont stats=new RtfFont("Arial",10);
            
            //document.add(new Paragraph("\nFileName: " + fileName + "Linked As : stats))";
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
            this.rptDays = 1.0;
        }  
    }

    
    public void generate (CDIS cdis) {
        
        this.damsConn = cdis.damsConn;
        
        try {
            this.rptHours = cdis.properties.getProperty("rptHours");
            this.rptDays = Double.parseDouble(rptHours) / 24;
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "Unable to calculate timeframe of report, defaulting to last 24 hours");
            this.rptDays = 1.0;
            this.rptHours = "24";
        }        
                
        create(cdis.properties.getProperty("siUnit"));
         
        //Get list of completed records (UOI_IDs) from the past increment
        // In progress should be collected first in case there are some currently in progress while the report is running
        // ...we dont want to miss the ones that may be in progress now but completed by the time we get to the next step.
        this.inProgressIds = new ArrayList<>();
        
        //Get list of completed records (UOI_IDs) from the past increment
        this.completedIds = new ArrayList<>();
        genCompletedUoiidList ();

        //Get the metadata synced records from the past increment
        this.metaSyncedIds = new ArrayList<>();
        genMetaSyncedUoiidList ();
        
        //Get the failed records from the past increment
        this.failedIds = new ArrayList<>();
        genUoiidListError ();
        
        //Loop through the processed list and generate report
        
        statisticsWrite();
        
        if (completedIds.size() > 0 ) {
            completedWrite();
        }
       
        
        //Loop through the metadata sync list and generate report
        
        //Loop through error report and generate report
        
        //send email to list
        
        document.close();
        
    }
}
