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

/**
 *
 * @author rfeldman
 */
public class Report {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Connection damsConn;
    private Integer rptDays;
    private String rptHours;
    ArrayList<String> CompletedUoiids;
    ArrayList<String> metaSyncedUoiids;
    ArrayList<Integer> failedMapIds;
    File linkedFile;
    File metaDataSyncFile;
    File errorFile;
    Document document;
     
    
     private boolean genMetaSyncedUoiidList () {
        
        String sql = "SELECT uoi_id FROM cdis_map " + 
                     "WHERE metadata_sync_dt > (SYSDATE - " + this.rptDays + ")" +
                     "AND integration_complete_dt < (SYSDATE - " + this.rptDays + ")"; 
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
                
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                    
                while (rs.next()) {
                   metaSyncedUoiids.add(rs.getString(1));
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
        
        String sql = "SELECT uoi_id FROM cdis_map " + 
                     "WHERE integration_complete_dt > (SYSDATE - " + this.rptDays + ")"; 
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
                
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                    
                while (rs.next()) {
                   CompletedUoiids.add(rs.getString(1));
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
        
        String sql = "SELECT uoi_id FROM cdis_map a " + 
                     "WHERE EXISTS ( " + 
                            "SELECT 'X' from cdis_error b " +
                            "WHERE a.cdis_map_id = b.cdis_map_id ) " +
                            "AND b.error_dt > (SYSDATE - " + this.rptDays + ")";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
                
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                    
                if (rs.next()) {
                   // this.mediaPathLocation = rs.getString(1);           
                }        
                else {
                    throw new Exception();
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
            
            RtfFont title=new RtfFont("Arial",14,Font.BOLD);
            document.add(new Paragraph(siUnit + " CDIS Report and Statistics for Past " + this.rptHours + " Hours", title));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
            this.rptDays = 1;
        }  
        
        
         
        return true;
    }
    
    private void statisticsWrite () {
        
        try {
            RtfFont stats=new RtfFont("Arial",12);
            
            document.add(new Paragraph("\nNumber of Successful CDIS Linkages between DAMS and the CIS: " + this.CompletedUoiids.size(), stats));
            document.add(new Paragraph("Number of Successful MetaData Re-Synced Records: " + this.metaSyncedUoiids.size(), stats));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
            this.rptDays = 1;
        }  
    }
    
    private void CompletedWrite() {
        String fileName = null;
        String sourceSystemId = null;
        Integer cisID = null;
        
        String listing = "FileName: " + fileName + "Linked to CisID: " + cisID + " Source System ID: " + sourceSystemId; 
        
        try {
            RtfFont stats=new RtfFont("Arial",10);
            
            document.add(new Paragraph("\n " + listing,stats));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
            this.rptDays = 1;
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
            this.rptDays = 1;
        }  
    }
    
    
    public void generate (CDIS cdis) {
        
        this.damsConn = cdis.damsConn;
        
        try {
            this.rptHours = cdis.properties.getProperty("rptHours");
            this.rptDays = Integer.parseInt(rptHours) / 24;
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "Unable to calculate timeframe of report, defaulting to last 24 hours");
            this.rptDays = 1;
            this.rptHours = "24";
        }        
        
        create(cdis.properties.getProperty("siUnit"));
         
        //Get list of completed records (UOI_IDs) from the past increment
        this.CompletedUoiids = new ArrayList<>();
        genCompletedUoiidList ();

        //Get the metadata synced records from the past increment
        this.metaSyncedUoiids = new ArrayList<>();
        genMetaSyncedUoiidList ();
        
        //Get the failed records from the past increment
        this.failedMapIds = new ArrayList<>();
        genUoiidListError ();
        
        //Loop through the processed list and generate report
        
        statisticsWrite();
        
        CompletedWrite();
        
        //Loop through the metadata sync list and generate report
        
        //Loop through error report and generate report
        
        //send email to list
        
        document.close();
        
    }
}
