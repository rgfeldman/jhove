/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report;


import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.operations.Operation;
import edu.si.damsTools.utilities.XmlUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class ReportGenerator extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList<String> multiReportKeyValues;
    
    protected DisplayFormat displayFormat;  
    Report[] reportObjectList;

    public ReportGenerator() {   
       
        switch (DamsTools.getSubOperation()) {
            case "cdisVfcuDir":
                // Get the md5 file ids for each genDisplayFormat
                displayFormat = new CdisVfcuDirReport();
                break;
            case "timeframe":
                displayFormat = new TimeFrameReport();
                break;
            case "vfcuMd5Error":
                displayFormat = new VfcuMd5Error();
                break;
            case "vfcuMediaFile":
                displayFormat = new VfcuMediaReport();
                break;
            default:
                logger.log(Level.SEVERE, "Critical Error: Invalid Report type");
        }
        multiReportKeyValues = new ArrayList();
                
    }

    public void invoke () {
        
        if (displayFormat.returnMultiReportInd() ) {
            // We have multiple reports to generate, get the report key for each
            boolean multiValueReport = populateMultiReportKeyValues();
            if (! multiValueReport) {
                logger.log(Level.FINEST, "Error generating Key Values for report ");
                return;
            }
        }
        else {
            // We only have a single report to generate
            multiReportKeyValues.add(null);
        }
        
        //There are possibly more than one report to generate per execution, loop through the keyvalues of the report
        for (String keyValue : multiReportKeyValues) {
            Report rpt = new Report();
            rpt.setKeyValue(keyValue);
            logger.log(Level.FINEST, "MultRpt Key Value: " + keyValue);

            //Generate and Send the attachment
            boolean reportSuccess = rpt.generate();
            
            if (!reportSuccess) {
                logger.log(Level.SEVERE, "Error generating report for MultRpt Key Value: " + keyValue);
                continue;
            }
            //update the database
            boolean dbUpdated = displayFormat.updateDbComplete(keyValue);
        }
                
    }
    
    public boolean populateMultiReportKeyValues() {
        
        String sql = XmlUtils.returnFirstSqlForTag("getMultiReportKeyValue");          
        if (sql == null) {
            logger.log(Level.FINEST, "Error: Key Value Sql not found");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
            
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {     
                multiReportKeyValues.add(rs.getString(1));
            }
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to Obtain list for failed report, returning", e);
            return false; 
        }            
        return true;
    }
    
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        reqProps.add("deliveryMethod");
        if (XmlUtils.getConfigValue("deliveryMethod").equals("email") ) {
            reqProps.add("emailToList");
        }
        reqProps.add("sqlFile");

        switch (DamsTools.getSubOperation()) {
             case "cdisVfcuDir":
               reqProps.add("suppressAttch");
               reqProps.add("useMasterSubPairs");
               break;
            case "timeframe":
                reqProps.add("rptHours"); 
                reqProps.add("timeFrameEmailList");
                reqProps.add("suppressAttch");
                break;
            case "vfcuMd5Error":
                reqProps.add("rptHours"); 
            case "vfcuMediaFile":
                reqProps.add("useMasterSubPairs");
                break;             
               
            default:
                logger.log(Level.SEVERE, "Fatal Error, Invalid report type: " + DamsTools.getSubOperation());
        }
            
        //add more required props here
        return reqProps;    
    }
    
    public boolean requireSqlCriteria () {
        return true;
    }
    
}
