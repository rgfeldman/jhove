/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations;


import edu.si.damsTools.DamsTools;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import edu.si.damsTools.cdis.operations.report.DisplayFormat;
import edu.si.damsTools.cdis.operations.report.Report;
import edu.si.damsTools.cdis.operations.report.TimeFrameReport;
import edu.si.damsTools.cdis.operations.report.VfcuDirReport;

/**
 *
 * @author rfeldman
 */
public class ReportGenerator extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    protected DisplayFormat displayFormat;  
    Report[] reportObjectList;

    public ReportGenerator() {   
         switch (DamsTools.getOperationType()) {
            case "vfcuDirReport":
                //Get the bundle of reports
                // Get the md5 file ids for each genDisplayFormat
                displayFormat = new VfcuDirReport();
                break;
            case "timeframeReport":
                displayFormat = new TimeFrameReport();
                break;
            default:
                logger.log(Level.SEVERE, "Error: Additonal paramater required and not supplied");
        }
    }

    public void invoke () {
        
        boolean multiValueReport = displayFormat.populateMultiReportKeyValues();
        
        //If there are no key values then the report is intended on running only once per generator instance
        if (! multiValueReport) {
            Report rpt = new Report();
            rpt.generate();
        }
        else {
            //There are possibly more than one report to generate per execution, loop through the keyvalues of the report
            for (String keyValue : displayFormat.returnKeyValueList()) {
                Report rpt = new Report();
                rpt.setKeyValue(keyValue);
                logger.log(Level.FINEST, "MultRpt Key Value: " + keyValue);

                //check if all of the files have been physically moved
            
                //Generate and Send the attachment
                boolean reportSuccess = rpt.generate();
            
                if (reportSuccess) {
                    //update the database
                    boolean dbUpdated = displayFormat.updateDbComplete(keyValue);
                }
            }
        }        
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        
        switch (DamsTools.getOperationType()) {
             case "vfcuDirReport":
               reqProps.add("rptDeliveryMethod");
               if (DamsTools.getProperty("rptDeliveryMethod").equals("email") ) {
                   reqProps.add("vfcuDirEmailList");
               }
               reqProps.add("vfcuDirRptSupressAttch");
               reqProps.add("vfcuDirReportXmlFile");
               break;
            case "timeframeReport":
               reqProps.add("rptHours"); 
               reqProps.add("timeFrameEmailList");
               reqProps.add("tfRptSupressAttch");
               reqProps.add("timeframeReportXmlFile");
               break;
            default:
                logger.log(Level.SEVERE, "Error: Additonal paramater required and not supplied");
        }
            
        //add more required props here
        return reqProps;    
    }
    
}
