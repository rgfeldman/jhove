/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report.rptFile;

/**
 *
 * @author rfeldman
 */

import com.lowagie.text.Document;
import com.lowagie.text.Font;
import com.lowagie.text.Paragraph;
import com.lowagie.text.Phrase;
import com.lowagie.text.rtf.RtfWriter2;
import com.lowagie.text.rtf.style.RtfFont;
import edu.si.damsTools.DamsTools; 
import edu.si.damsTools.report.DisplayFormat;
import edu.si.damsTools.report.Report;
import edu.si.damsTools.vfcu.database.VfcuMd5FileHierarchy;
import edu.si.damsTools.utilities.XmlUtils;
import java.io.FileOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Random;

public class RptFile extends Report  {
        
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String fileNameLoc;
    private Document document;
    private final ArrayList<String> statsList;
    
    private final RtfFont headerFont;
    private final RtfFont SectionHeaderFont;
    private final RtfFont listElementFont;
    private ArrayList<Integer> recordIdList;
    
    private String keyValue;
    

    public String getKeyValue () {
        return this.keyValue;
    }
    
    public void setKeyValue (String keyValue) {
        this.keyValue = keyValue;
    }
    
    public RptFile () {
        
        super();
        
        this.headerFont = new RtfFont("Times New Roman",14,Font.BOLD);
        this.SectionHeaderFont = new RtfFont("Times New Roman",12,Font.BOLD);
        this.listElementFont = new RtfFont("Courier",8);
        
        statsList = new ArrayList<>();
        recordIdList = new ArrayList<>();

    }
    
    public ArrayList<String> getStatsList() {
        return this.statsList;
    }
    
    public String getFileNameLoc () {
        return this.fileNameLoc;
    }
    
    public void closeDocument() {
        this.document.close();
    }
    
    public boolean createFileAndHeader (DisplayFormat displayFormat) {
       
        String timeStampWords;       
        //get the date timestamp for use in the report file
        DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmmssSSS");
        
        Random rand = new Random();
        String fileDtNum = df.format(new Date()) + rand.nextInt(100);
        
        DateFormat dfWords = new SimpleDateFormat();
        timeStampWords = dfWords.format(new Date());
        
        this.fileNameLoc =  DamsTools.getDirectoryName()+ "/rpt/CDISRPT-" + XmlUtils.getConfigValue("projectCd").toUpperCase() + "-" + fileDtNum + ".rtf";
        this.document = new Document();
         
        try {
            
            RtfWriter2.getInstance(document,new FileOutputStream(fileNameLoc));
       
            this.document.open();

            logger.log(Level.FINEST, "Will get header for keyvalue: " + keyValue);    
            String rptDocHeaderString = displayFormat.returnDocHeader(keyValue);
            if (rptDocHeaderString != null) {
                this.document.add(new Paragraph(timeStampWords + "\n" + rptDocHeaderString + "\n", headerFont)); 
            }
            else {
                logger.log(Level.FINEST, "No Header, No Report to Create ");    
                return false;
            }
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR, cannot create report ", e);
            return false;
        }  
        
        return true;
    }
    
    public boolean obtainSectionData (DisplayFormat displayFormat) {
        List<DataSection> sections = new ArrayList<>();
        sections = displayFormat.sectionFactory(); 
        
        String statHeader = displayFormat.returnStatsListsHeader(keyValue);
        
        if (statHeader != null) {
            statsList.add(statHeader);
        }
        
        boolean sectionsGenerated = genData(sections);
        if (! sectionsGenerated) {
            return false;
        }
        
        printStats();
        printSectionData(sections);
        
        return true;
    }
    
    public boolean genData (List<DataSection> sections) {
                
        for (Iterator<DataSection> it = sections.iterator(); it.hasNext();) {
            DataSection section = it.next();
            try {
                 
                recordIdList = new ArrayList();
                
                boolean recordsObtained = populateRecordIdList(section, keyValue);
                if (!recordsObtained) {
                    logger.log(Level.FINEST, "Error obtained while obtain mapId list, or list type does not apply");
                    return false;
                }                
                
                if (this.recordIdList.isEmpty() ) {
                    //this section returned an error
                    logger.log(Level.FINEST, "No records obtained meeting condition");
                    statsList.add ("Number of Records " + section.returnTitlePhrase() + ": " + 0);
                    //it.remove();
                    continue;
                } 

                for (Integer dataId : this.recordIdList) {
                    section.generateTextForRecord(dataId);
                }
       
                statsList.add ("Number of Records " + section.returnTitlePhrase() + ": " + section.getSectionTextData().size());
            
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR, cannot create report ", e);
                return false;
            }
        }
        
        return true;
    }
    
    private boolean printStats () {
        for (String statLine : this.statsList) {
            try {
                document.add(new Phrase(statLine + "\n"));
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR, cannot create report ", e);
                return false;
            }
        }
        return true;
    }
    
    public boolean printSectionData (List<DataSection> sections) {
        //now that statistics are part of the report, loop through now again to add the section data one at a time
        for (DataSection section : sections) {
            try {
                
                ArrayList<String> dataForReport = new ArrayList();
                
                document.add(new Phrase("\n\n" + "The Following Records " + section.returnTitlePhrase() + ": \n",SectionHeaderFont));
                                logger.log(Level.FINEST, "DEBUG: Added  " + section.returnTitlePhrase());
                document.add(new Phrase("-------------------------------------------------------------------------------------\n",SectionHeaderFont));
                
                dataForReport = section.getSectionTextData();
                
                if (dataForReport.size() == 0) {
                    document.add(new Phrase(section.returnEmptyListString() + "\n",listElementFont));
                    continue;
                }
                
                for (String reportLine : dataForReport) {
                    document.add(new Phrase(reportLine + "\n",listElementFont));
                }
                
            
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR, cannot create report ", e);
               return false;
            }
        }
        return true;
        
    }
    
    public boolean populateRecordIdList (DataSection section, String keyValue) {
        
        String sql = XmlUtils.returnFirstSqlForTag(section.returnXmlTag());
        if (sql == null) {
            logger.log(Level.FINEST, "sql not found for " + section.returnXmlTag());
            return false;
        }
        logger.log(Level.FINEST, "SQL: " + sql);;
        
        if (sql.contains("?RPT_HOURS?")) {
            sql = sql.replace("?RPT_HOURS?", XmlUtils.getConfigValue("rptHours"));
        }
        if (sql.contains("?MULTIRPT_KEYVAL?")) {
            
            if (XmlUtils.getConfigValue("useMasterSubPairs").equals("true") ) {
                VfcuMd5FileHierarchy vfcuMd5FileHierarchy = new VfcuMd5FileHierarchy();
                vfcuMd5FileHierarchy.setMasterFileVfcuMd5FileId(Integer.parseInt(keyValue));
                vfcuMd5FileHierarchy.populateSubfileIdForMasterId();
                
                keyValue = keyValue + "," + vfcuMd5FileHierarchy.getSubFileVfcuMd5FileId();
                sql = sql.replace("?MULTIRPT_KEYVAL?", keyValue);      
            }
            else {
                sql = sql.replace("?MULTIRPT_KEYVAL?", keyValue);
            }
        }
            
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {   
                 recordIdList.add(rs.getInt(1));
            }   
        }   
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to Obtain list for failed report, returning", e);
            return false;
        }            

        return true;
    }
}
