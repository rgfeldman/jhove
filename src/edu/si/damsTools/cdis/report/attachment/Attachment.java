/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.attachment;

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
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.report.DisplayFormat;
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
import edu.si.damsTools.cdis.report.Report;
import edu.si.damsTools.utilities.XmlQueryData;

public class Attachment extends Report  {
        
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String fileNameLoc;
    private Document document;
    private final ArrayList<String> statsList;
    
    private final RtfFont headerFont;
    private final RtfFont SectionHeaderFont;
    private final RtfFont listElementFont;
    
    private String keyValue;
    

    
    public void setKeyValue (String keyValue) {
        this.keyValue = keyValue;
    }
    
    public Attachment () {
        
        super();
        
        this.headerFont = new RtfFont("Times New Roman",14,Font.BOLD);
        this.SectionHeaderFont = new RtfFont("Times New Roman",12,Font.BOLD);
        this.listElementFont = new RtfFont("Courier",8);
        
        statsList = new ArrayList<>();
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
        
        String timeStamp;
        String timeStampWords;
        
        //get the date timestamp for use in the report file
        DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmmss");
        timeStamp = df.format(new Date());
        
        DateFormat dfWords = new SimpleDateFormat();
        timeStampWords = dfWords.format(new Date());
        
        this.fileNameLoc =  DamsTools.getDirectoryName()+ "/rpt/CDISRPT-" + DamsTools.getProjectCd().toUpperCase() + "-" + timeStamp + ".rtf";
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
        
        genData(sections);
        printStats();
        printSectionData(sections);
        
        return true;
    }
    
    public boolean genData (List<DataSection> sections) {
                
        for (Iterator<DataSection> it = sections.iterator(); it.hasNext();) {
            DataSection section = it.next();
            try {
                 
                List<CdisMap> cdisMapList = new ArrayList<>();
            
                cdisMapList = returnCdisMapList(section, keyValue);
                        
                if (cdisMapList == null) {
                    //this section returned an error
                    logger.log(Level.FINEST, "Error obtained while obtain mapId list, or list type does not apply");
                    it.remove();
                    continue;
                } 
                
                for (CdisMap cdisMap : cdisMapList) {
                    //Get the text for this cdisMapId for this section of the report
                    section.generateTextForRecord(cdisMap);
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
    
    public List returnCdisMapList (DataSection section, String keyValue) {
        
        //start with a null list, if the list doesnt apply we want a null list rather than an empty list
        List<CdisMap> idList = new ArrayList<CdisMap>();
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type",section.returnXmlTag());
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.FINEST, "sql not found for " + section.returnXmlTag());
            return idList;
        }
        logger.log(Level.FINEST, "SQL: " + sql);;
        
        if (sql.contains("?RPT_HOURS?")) {
            sql = sql.replace("?RPT_HOURS?", DamsTools.getProperty("rptHours"));
        }
        if (sql.contains("?MULTIRPT_KEYVAL?")) {
            sql = sql.replace("?MULTIRPT_KEYVAL?", keyValue);
        }
            
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) { 
                CdisMap cdisMap = new CdisMap();
                cdisMap.setCdisMapId(rs.getInt(1));
                cdisMap.populateMapInfo();
                idList.add(cdisMap);
            }
        }   
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to Obtain list for failed report, returning", e);
            return null;
        }            
        return idList;   
    }
}
