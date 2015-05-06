/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.CIS.Database.CDISTable;
import edu.si.CDIS.utilties.DataProvider;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.sql.Connection;
import java.util.logging.Logger;


public class SiAssetMetaData {
    //class attributes
    String caption;
    String credit;
    String description;
    String digitalItemNotes;
    String groupTitle;
    String intellectualContentCreator;
    String isRestricted;
    String keywords;
    String maxIdsSize;
    String namedPerson;
    String otherConstraints;
    String owningUnitUniqueName;
    String primaryCreator;
    String rightsHolder;
    String seriesTitle;
    String termsAndRestrictions;
    String title;
    String uoiid;  
    String useRestrictions;
    String sourceSystemId;
    String workCreationDate;
    Connection damsConn;
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
      
    
    // get functions
    public String getCaption () {
        return this.caption;
    }
    
    public String getCredit () {
        return this.credit;
    }
    
    public String getDescription () {
        return this.description;
    }
    
    public String getDigitalItemNotes () {
        return this.digitalItemNotes;
    }
    
    public String getGroupTitle () {
        return this.groupTitle;
    } 
    
    public String getIsRestricted () {
        return this.isRestricted;
    } 
     
    public String getIntellectualContentCreator () {
        return this.intellectualContentCreator;
    } 
    
    public String getKeywords () {
        return this.keywords;
    } 
    
    public String getMaxIdsSize () {
        return this.maxIdsSize;
    } 
    
    public String getNamedPerson () {
        return this.namedPerson;
    }
    
    public String getOtherConstraints () {
        return this.otherConstraints;
    }
    
    public String getOwningUnitUniqueName () {
        return this.owningUnitUniqueName;
    }
    
    public String getPrimaryCreator () {
        return primaryCreator;
    } 
    
    public String getRightsHolder () {
        return rightsHolder;
    } 
    
    public String getSeriesTitle () {
        return seriesTitle;
    } 
    
    public String getTitle () {
        return title;
    } 
    
    public String getUoiid () {
        return uoiid;
    } 
    
    public String getTermsAndRestrictions () {
        return termsAndRestrictions;
    } 
    
    public String getUseRestrictions () {
        return useRestrictions;
    } 

    public String getSourceSystemId () {
        return sourceSystemId;
    } 
    
    public String getWorkCreationDate () {
        return workCreationDate;
    } 
    
    
    
    // set functions
    
    public void appendCaption(String caption, String delimiter) {
        caption = scrubString(caption);
        
        if ( this.caption != null ) {
            
            if (this.caption.length() + caption.length() > 4000 ) {
                this.caption = this.caption += delimiter + caption.substring(0,3999);
            }
            else {
                this.caption = this.caption += delimiter + caption;
            }
        }
        else {
                this.caption = caption;
        }
    }
        
    public void appendKeywords(String keywords, String delimiter) {
        keywords = scrubString(keywords);
        
        if ( this.keywords != null ) {
            
            if (this.keywords.length() + keywords.length() > 4000 ) {
                this.keywords = this.keywords += delimiter + keywords.substring(0,3999);
            }
            else {
                this.keywords = this.keywords += delimiter + keywords;
            }
        }
        else {
                this.keywords = keywords;   
        }
    }
    
    public void appendNamedPerson (String namedPerson, String delimiter) {
        namedPerson = scrubString(namedPerson);
        
        if ( this.namedPerson != null ) {
            
            if (this.namedPerson.length() + namedPerson.length() > 2000 ) {
                this.namedPerson = this.namedPerson += delimiter + namedPerson.substring(0,1999);
            }
            else {
                this.namedPerson = this.namedPerson += delimiter + namedPerson;
            }
        }
        else {
                this.namedPerson = namedPerson;   
        }
    }
    
    public void setCaption(String caption) {
        caption = scrubString(caption);
        
        if ( caption.length() > 4000 ) {
        	this.caption = caption.substring(0,3999);
        }
        else {
            this.caption = caption;
        }
    }
    
    public void setCredit(String credit) {
        credit = scrubString(credit);
        
        if ( credit.length() > 2000 ) {
        	this.credit = credit.substring(0,1999);
        }
        else {
            this.credit = credit;
        }
    }
    
    public void setDescription(String description) {
         description = scrubString(description);
         
        if ( description.length() > 4000 ) {
        	this.description = description.substring(0,3999);
        }
        else {
            this.description = description;
        }
    }
    
    public void setDigitalItemNotes(String digitalItemNotes) {
        digitalItemNotes = scrubString(digitalItemNotes);
        
        if ( digitalItemNotes.length() > 200 ) {
        	this.digitalItemNotes = digitalItemNotes.substring(0,199);
        }
        else {
            this.digitalItemNotes = digitalItemNotes;
        }
    }
    
    public void setGroupTitle(String groupTitle) {
        groupTitle = scrubString(groupTitle);
        
        if (groupTitle.length() > 2000 ) {
            this.groupTitle = groupTitle.substring(0,1999);
        }
        else {
            this.groupTitle = groupTitle;
        }
    }
    
    public void setIntellectualContentCreator(String intellectualContentCreator) {
        intellectualContentCreator = scrubString(intellectualContentCreator);
                
        if (intellectualContentCreator.length() > 999 ) {
            this.intellectualContentCreator = intellectualContentCreator.substring(0,1999);
        }
        else {
            this.intellectualContentCreator = intellectualContentCreator;
        }
    }
    
    public void setIsRestricted(String isRestricted) {   
       
        if (isRestricted.equals("Yes")) {
            this.isRestricted = "Yes";
        }
        else {
            this.isRestricted = "No";
        }
    }
    
    public void setKeywords(String keywords) {
        keywords = scrubString(keywords);
        
        if (keywords.length() > 4000 ) {
            this.keywords = keywords.substring(0,3999);
        }
        else {
            this.keywords = keywords;
        }
    }
    
    public void setMaxIdsSize(String maxIdsSize) {
        
        maxIdsSize = scrubString(maxIdsSize);
        
        //make sure our maxIDS Size is an Int
        try {
            Integer maxIdsSizeInt = Integer.parseInt(maxIdsSize);
            
            this.maxIdsSize = maxIdsSizeInt.toString();
            
        } catch (Exception e) {
            // We didnt have a valid (numeric) max ids size, set it to the default (null)
            //dont set maxIds size to anything
        }
        
    }
    
    public void setNamedPerson(String namedPerson) {
        namedPerson = scrubString(namedPerson);
        
	if (namedPerson.length() > 2000) {
                this.namedPerson = namedPerson.substring(0,1999);
        }
        else {
            this.namedPerson = namedPerson;
        }
    }
    
    public void setOtherConstraints(String otherConstraints) {
        otherConstraints = scrubString(otherConstraints);
        
	if (otherConstraints.length() > 2000) {
                this.otherConstraints = otherConstraints.substring(0,1999);
        }
        else {
            this.otherConstraints = otherConstraints;
        }
    }
    
    public void setOwningUnitUniqueName(String owningUnitUniqueName) {
        this.owningUnitUniqueName = owningUnitUniqueName;
    }
    
    public void setPrimaryCreator(String primaryCreator) {
        primaryCreator = scrubString(primaryCreator);
        
	if (primaryCreator.length() > 1000) {
                this.primaryCreator = primaryCreator.substring(0,999);
        }
        else {
            this.primaryCreator = primaryCreator;
        }
    }
    
    public void setRightsHolder(String rightsHolder) {
        rightsHolder = scrubString(rightsHolder);
        
	if (rightsHolder.length() > 2000) {
                this.rightsHolder = rightsHolder.substring(0,1999);
        }
        else {
            this.rightsHolder = rightsHolder;
        }
    }
    
    public void setSeriesTitle(String seriesTitle) {
        seriesTitle = scrubString(seriesTitle);
        
	if (seriesTitle.length() > 150) {
                this.seriesTitle = seriesTitle.substring(0,149);
        }
        else {
            this.seriesTitle = seriesTitle;
        }
    }
    
    public void setTermsAndRestrictions(String termsAndRestrictions) {
        termsAndRestrictions = scrubString(termsAndRestrictions);
        
	if (termsAndRestrictions.length() > 4000) {
                this.termsAndRestrictions = termsAndRestrictions.substring(0,3999);
        }
        else {
            this.termsAndRestrictions = termsAndRestrictions;
        }
    }
    
    public void setTitle(String title) {
        title = scrubString(title);
        
	if (title.length() > 2000) {
                this.title = title.substring(0,1999);
        }
        else {
            this.title = title;
        }
    }
    
    public void setUoiid(String uoiid) {
           this.uoiid = uoiid;
    }
    
    public void setUseRestrictions(String useRestrictions) {
        useRestrictions = scrubString(useRestrictions);
        
        if (useRestrictions.length() > 250 ) {
            this.useRestrictions = useRestrictions.substring(0,249);
        }
        else {
            this.useRestrictions = useRestrictions;
        }
    }

    public void setSourceSystemId(String sourceSystemId) {
	this.sourceSystemId = sourceSystemId;
    }
    
    public void setWorkCreationDate(String workCreationDate) {
        workCreationDate = scrubString(workCreationDate);
        
        // get rid of extra leading 0's in the date
        workCreationDate = workCreationDate.replaceAll(" 0", "");
        
        if (workCreationDate.length() > 255 ) {
            this.workCreationDate = workCreationDate.substring(0,254);
        }
        else {
            this.workCreationDate = workCreationDate;
        }
    }
    
    // This method cleans/reformats the string data
    
    private String scrubString(String inputString) {
          
        String newString;
        
        // remove & for easy insert into db
        newString = inputString.replaceAll("&", "and");
		
	//escape any single quotes
	newString = newString.replaceAll("'", "''");
        
        // remove leading and trailing spaces
        newString = newString.trim();
        
        return newString;
        
    }
    
    /*  Method :        updateDAMSSourceSystemID
        Arguments:      
        Description:    
        RFeldman 2/2015
    */
    
    public int updateDAMSSourceSystemID (Connection damsConn, String uoiid, String sourceSystemId) {
        int recordsUpdated = 0;
        Statement stmt = null;
        
        String sql = "update SI_ASSET_METADATA set source_system_id = '" + sourceSystemId + "' " +
                    "where UOI_ID = '" + uoiid + "'";

        logger.log(Level.FINEST, "SQL! {0}", sql);
        
        try {
            recordsUpdated = DataProvider.executeUpdate(this.damsConn, sql);
        
            stmt = this.damsConn.createStatement();
            recordsUpdated = stmt.executeUpdate(sql);
        
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
                e.printStackTrace();
        }finally {
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
            
        return recordsUpdated;
        
    }
}
