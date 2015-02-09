/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS_redesigned.DAMSDatabase;


public class SiAssetMetaData {
    //class attributes
    String caption;
    String credit;
    String description;
    String digitalItemNotes;
    String groupTitle;
    String isRestricted;
    String keywords;
    String maxIdsSize;
    String otherConstraints;
    String primaryCreator;
    String seriesTitle;
    String termsAndRestrictions;
    String title;
    String useRestrictions;
    String sourceSystemId;
    String workCreationDate;
        
    
    // get functions
    public String getCaption () {
        return caption;
    }
    
    public String getCredit () {
        return credit;
    }
    
    public String getDescription () {
        return description;
    }
    
    public String getDigitalItemNotes () {
        return digitalItemNotes;
    }
    
    public String getGroupTitle () {
        return groupTitle;
    } 
    
    public String getIsRestricted () {
        return isRestricted;
    } 
    
    public String getKeywords () {
        return keywords;
    } 
    
    public String getMaxIdsSize () {
        return maxIdsSize;
    } 
    
    public String getOtherConstraints () {
        return otherConstraints;
    }
    
    public String getPrimaryCreator () {
        return primaryCreator;
    } 
    
    public String getSeriesTitle () {
        return seriesTitle;
    } 
    
    public String getTitle () {
        return title;
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
    
    public void appendKeywords(String keywords, String delimiter) {
        keywords = scrubString(keywords);
        
        if (this.keywords.length() + keywords.length() > 4000 ) {
            this.keywords = this.keywords += delimiter + keywords.substring(0,3999);
        }
        else {
            this.keywords = keywords;
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
        this.maxIdsSize = scrubString(maxIdsSize);
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
    
    public void setPrimaryCreator(String primaryCreator) {
        primaryCreator = scrubString(primaryCreator);
        
	if (primaryCreator.length() > 1000) {
                this.primaryCreator = primaryCreator.substring(0,999);
        }
        else {
            this.primaryCreator = primaryCreator;
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
        
}
