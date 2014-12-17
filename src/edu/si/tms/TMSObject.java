/**
 CDIS 2.0 - Common Code
 TMSObject.java
 */
package edu.si.tms;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;


public class TMSObject {
	
	private String uoiID;
	private String objectID;
	private String seriesTitle;
	private String keywords;
	private String credit;
	private String workCreationDate;
	private String beginningDate;
	private String endDate;
	private String otherDates;
	private String primaryCreator; 
	private String primaryCreatorRole; 
	private String primarySubject; 
	private String primarySubjectRole; 
	private String owningUnit;
	private String acquisitionNature;
	private String acquiredFrom;
	private String acquisitionCost;
	private String acquisitionNotes;
	private String librarianName;
	private String owningUnitUniqueName;
	private String sourceSystemID;
	private String groupTitle;
	private String relatedSICollectionObjects;
	private String notes;
	private String holdingUnit;
	private String rightsHolder;
	private String acquisitionDate;
	private String imageID;
	private String medium;
	private String dimensions;
	private String classification;
	private String subClassification;
	private String paperSupport;
	private String inscribed;
	private String state;
	private String edition;
	private String description;
	private String caption;

	/**
	 * @return the medium
	 */
	public String getMedium() {
		return medium;
	}

	/**
	 * @param medium the medium to set
	 */
	public void setMedium(String medium) {
		this.medium = medium;
	}

	/**
	 * @return the dimensions
	 */
	public String getDimensions() {
		return dimensions;
	}

	/**
	 * @param dimensions the dimensions to set
	 */
	public void setDimensions(String dimensions) {
		this.dimensions = dimensions;
	}

	/**
	 * @return the classification
	 */
	public String getClassification() {
		return classification;
	}

	/**
	 * @param classification the classification to set
	 */
	public void setClassification(String classification) {
		this.classification = classification;
	}

	/**
	 * @return the subClassification
	 */
	public String getSubClassification() {
		return subClassification;
	}

	/**
	 * @param subClassification the subClassification to set
	 */
	public void setSubClassification(String subClassification) {
		this.subClassification = subClassification;
	}

	/**
	 * @return the paperSupport
	 */
	public String getPaperSupport() {
		return paperSupport;
	}

	/**
	 * @param paperSupport the paperSupport to set
	 */
	public void setPaperSupport(String paperSupport) {
		this.paperSupport = paperSupport;
	}

	/**
	 * @return the inscribed
	 */
	public String getInscribed() {
		return inscribed;
	}

	/**
	 * @param inscribed the inscribed to set
	 */
	public void setInscribed(String inscribed) {
		this.inscribed = inscribed;
	}

	/**
	 * @return the state
	 */
	public String getState() {
		return state;
	}

	/**
	 * @param state the state to set
	 */
	public void setState(String state) {
		this.state = state;
	}

	/**
	 * @return the edition
	 */
	public String getEdition() {
		return edition;
	}

	/**
	 * @param edition the edition to set
	 */
	public void setEdition(String edition) {
		this.edition = edition;
	}

	public String toString() {
		  StringBuilder result = new StringBuilder();
		  String newLine = System.getProperty("line.separator");

		  result.append( this.getClass().getName() );
		  result.append( " Object {" );
		  result.append(newLine);

		  //determine fields declared in this class only (no fields of superclass)
		  Field[] fields = this.getClass().getDeclaredFields();

		  //print field names paired with their values
		  for ( Field field : fields  ) {
		    result.append("  ");
		    try {
		      result.append( field.getName() );
		      result.append(": ");
		      //requires access to private field:
		      result.append( field.get(this) );
		    } catch ( IllegalAccessException ex ) {
		      System.out.println(ex);
		    }
		    result.append(newLine);
		  }
		  result.append("}");

		  return result.toString();
	}

	/**
	 * @return the uoiID
	 */
	public String getUoiID() {
		return uoiID;
	}

	/**
	 * @param uoiID the uoiID to set
	 */
	public void setUoiID(String uoiID) {
		this.uoiID = uoiID;
	}

	public String getObjectID() {
		return objectID;
	}

	public void setObjectID(String objectID) {
		this.objectID = objectID;
	}

	/**
	 * @return the seriesTitle
	 */
	public String getSeriesTitle() {
		return seriesTitle;
	}

	/**
	 * @param seriesTitle the seriesTitle to set
	 */
	public void setSeriesTitle(String seriesTitle) {
		this.seriesTitle = seriesTitle;
	}

	/**
	 * @return the keywords
	 */
	public String getKeywords() {
		return keywords;
	}

	/**
	 * @param keywords the keywords to set
	 */
	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	/**
	 * @return the credit
	 */
	public String getCredit() {
		return credit;
	}

	/**
	 * @param credit the credit to set
	 */
	public void setCredit(String credit) {
		this.credit = credit;
	}

	/**
	 * @return the workCreationDate
	 */
	public String getWorkCreationDate() {
		return workCreationDate;
	}

	/**
	 * @param workCreationDate the workCreationDate to set
	 */
	public void setWorkCreationDate(String workCreationDate) {
		this.workCreationDate = workCreationDate;
	}

	/**
	 * @return the beginningDate
	 */
	public String getBeginningDate() {
		return beginningDate;
	}

	/**
	 * @param beginningDate the beginningDate to set
	 */
	public void setBeginningDate(String beginningDate) {
		this.beginningDate = beginningDate;
	}

	/**
	 * @return the endDate
	 */
	public String getEndDate() {
		return endDate;
	}

	/**
	 * @param endDate the endDate to set
	 */
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	/**
	 * @return the otherDates
	 */
	public String getOtherDates() {
		return otherDates;
	}

	/**
	 * @param otherDates the otherDates to set
	 */
	public void setOtherDates(String otherDates) {
		this.otherDates = otherDates;
	}

	/**
	 * @return the primaryCreator
	 */
	public String getPrimaryCreator() {
		return primaryCreator;
	}

	/**
	 * @param primaryCreator the primaryCreator to set
	 */
	public void setPrimaryCreator(String primaryCreator) {
		this.primaryCreator = primaryCreator;
	}

	/**
	 * @return the primaryCreatorRole
	 */
	public String getPrimaryCreatorRole() {
		return primaryCreatorRole;
	}

	/**
	 * @param primaryCreatorRole the primaryCreatorRole to set
	 */
	public void setPrimaryCreatorRole(String primaryCreatorRole) {
		this.primaryCreatorRole = primaryCreatorRole;
	}

	/**
	 * @return the primarySubject
	 */
	public String getPrimarySubject() {
		return primarySubject;
	}

	/**
	 * @param primarySubject the primarySubject to set
	 */
	public void setPrimarySubject(String primarySubject) {
		this.primarySubject = primarySubject;
	}

	/**
	 * @return the primarySubjectRole
	 */
	public String getPrimarySubjectRole() {
		return primarySubjectRole;
	}

	/**
	 * @param primarySubjectRole the primarySubjectRole to set
	 */
	public void setPrimarySubjectRole(String primarySubjectRole) {
		this.primarySubjectRole = primarySubjectRole;
	}

	/**
	 * @return the owningUnit
	 */
	public String getOwningUnit() {
		return owningUnit;
	}

	/**
	 * @param owningUnit the owningUnit to set
	 */
	public void setOwningUnit(String owningUnit) {
		this.owningUnit = owningUnit;
	}

	/**
	 * @return the acquisitionNature
	 */
	public String getAcquisitionNature() {
		return acquisitionNature;
	}

	/**
	 * @param acquisitionNature the acquisitionNature to set
	 */
	public void setAcquisitionNature(String acquisitionNature) {
		this.acquisitionNature = acquisitionNature;
	}

	/**
	 * @return the acquiredFrom
	 */
	public String getAcquiredFrom() {
		return acquiredFrom;
	}

	/**
	 * @param acquiredFrom the acquiredFrom to set
	 */
	public void setAcquiredFrom(String acquiredFrom) {
		this.acquiredFrom = acquiredFrom;
	}

	/**
	 * @return the acquisitionCost
	 */
	public String getAcquisitionCost() {
		return acquisitionCost;
	}

	/**
	 * @param acquisitionCost the acquisitionCost to set
	 */
	public void setAcquisitionCost(String acquisitionCost) {
		this.acquisitionCost = acquisitionCost;
	}

	/**
	 * @return the acquisitionNotes
	 */
	public String getAcquisitionNotes() {
		return acquisitionNotes;
	}

	/**
	 * @param acquisitionNotes the acquisitionNotes to set
	 */
	public void setAcquisitionNotes(String acquisitionNotes) {
		this.acquisitionNotes = acquisitionNotes;
	}

	/**
	 * @return the librarianName
	 */
	public String getLibrarianName() {
		return librarianName;
	}

	/**
	 * @param librarianName the librarianName to set
	 */
	public void setLibrarianName(String librarianName) {
		this.librarianName = librarianName;
	}

	/**
	 * @return the owningUnitUniqueName
	 */
	public String getOwningUnitUniqueName() {
		return owningUnitUniqueName;
	}

	/**
	 * @param owningUnitUniqueName the owningUnitUniqueName to set
	 */
	public void setOwningUnitUniqueName(String owningUnitUniqueName) {
		this.owningUnitUniqueName = owningUnitUniqueName;
	}

	/**
	 * @return the sourceSystemID
	 */
	public String getSourceSystemID() {
		return sourceSystemID;
	}

	/**
	 * @param sourceSystemID the sourceSystemID to set
	 */
	public void setSourceSystemID(String sourceSystemID) {
		this.sourceSystemID = sourceSystemID;
	}

	/**
	 * @return the groupTitle
	 */
	public String getGroupTitle() {
		return groupTitle;
	}

	/**
	 * @param groupTitle the groupTitle to set
	 */
	public void setGroupTitle(String groupTitle) {
		this.groupTitle = groupTitle;
	}

	/**
	 * @return the relatedSICollectionObjects
	 */
	public String getRelatedSICollectionObjects() {
		return relatedSICollectionObjects;
	}

	/**
	 * @param relatedSICollectionObjects the relatedSICollectionObjects to set
	 */
	public void setRelatedSICollectionObjects(String relatedSICollectionObjects) {
		this.relatedSICollectionObjects = relatedSICollectionObjects;
	}

	/**
	 * @return the notes
	 */
	public String getNotes() {
		return notes;
	}

	/**
	 * @param notes the notes to set
	 */
	public void setNotes(String notes) {
		this.notes = notes;
	}

	/**
	 * @return the holdingUnit
	 */
	public String getHoldingUnit() {
		return holdingUnit;
	}

	/**
	 * @param holdingUnit the holdingUnit to set
	 */
	public void setHoldingUnit(String holdingUnit) {
		this.holdingUnit = holdingUnit;
	}

	/**
	 * @return the rightsHolder
	 */
	public String getRightsHolder() {
		return rightsHolder;
	}

	/**
	 * @param rightsHolder the rightsHolder to set
	 */
	public void setRightsHolder(String rightsHolder) {
		this.rightsHolder = rightsHolder;
	}

	/**
	 * @return the acquisitionDate
	 */
	public String getAcquisitionDate() {
		return acquisitionDate;
	}

	/**
	 * @param acquisitionDate the acquisitionDate to set
	 */
	public void setAcquisitionDate(String acquisitionDate) {
		this.acquisitionDate = acquisitionDate;
	}

	/**
	 * @return the imageID
	 */
	public String getImageID() {
		return imageID;
	}

	/**
	 * @param imageID the imageID to set
	 */
	public void setImageID(String imageID) {
		this.imageID = imageID;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getCaption() {
		return caption;
	}

	public void setCaption(String caption) {
		this.caption = caption;
	}

}
