/**
 CDIS 2.0 - Common Code
 TMSMediaRendition.java
 */

package edu.si.tms;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Properties;

import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;

import com.jamesmurty.utils.XMLBuilder;

import edu.si.data.DataProvider;

public class TMSMediaRendition {
	
	private String renditionID;
	private String renditionNumber;
	private String name;
	private String masterObjectName;
	private String title;
	private String workType;
	private String caption;
	private String description;
	private String workNotes;
	private String rightsSummary;
	private String termsRestrictions;
	private String useRestrictions;
	private String color;
	private String duration;
	private String technicalQuality;
	private String bitDepth;
	private String captureDevice;
	private String digitalItemNotes;
	private String mediaFormat;
	private String mediaDimensions;
	private String copyright;
	private String structuralPath;
	private String repositoryCode;
	private String action;
	private String fileName;
	private String publicAccess;
	private String objectPublicAccess;
	
	private TMSObject objectData = null;
	
	
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
	
	private void populateMediaData(Connection tmsconn, String renditionID, Logger log) throws SQLException {
		
		try {
			
			String mediaSQL = "select mf.FileName, mf.Duration, mf.PixelH, " +
					"mf.PixelW, mp.Path, mm.PublicCaption, mm.Description, mm.Remarks, " +
					"mm.Copyright, mm.Restrictions, mr.Quality, mr.Remarks, ms.MediaStatus," +
					"mr.RenditionNumber, mm.PublicAccess " +
					"from MediaFiles mf, MediaRenditions mr, MediaMaster mm, MediaStatuses ms, MediaPaths mp " +
					"where " +
					"mf.FileID = mr.PrimaryFileID AND " +
					"mr.MediaMasterID = mm.MediaMasterID AND " +
					"mr.MediaStatusID = ms.MediaStatusID AND " +
					"mf.PathID = mp.PathID AND " +
					"mr.RenditionID = " + renditionID;
			
			
			log.log(Level.ALL, "SQL: " + mediaSQL);
			
			ResultSet rs;
			
			rs = DataProvider.executeSelect(tmsconn, mediaSQL);
			boolean foundRendition = false;
			
			while(rs.next()) {
				
				foundRendition = true;
				this.setRenditionID(renditionID);
				
				if(rs.getString(1).contains("\\")) {
					this.setName(rs.getString(1).split("\\\\")[rs.getString(1).split("\\\\").length-1]);
					this.setMasterObjectName(rs.getString(1).split("\\\\")[rs.getString(1).split("\\\\").length-1]);
				}
				else {
					this.setName(rs.getString(1));
					this.setMasterObjectName(rs.getString(1));
				}
				this.setFileName(rs.getString(1));
				/*if(rs.getString(2).equals("8-Bit Grayscale")) {
					this.setColor("No");
					this.setBitDepth("8-bit");
				}
				else if(rs.getString(2).equals("True Color")) {
					this.setColor("Yes");
					this.setBitDepth("16-bit");
				}
				else {
					this.setColor(null);
					this.setBitDepth(null);
				}*/
				this.setDuration(rs.getString(2));
				this.setMediaDimensions(rs.getString(3) + "x" + rs.getString(4));
				this.setStructuralPath(rs.getString(5));
				this.setCaption(rs.getString(6));
				this.setDescription(rs.getString(7));
				this.setWorkNotes(rs.getString(8));
				this.setTermsRestrictions(rs.getString(9));
				this.setUseRestrictions(rs.getString(10));
				this.setTechnicalQuality(rs.getString(11));
				this.setDigitalItemNotes(rs.getString(12));		
				this.setRightsSummary(rs.getString(12));
				this.setRenditionNumber(rs.getString(14));
				this.setTitle(rs.getString(14));
				if(rs.getInt(15) == 1) {
					this.setPublicAccess("Yes");
				}
				else {
					this.setPublicAccess("No");
				}		
			}
			
			if(!foundRendition) {
				throw new SQLException("No rendition records returned from populateMediaData query.");
			}
			
		}
		catch(SQLException sqlex) {
			throw new SQLException("SQLException thrown in populateMediaData", sqlex);
		}
		
	}
	
	private void populateObjectData(Connection tmsconn, String renditionID , Properties properties, Logger log) throws SQLException {
		
		try {
			
			TMSObject object = new TMSObject();
			int count = 0;
			//check if only one object, if not, exit
			String countSQL = "select count(*) from Objects where ObjectID in " +
					"(select ID from MediaXrefs where TableID = 108 and MediaMasterID = " +
					"(select MediaMasterID from MediaRenditions where RenditionID = " + renditionID + "))";
			Statement countStmt = tmsconn.createStatement();
			ResultSet countrs = countStmt.executeQuery(countSQL);
			
			while(countrs.next()) {
				count = countrs.getInt(1);
			}
			
			if(count <= 1) {
				//pull from Objects table
				Statement stmt = tmsconn.createStatement();
				String objectSQL = "select * from Objects where ObjectID = " +
						"(select ID from MediaXrefs where TableID = 108 and MediaMasterID = " +
						"(select MediaMasterID from MediaRenditions where RenditionID = " + renditionID + "))";
				ResultSet rs;
				
				rs = DataProvider.executeSelect(tmsconn, objectSQL);
				
			    if(rs.next()){
					
					this.setTitle(rs.getString("ObjectNumber"));
					object.setCredit(rs.getString("CreditLine"));
					object.setBeginningDate(rs.getString("DateBegin"));
					object.setEndDate(rs.getString("DateEnd"));
					object.setHoldingUnit(rs.getString("DepartmentID"));
					object.setObjectID(rs.getString("ObjectID"));
					object.setMedium(rs.getString("Medium"));
					object.setWorkCreationDate(rs.getString("Dated"));
					object.setInscribed(rs.getString("Inscribed"));
					object.setPaperSupport(rs.getString("PaperSupport"));
					object.setState(rs.getString("State"));
					object.setEdition(rs.getString("Edition"));
					object.setClassification(rs.getString("ClassificationID")); //may need to change these to use lookups
					object.setSubClassification(rs.getString("SubClassID")); //may needs to change these to use lookups
                                        object.setSeriesTitle(rs.getString("ObjectName"));
                                         object.setCaption(rs.getString("ObjectNumber") + " " + rs.getString("ObjectName"));
                                        
                                         //   all units wanted description + dimensions except for CH
                                         //    The dimension is added in the update statement...where it and the description is available
                                         object.setDescription(rs.getString("Description"));
                                         
                                        if(! properties.getProperty("siUnit").equals("CHSDM")) {
                                            /* Several fields are now commented out, per Allison Halle (CHSM), these fields do not need to be mapped */
                                            object.setLibrarianName(rs.getString("Cataloguer"));
                                            object.setGroupTitle(rs.getString("Exhibitions"));
                                            object.setNotes(rs.getString("CuratorialRemarks"));
                                            object.setAcquisitionDate(rs.getString("CatalogueDateOld"));
                                            object.setDimensions(rs.getString("Dimensions"));
                                            
                                        }
           
					objectData = object;
					objectPublicAccess = (rs.getInt("PublicAccess") == 1)?"Yes":"No";
			    }
			    else {
			    	log.log(Level.ALL, "No object record associated with this rendition. Skipping Object fields...");
			    	objectData = null;
			    	objectPublicAccess = "Yes";
			    }
			}
			else {
				log.log(Level.ALL, "Multiple object records associated with this rendition. Skipping Object fields...");
				objectData = null;
				//get public access data for these objects
				String query = "select count(*) from Objects where ObjectID in " +
						"(select ID from MediaXrefs where TableID = 108 and MediaMasterID = " +
						"(select MediaMasterID from MediaRenditions where RenditionID = " + renditionID + ")) AND " +
								"PublicAccess = 1";
				Statement publicStmt = tmsconn.createStatement();
				ResultSet publicRS = publicStmt.executeQuery(query);
				
				if(publicRS.next()) {
					int publicCount = publicRS.getInt(1);
					if(publicCount > 0) {
						objectPublicAccess = "Yes";
					}
					else {
						objectPublicAccess = "No";
					}
				}
				else {
					log.log(Level.ALL, "No count record returned for object public use check. Setting to false.");
					objectPublicAccess = "No";
				}
			}
			
			
			
			
		}
		catch(SQLException sqlex) {
			sqlex.printStackTrace();
			throw new SQLException("SQLException thrown in populateObjectData", sqlex);
		}
		
		
		
	}
	
	private void populateConstituentData(Connection tmsconn, Properties properties, Logger log) throws SQLException {
		
		try {
			
			//pull from Constituents table
			Statement stmt = tmsconn.createStatement();
			String conSQL = "select a.AlphaSort, b.RoleID, b.Role " +
					"from Constituents a, Roles b, ObjConXrefs c " +
					"where a.ConstituentID = c.ConstituentID AND " +
					"b.RoleID = c.RoleID AND " +
					"(c.RoleID = 1 OR c.RoleID = 21) AND " +
					"c.ObjectID = " + objectData.getObjectID();
			
			ResultSet rs;
			
                        log.log(Level.ALL, "SQL: Getting constituent data: {0}", conSQL);
                        
			rs = DataProvider.executeSelect(tmsconn, conSQL);
			
			String constituentName, roleID, roleName;
		
			while(rs.next()) {
				roleID = rs.getString("RoleID");
				constituentName = rs.getString("AlphaSort");
				roleName = rs.getString("Role");
				if(roleID != null && roleID.equals("1")) {
					objectData.setPrimaryCreator(constituentName);
					objectData.setPrimaryCreatorRole("Creator");
				}
				else if(roleID != null && roleID.equals("21")) {
					objectData.setPrimarySubject(constituentName);
					objectData.setPrimarySubjectRole("Photograph Subject");
				}
			}
		}
		catch(SQLException sqlex) {
			throw new SQLException("SQLException thrown in populateConstituentsData", sqlex);
		}
		
		
		
	}
	
	public boolean populate(Connection conn, String renditionID, Properties properties, Logger log) {
		
		try {
			this.populateMediaData(conn, renditionID, log);
		}
		catch(SQLException sqlex) {
			log.log(Level.ALL, "Exception occurred in TMSMediaRendition.populateMediaData: {0}", sqlex.getMessage());
			//sqlex.printStackTrace();
			return false;
		}
		
		try {
			this.populateObjectData(conn, renditionID, properties, log);
		}
		catch(SQLException sqlex) {
			log.log(Level.ALL, "Exception occurred in TMSMediaRendition.populateObjectData: {0}", sqlex.getMessage());
			//sqlex.printStackTrace();
			return false;
		}
		
		if(this.objectData != null) {
			try {
				this.populateConstituentData(conn, properties, log);
			}
			catch(SQLException sqlex) {
				log.log(Level.ALL, "Exception occurred in TMSMediaRendition.populateConstituentData: {0}", sqlex.getMessage());
				//sqlex.printStackTrace();
				return false;
			}
		}
		
		return true;
			
	}

	public XMLBuilder getMetadataXMP() {
		
		XMLBuilder xml = null;
		try {
			xml = XMLBuilder.create("tmsAsset");
			
			xml.e("x:xmpmeta")
				.a("xmlns:x", "adobe:ns:meta/")
				.a("x:xmptk", "XMP Core 5.1.2")
				.e("rdf:RDF")
					.a("xmlns:rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
					.e("rdf:Description")
						.a("rdf:about", "")
						.a("xmlns:photoshop", "http://ns.adobe.com/photoshop/1.0")
						.a("xmlns:xmpRights", "http://ns.adobe.com/xap/1.0/rights")
						.a("xmlns:dc", "http://purl.org/dc/elements/1.1")
						.a("xmlns:Iptc4xmpCore", "http://iptc.org/std/Iptc4xmpCore/1.0/xmlns")
						.a("xmlns:Iptc4xmpExt", "http://iptc.org/std/Iptc4xmpExt/2008-02-29/")
						.a("xmlns:plus", "http://ns.useplus.org/ldf/xmp/1.0/")
						.a("xmlns:aux", "http://ns.adobe.com/exif/1.0/aux/")
						.a("xmlns:photomechanic", "http://ns.camerabits.com/photomechanic/1.0/")
						.a("photoshop:City", "City")
						.a("photoshop:State", "State")
						.a("photoshop:Country", "Country")
						.a("photoshop:AuthorsPosition", "Creator Job Title")
						.a("photoshop:Credit", (this.objectData != null)?this.objectData.getCredit():"Credit Line")
						.a("photoshop:Source", "Source")
						.a("photoshop:CaptionWriter", "Caption Writer")
						.a("photoshop:Headline", "Headline")
						.a("photoshop:Instructions", "Instructions")
						.a("photoshop:TransmissionReference", "Job Identifier")
						.a("photoshop:DateCreated", "2001-01-01T00:00:00-04:00")
						.a("xmpRights:Marked", "True")
						.a("Iptc4xmpCore:CountryCode", "ISO Country")
						.a("Iptc4xmpCore:Location", "Sublocation")
						.a("Iptc4xmpCore:IntellectualGenre", "Intellectual Genre")
						.a("Iptc4xmpExt:AddlModelInfo", "Additional Model info")
						.a("Iptc4xmpExt:MaxAvailWidth", "Max Available Width")
						.a("Iptc4xmpExt:MaxAvailHeight", "Max Available Height")
						.a("Iptc4xmpExt:DigitalSourceType", "http://cv.iptc.org/newscodes/digitalsourcetype/digitalCapture")
						.a("plus.MinorModelAgeDisclosure", "http://ns.useplus.org/ldf/vocab/AG-UNK")
						.a("plus.ModelReleaseStatus", "http://ns.useplus.org/ldf/vocab/MR-UMR")
						.a("plus.PropertyReleaseStatus", "http://ns.useplus.org/ldf/vocab/PR-UPR")
						.a("plus.ImageSupplierImageID", "Supplier's Image ID")
						.a("aux.ImageNumber", "0")
						.a("photomechanic:Prefs", "-1:-1:-1:000000")
						.a("photomechanic:PMVersion", "PM5")
						.e("xmlRights:UsageTerms")
							.e("rdf:Alt")
								.e("rdf:li")
									.a("xml:lang", "x-default")
									.t("Rights Usage Terms")
								.up() //end rdf:li
							.up() //end rdf:Alt
						.up() //end xmlRights.UsageTerms
						.e("dc:subject")
							.e("rdf:Bag")
								.e("rdf:li")
									.t("Keywords")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end dc:subject
						.e("dc.description")
							.e("rdf:Alt")
								.e("rdf:li")
									.a("xml:lang", "x-default")
									.t((this.description != null)?this.description:"")
								.up() //end rdf:li
							.up() //end rdf:Alt
						.up() //end dc.description
						.e("dc.creator")
							.e("rdf:Seq")
								.e("rdf.li")
									.t((this.objectData != null)?((this.objectData.getPrimaryCreator() != null)?this.objectData.getPrimaryCreator():""):"")
								.up() //end rdf.li
							.up() //end rdf.Seq
						.up() //end dc.creator
						.e("dc.title")
							.e("rdf:Alt")
								.e("rdf:li")
									.a("xml:lang", "x-default")
									.t("")
								.up() //end rdf:li
							.up() //end rdf:Alt
						.up() //end dc.title
						.e("dc.rights")
							.e("rdf:Alt")
								.e("rdf:li")
									.a("xml:lang", "x-default")
									.t((this.rightsSummary != null)?this.rightsSummary:"")
								.up() //end rdf:li
							.up() //end rdf:Alt
						.up() //end dc.rights
						.e("Iptc4xmpCore:Scene")
							.e("rdf:Bag")
								.e("rdf:li")
									.t("IPTC Scene Code")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end Iptc4xmpCore:Scene
						.e("Iptc4xmpCore:SubjectCode")
							.e("rdf:Bag")
								.e("rdf:li")
									.t("IPTC Subject Code")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end Iptc4xmpCore:SubjectCode
						.e("Iptc4xmpCore:CreatorContactInfo")
							.a("Iptc4xmpCore:CiAdrExtadr", "Creator Address")
							.a("Iptc4xmpCore:CiAdrCity", "Creator City")
							.a("Iptc4xmpCore:CiAdrRegion", "Creator State")
							.a("Iptc4xmpCore:CiAdrPcode", "Creator Postal Code")
							.a("Iptc4xmpCore:CiAdrCtry", "Creator Country")
							.a("Iptc4xmpCore:CiTelWork", "Creator Phones")
							.a("Iptc4xmpCore:CiEmailWork", "Creator Emails")
							.a("Iptc4xmpCore:CiUrlWork", "Creator Website")
						.up() //end Iptc4xmpCore:CreatorContactInfo
						.e("Iptc4xmpExt:PersonInImage")
							.e("rdf:Bag")
								.e("rdf:li")
									.t((this.objectData != null)?((this.objectData.getPrimarySubject() != null)?this.objectData.getPrimarySubject():""):"")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end Iptc4xmpExt:PersonInImage
						.e("Iptc4xmpExt:LocationCreated")
							.e("rdf:Bag")
								.e("rdf:li")
									.a("Iptc4xmpExt:Sublocation", "sublocation")
									.a("Iptc4xmpExt:City", "City")
									.a("Iptc4xmpExt:ProvinceState", "State")
									.a("Iptc4xmpExt:CountryName", "Country")
									.a("Iptc4xmpExt:CountryCode", "country iso")
									.a("Iptc4xmpExt:WorldRegion", "world region")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end Iptc4xmpExt:LocationCreated
						.e("Iptc4xmpExt:LocationShown")
							.e("rdf:Bag")
								.e("rdf:li")
									.a("Iptc4xmpExt:Sublocation", "sublocation")
									.a("Iptc4xmpExt:City", "City")
									.a("Iptc4xmpExt:ProvinceState", "State")
									.a("Iptc4xmpExt:CountryName", "Country")
									.a("Iptc4xmpExt:CountryCode", "country iso")
									.a("Iptc4xmpExt:WorldRegion", "world region")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end Iptc4xmpExt:LocationShown
						.e("Iptc4xmpExt:OrganisationInImageName")
							.e("rdf:Bag")
								.e("rdf:li")
									.t("Name of Featured Organization")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end Iptc4xmpExt:OrganisationInImageName
						.e("Iptc4xmpExt:OrganisationInImageCode")
							.e("rdf:Bag")
								.e("rdf:li")
									.t("Code of Featured Organisation")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end Iptc4xmpExt:OrganisationInImageCode
						.e("Iptc4xmpExt:Event")
							.e("rdf:Alt")
								.e("rdf:li")
									.a("xml:lang", "x-default")
									.t("Event")
								.up() //end rdf:li
							.up() //end rdf:Alt
						.up() //end Iptc4xmpExt:Event
						.e("Iptc4xmpExt:ModelAge")
							.e("rdf:Bag")
								.e("rdf:li")
									.t("Model Age")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end Iptc4xmpExt:ModelAge
						.e("Iptc4xmpExt:RegistryId")
							.e("rdf:Bag")
								.e("rdf:li")
									.a("Iptc4xmpExt:RegOrgId", "Registry organisation identifier")
									.a("Iptc4xmpExt:RegItemId", "Registry image identifier")
								.up() //end rdf:li
								.e("rdf:li")
									.a("Iptc4xmpExt:RegOrgId", "Registry organization identifier")
									.a("Iptc4xmpExt:RegItemId", "Registry image identifier")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end Iptc4xmpExt:RegistryId
						.e("Iptc4xmpExt:ArtworkOrObject")
							.e("rdf:Bag")
								.e("rdf:li")
									.e("rdf:Description")
										.a("Iptc4xmpExt:AODateCreated", "2001-01-01T00:00Z")
										.a("Iptc4xmpExt:AOSource", "Artwork Source")
										.a("Iptc4xmpExt:AOSourceInvNo", "Artwork Source Inventory number")
										.a("Iptc4xmpExt:AOCopyrightNotice", "Artwork Copyright notice")
										.e("Iptc4xmpExt:AOTitle")
											.e("rdf:Alt")
												.e("rdf:li")
													.a("xml:lang", "x-default")
													.t("Artwork Title")
												.up() //end rdf.li
											.up() //end rdf.Alt
										.up() //end Iptc4xmpExt:AOTitle
										.e("Iptc4xmpExt:AOCreator")
											.e("rdf:Seq")
												.e("rdf.li")
													.t("ArtworkCreator")
												.up() //end rdf.li
											.up() //end rdf.Seq
										.up() //end Iptc4xmpExt:AOCreator
									.up() //end rdf:Description
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end Iptc4xmpExt:ArtworkOrObject
						.e("plus.ModelReleaseID")
							.e("rdf:Bag")
								.e("rdf:li")
									.t("Model Release Identifier")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end plus.ModelReleaseID
						.e("plus.ImageCreator")
							.e("rdf:Seq")
								.e("rdf.li")
									.a("plus.ImageCreatorName", "Image Creator Name")
									.a("plus.ImageCreatorID", "Image Creator Identifier")
								.up() //end rdf.li
							.up() //end rdf.Seq
						.up() //end plus.ImageCreator
						.e("plus.CopyrightOwner")
							.e("rdf:Seq")
								.e("rdf.li")
									.a("plus.CopyrightOwnerName", "Copyright Owner Name")
									.a("plus.CopyrightOwnerID", "Copyright Owner Identifier")
								.up() //end rdf.li
							.up() //end rdf.Seq
						.up() //end plus.CopyrightOwner
						.e("plus.PropertyReleaseID")
							.e("rdf:Bag")
								.e("rdf:li")
									.t("Property Release ID")
								.up() //end rdf:li
							.up() //end rdf:Bag
						.up() //end plus.PropertyReleaseID
						.e("plus.ImageSupplier")
							.e("rdf:Seq")
								.e("rdf.li")
									.a("plus.ImageSupplierName", "Image Supplier Name")
									.a("plus.ImageSupplierID", "Image Supplier ID")
								.up() //end rdf.li
							.up() //end rdf.Seq
						.up() //end plus.ImageSupplier
					.up()//end rdf:Description
				.up() //end rdf:RDF
			.up(); //end x:xmpmeta
		
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FactoryConfigurationError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return xml;
		
	}
	
	public XMLBuilder getBagMetadataXML() {
		
		XMLBuilder xml = null;
		try {
			xml = XMLBuilder.create("yaleObject");
			
			xml.e("workflow")
					.e("repositoryCode").t((getRepositoryCode() == null)?"":getRepositoryCode()).up()
					.e("repositoryId").t((getName() == null)?"":getName()).up()
					.e("damId").up()
					.e("securityPolicies");
			
			//TODO: SECURITY POLICIES HERE
					
				xml.up()
					.e("action").t((getAction() == null)?"":getAction()).up()
				.up()
				.e("data")
					.a("metadatamodel", (getMetadataModel() == null)?"":getMetadataModel());
				
				if(this.objectData != null) {
						xml.e("culturalObject")
							.e("objectID").t((getObjectData().getObjectID() == null)?"":getObjectData().getObjectID()).up()
							.e("accessionNumber").t((getRenditionNumber() == null)?"":getRenditionNumber()).up()
							//.e("accessionSortNumber").t("PLACEHOLDER").up()
							.e("title").t((getTitle() == null)?"":getTitle()).up()
							//.e("date").t("PLACEHOLDER").up()
							.e("beginYear").t((getObjectData().getBeginningDate() == null)?"":getObjectData().getBeginningDate()).up()
							.e("endYear").t((getObjectData().getEndDate() == null)?"":getObjectData().getEndDate()).up()
							.e("creditLine").t((getObjectData().getCredit() == null)?"":getObjectData().getCredit()).up()
							//.e("objectOwner").t("PLACEHOLDER").up()
							//.e("department").t("PLACEHOLDER").up()
							//.e("classification").t("PLACEHOLDER").up()
							//.e("subclassification").t("PLACEHOLDER").up()
							//.e("sourceClassification").t("PLACEHOLDER").up()
							//.e("culture").t("PLACEHOLDER").up()
							/*.e("medium").t((getMedium() == null)?"":getMedium()).up()
							.e("dimensions").t((getDimensions() == null)?"":getDimensions()).up()
							.e("paperSupport").t((getPaperSupport() == null)?"":getPaperSupport()).up()
							.e("inscriptions").t((getInscribed() == null)?"":getInscribed()).up()
							.e("state").t((getState() == null)?"":getState()).up()
							.e("edition").t((getEdition() == null)?"":getEdition()).up()*/
							//.e("copyrightStatusForWork").t("PLACEHOLDER").up()
							//.e("copyrightBylineForWork").t("PLACEHOLDER").up()
							//.e("restrictionsForTheWork").t("PLACEHOLDER").up()
							//.e("creditLineReproduction").t("PLACEHOLDER").up()
							//.e("keywords").t("PLACEHOLDER").up()
							//.e("language").t("PLACEHOLDER").up()
							//.e("alphaSort").t("PLACEHOLDER").up()
							.e("creators")
								.e("creator")
									.e("name").t((getObjectData().getPrimaryCreator() == null)?"":getObjectData().getPrimaryCreator()).up()
									.e("role").t((getObjectData().getPrimaryCreatorRole() == null)?"":getObjectData().getPrimaryCreatorRole()).up()
									.e("rank").t("1").up()
								.up()
							.up()
							/*.e("alternateNumbers")
								.e("alternateNumber")
									.t("PLACEHOLDER")
								.up()
							.up()
							.e("workTypes")
								.e("workType")
									.t("PLACEHOLDER")
								.up()
							.up()*/
						.up(); //end cultural object
				}
				xml.up() //end data
			.up(); //end yaleObject
				
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FactoryConfigurationError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return xml;
		
	}
	
	private String getMetadataModel() {
		// TODO Auto-generated method stub
		return "TEST MODEL";
	}

	/**
	 * @return the renditionID
	 */
	public String getRenditionID() {
		return renditionID;
	}

	/**
	 * @param renditionID the renditionID to set
	 */
	public void setRenditionID(String renditionID) {
		this.renditionID = renditionID;
	}

	/**
	 * @return the renditionNumber
	 */
	public String getRenditionNumber() {
		return renditionNumber;
	}

	/**
	 * @param renditionNumber the renditionNumber to set
	 */
	public void setRenditionNumber(String renditionNumber) {
		this.renditionNumber = renditionNumber;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}
	/**
	 * @return the masterObjectName
	 */
	public String getMasterObjectName() {
		return masterObjectName;
	}
	/**
	 * @param masterObjectName the masterObjectName to set
	 */
	public void setMasterObjectName(String masterObjectName) {
		this.masterObjectName = masterObjectName;
	}
	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}
	/**
	 * @param title the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}
	/**
	 * @return the workType
	 */
	public String getWorkType() {
		return workType;
	}
	/**
	 * @param workType the workType to set
	 */
	public void setWorkType(String workType) {
		this.workType = workType;
	}
	/**
	 * @return the caption
	 */
	public String getCaption() {
		return caption;
	}
	/**
	 * @param caption the caption to set
	 */
	public void setCaption(String caption) {
		this.caption = caption;
	}
	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}
	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}
	/**
	 * @return the workNotes
	 */
	public String getWorkNotes() {
		return workNotes;
	}
	/**
	 * @param workNotes the workNotes to set
	 */
	public void setWorkNotes(String workNotes) {
		this.workNotes = workNotes;
	}
	/**
	 * @return the rightsSummary
	 */
	public String getRightsSummary() {
		return rightsSummary;
	}
	/**
	 * @param rightsSummary the rightsSummary to set
	 */
	public void setRightsSummary(String rightsSummary) {
		this.rightsSummary = rightsSummary;
	}
	/**
	 * @return the termsRestrictions
	 */
	public String getTermsRestrictions() {
		return termsRestrictions;
	}
	/**
	 * @param termsRestrictions the termsRestrictions to set
	 */
	public void setTermsRestrictions(String termsRestrictions) {
		this.termsRestrictions = termsRestrictions;
	}
	/**
	 * @return the useRestrictions
	 */
	public String getUseRestrictions() {
		return useRestrictions;
	}
	/**
	 * @param useRestrictions the useRestrictions to set
	 */
	public void setUseRestrictions(String useRestrictions) {
		this.useRestrictions = useRestrictions;
	}
	/**
	 * @return the color
	 */
	public String getColor() {
		return color;
	}
	/**
	 * @param color the color to set
	 */
	public void setColor(String color) {
		this.color = color;
	}
	/**
	 * @return the duration
	 */
	public String getDuration() {
		return duration;
	}
	/**
	 * @param duration the duration to set
	 */
	public void setDuration(String duration) {
		this.duration = duration;
	}
	/**
	 * @return the technicalQuality
	 */
	public String getTechnicalQuality() {
		return technicalQuality;
	}
	/**
	 * @param technicalQuality the technicalQuality to set
	 */
	public void setTechnicalQuality(String technicalQuality) {
		this.technicalQuality = technicalQuality;
	}
	/**
	 * @return the bitDepth
	 */
	public String getBitDepth() {
		return bitDepth;
	}
	/**
	 * @param bitDepth the bitDepth to set
	 */
	public void setBitDepth(String bitDepth) {
		this.bitDepth = bitDepth;
	}
	/**
	 * @return the captureDevice
	 */
	public String getCaptureDevice() {
		return captureDevice;
	}
	/**
	 * @param captureDevice the captureDevice to set
	 */
	public void setCaptureDevice(String captureDevice) {
		this.captureDevice = captureDevice;
	}
	/**
	 * @return the digitalItemNotes
	 */
	public String getDigitalItemNotes() {
		return digitalItemNotes;
	}
	/**
	 * @param digitalItemNotes the digitalItemNotes to set
	 */
	public void setDigitalItemNotes(String digitalItemNotes) {
		this.digitalItemNotes = digitalItemNotes;
	}
	/**
	 * @return the mediaFormat
	 */
	public String getMediaFormat() {
		return mediaFormat;
	}
	/**
	 * @param mediaFormat the mediaFormat to set
	 */
	public void setMediaFormat(String mediaFormat) {
		this.mediaFormat = mediaFormat;
	}
	/**
	 * @return the mediaDimensions
	 */
	public String getMediaDimensions() {
		return mediaDimensions;
	}
	/**
	 * @param mediaDimensions the mediaDimensions to set
	 */
	public void setMediaDimensions(String mediaDimensions) {
		this.mediaDimensions = mediaDimensions;
	}
	
	/**
	 * @return the structuralPath
	 */
	public String getStructuralPath() {
		return structuralPath;
	}
	/**
	 * @param structuralPath the structuralPath to set
	 */
	public void setStructuralPath(String structuralPath) {
		this.structuralPath = structuralPath;
	}

	/**
	 * @return the objectData
	 */
	public TMSObject getObjectData() {
		return objectData;
	}

	/**
	 * @param objectData the objectData to set
	 */
	public void setObjectData(TMSObject objectData) {
		this.objectData = objectData;
	}

	/**
	 * @return the repositoryCode
	 */
	public String getRepositoryCode() {
		return repositoryCode;
	}

	/**
	 * @param repositoryCode the repositoryCode to set
	 */
	public void setRepositoryCode(String repositoryCode) {
		this.repositoryCode = repositoryCode;
	}

	/**
	 * @return the action
	 */
	public String getAction() {
		return action;
	}

	/**
	 * @param action the action to set
	 */
	public void setAction(String action) {
		this.action = action;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public boolean isSynced(Connection tmsConn, Logger log) {
		
		boolean retval = false;
		
		String sql = "select count(*) from CDIS where RenditionNumber = '" + this.renditionNumber + "' and UOIID != '-1'";
		ResultSet rs = DataProvider.executeSelect(tmsConn, sql);
		int tempCount = 0;
		
		try {
			if(rs.next()) {
				tempCount = rs.getInt(1);
				if(tempCount > 0) {
					retval = true;
				}
			}
		}
		catch(SQLException sqlex) {
			log.log(Level.ALL, "There was an error in isSync function: {0}", sqlex.getMessage());
			retval = false;
		}
		
		return retval;
	}

	public String getChecksum(Connection tmsConn, Logger log) {
		
		String retval = null;
		
		String sql = "select Checksum from CDIS where RenditionNumber = '" + this.renditionNumber + "'";
		
		ResultSet rs = DataProvider.executeSelect(tmsConn, sql);
		
		try {
			if(rs.next()) {
				retval = rs.getString(1);
			}
		} catch (SQLException e) {
			log.log(Level.ALL, "There was an error retrieving the checksum for rendition {0}: {1}", new Object[]{this.renditionNumber, e.getMessage()});
		}
		
		
		return retval;
	}

	public String getPublicAccess() {
		return publicAccess;
	}

	public void setPublicAccess(String publicAccess) {
		this.publicAccess = publicAccess;
	}

	public String getObjectPublicAccess() {
		return objectPublicAccess;
	}

	public void setObjectPublicAccess(String objectPublicAccess) {
		this.objectPublicAccess = objectPublicAccess;
	}

	public String getFinalPublicUse() {
		
		//check for Object data
		if(this.publicAccess.equals("No")) {
			return "No";
		}
		else if(this.publicAccess.equals("Yes")) {
			//check object
			if(this.objectPublicAccess.equals("Yes")) {
				return "Yes";
			}
			else {
				return "No";
			}
		}
		else {
			return "No";
		}
	}
	
	//THIS NOW FILLS THE SI_ASSET_METADATA.IS_RESTRICTED FIELD IN AN INVERSE CAPACITY
	public String getIsRestricted() {
		
		return (getFinalPublicUse().equals("Yes")?"No":"Yes");
		
	}

	public String getMetadataQuery(String UOIID) {
		
		StringBuffer query = new StringBuffer();
		
		query.append("update SI_ASSET_METADATA set ");
		query.append("SOURCE_SYSTEM_ID = '" + String.valueOf(this.getTitle()) + "' ");

		if(this.getTitle() != null) {
			query.append(", ");
			query.append("TITLE = '" + scrubString(String.valueOf(this.getTitle())) + "' ");
		}
		if(this.getCaption() != null) {
			query.append(", ");
			query.append("KEYWORDS = '" + scrubString(String.valueOf(this.getCaption())) + "' ");
		}

		if(this.getColor() != null) {
			query.append(", ");
			query.append("COLOR = '" + scrubString(String.valueOf(this.getColor())) + "' ");
		}
		if(this.getBitDepth() != null) {
			query.append(", ");
			query.append("BIT_DEPTH = '" + scrubString(String.valueOf(this.getBitDepth())) + "' ");
		}
		if(this.getDuration() != null) {
			query.append(", ");
			query.append("DURATION = '" + scrubString(String.valueOf(this.getDuration())) + "' ");
		}
		if(this.getMediaDimensions() != null) {
			query.append(", ");
			query.append("MEDIA_DIMENSIONS = '" + scrubString(String.valueOf(this.getMediaDimensions())) + "' ");
		}
                //Dec 2014, Structural Path no longer needed in metadata sync per Isabel/Duy
		//if(this.getStructuralPath() != null) {
		//	query.append(", ");
		//	query.append("STRUCTURAL_PATH = '" + scrubString(String.valueOf(this.getStructuralPath())) + "' ");
		//}
		if(this.getWorkNotes() != null) {
			query.append(", ");
			query.append("WORK_NOTES = '" + scrubString(String.valueOf(this.getWorkNotes())) + "' ");
			
		}
		if(this.getIsRestricted() != null) {
			query.append(", ");
			query.append("IS_RESTRICTED = '" + scrubString(String.valueOf(this.getIsRestricted())) + "' ");
		}
		if(this.getTermsRestrictions() != null) {
			query.append(", ");
			query.append("TERMS_AND_RESTRICTIONS = '" + scrubString(String.valueOf(this.getTermsRestrictions())) + "' ");
		}
		if(this.getUseRestrictions() != null) {
			query.append(", ");
			query.append("USE_RESTRICTIONS = '" + scrubString(String.valueOf(this.getUseRestrictions())) + "' ");
		}

		if(this.getDigitalItemNotes() != null) {
			query.append(", ");
			query.append("DIGITAL_ITEM_NOTES = '" + scrubString(String.valueOf(this.getDigitalItemNotes())) + "' ");
			//IMAGE RESTRICTION CODE
			//[Max Image Size = xxx]
			//if string is present, send size to SI_ASSET_METADATA.MAX_IDS_SIZE FIELD
			if(this.getDigitalItemNotes().contains("[MAX IDS SIZE = ")) {
				String sizeString = this.getDigitalItemNotes().substring(this.getDigitalItemNotes().indexOf("["), this.getDigitalItemNotes().indexOf("]")).split("=")[1].trim();
				int size = Integer.parseInt(sizeString);
				if(size >= 0) {
					query.append(", ");
					query.append("MAX_IDS_SIZE = " + size + " ");
				}
			}
		}

		if(this.getCaptureDevice() != null) {
			query.append(", ");
			query.append("CAPTURE_DEVICE = '" + scrubString(String.valueOf(this.getCaptureDevice())) + "' ");
		}
		
		if(this.getObjectData() != null) {
			if(this.getObjectData().getSeriesTitle() != null) {
				query.append(", ");
				query.append("SERIES_TITLE = '" + scrubString(String.valueOf(this.getObjectData().getSeriesTitle())) + "' ");
			}
			if(this.getObjectData().getDescription() != null) {
				query.append(", ");
                                if(this.getObjectData().getDimensions() != null) {
                                    query.append("DESCRIPTION = '" + scrubString(String.valueOf(this.getObjectData().getDescription())) + "' ");
                                }
                                else {
                                    query.append("DESCRIPTION = '" + scrubString(String.valueOf(this.getObjectData().getDescription())) + " " + 
                                                scrubString(String.valueOf(this.getObjectData().getDimensions())) + "' ");
                                }
			}
			if(this.getObjectData().getCredit() != null) {
				query.append(", ");
				query.append("CREDIT = '" + scrubString(String.valueOf(this.getObjectData().getCredit())) + "' ");
			}
			if(this.getObjectData().getBeginningDate() != null) {
				query.append(", ");
				query.append("BEGINNING_DATE = '" + scrubString(String.valueOf(this.getObjectData().getBeginningDate())) + "' ");
			}
			if(this.getObjectData().getEndDate() != null) {
				query.append(", ");
				query.append("ENDING_DATE = '" + scrubString(String.valueOf(this.getObjectData().getEndDate())) + "' ");
			}
			if(this.getObjectData().getLibrarianName() != null) {
				query.append(", ");
				query.append("LIBRARIAN_NAME = '" + scrubString(String.valueOf(this.getObjectData().getLibrarianName())) + "' ");
			}
			if(this.getObjectData().getGroupTitle() != null) {
				query.append(", ");
				query.append("GROUP_TITLE = '" + scrubString(String.valueOf(this.getObjectData().getGroupTitle())) + "' ");
			}
			if(this.getObjectData().getNotes() != null) {
				query.append(", ");
				query.append("NOTES = '" + scrubString(String.valueOf(this.getObjectData().getNotes())) + "' ");
			}
			if(this.getObjectData().getAcquisitionNotes() != null) {
				query.append(", ");
				query.append("ACQUISITION_NOTES = '" + scrubString(String.valueOf(this.getObjectData().getAcquisitionDate())) + "' ");
			}
			if(this.getObjectData().getDimensions() != null) {
				query.append(", ");
				query.append("CONTAINER_DIMENSIONS = '" + scrubString(String.valueOf(this.getObjectData().getDimensions())) + "' ");
			}
			if(this.getObjectData().getPrimaryCreator() != null) {
				query.append(", ");
				query.append("PRIMARY_CREATOR = '" + scrubString(String.valueOf(this.getObjectData().getPrimaryCreator())) + "' ");
			}
			if(this.getObjectData().getPrimarySubject() != null) {
				query.append(", ");
				query.append("PRIMARY_SUBJECT = '" + scrubString(String.valueOf(this.getObjectData().getPrimarySubject())) + "' ");
				//also add additional fields for F|S, for CAPTION field
				query.append(", ");
				query.append("CAPTION = '" + scrubString(String.valueOf(this.getObjectData().getPrimarySubject())));
				if(this.getObjectData().getWorkCreationDate() != null) {
					query.append(", " + scrubString(String.valueOf(this.getObjectData().getWorkCreationDate())));
				}
				if(this.getObjectData().getMedium() != null) {
					query.append(", " + scrubString(String.valueOf(this.getObjectData().getMedium())));
				}
				if(this.getObjectData().getDimensions() != null) {
					query.append(", " + scrubString(String.valueOf(this.getObjectData().getDimensions())));
				}
				if(this.getObjectData().getCredit() != null) {
					query.append(", " + scrubString(String.valueOf(this.getObjectData().getCredit())));
				}
				query.append("' ");
			}
			/*if(this.getObjectData().getPrimarySubjectRole() != null) {
				query.append(", ");
				query.append("PRIMARY_SUBJECT_ROLE = '" + String.valueOf(this.getObjectData().getPrimarySubjectRole()) + "'");
			}*/
			if(this.getObjectData().getWorkCreationDate() != null) {
				query.append(", ");
				query.append("WORK_CREATION_DATE = '" + scrubString(String.valueOf(this.getObjectData().getWorkCreationDate())) + "' ");
			}
			
		}
		query.append(", ");
		query.append("PUBLIC_USE = 'Yes' ");
		
		query.append(" where UOI_ID = '" + UOIID + "'");
		
		return query.toString();
	}
	
	public String scrubString(String input) {
		
		//System.out.println("Original string: " + input);
		String newString = new String();
		
		//remove ampersands - replace with 'and'
		newString = input.replaceAll("&", "and");
		
		//escape any single quotes
		newString = newString.replaceAll("'", "''");
		
		//System.out.println("Scrubbed string: " + newString);
		
		return newString;
	}

}
