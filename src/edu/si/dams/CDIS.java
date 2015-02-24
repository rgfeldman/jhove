/**
 CDIS 2.0 - Common Code Branch
 CDIS.java
 */
package edu.si.dams;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.spi.DateFormatProvider;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import mediaTools.bag.BagException;
import mediaTools.bag.Bagger;
import mediaTools.bag.ChecksumException;
import mediaTools.bag.ChecksumUtils;

import com.artesia.common.utils.DateUtils;
import com.jamesmurty.utils.XMLBuilder;

import edu.si.data.DataProvider;
import edu.si.tms.TMSMediaRendition;
import edu.si.tms.audit.AuditTrailReader;
import edu.si.tms.factory.TMSMediaRenditionFactory;

import CDIS_redesigned.MetaData;
import CDIS_redesigned.CollectionsSystem.ImageFilePath;
import CDIS_redesigned.CDIS_new;

public class CDIS {
	
	public Logger _log = null;
	
	private Properties properties = new Properties();
	
	private static final String[] requiredProps = {"damsDriver",
							"damsUrl",
							"damsUser",
							"damsPass",
							"tmsDriver",
							"tmsUrl",
                                                        "tmsUser",
							"tmsPass",
							"workFolder",
							"hotFolder",
							"mediaDrive",
                                                        "siUnit"
                                                };

	public static void main(String[] args) {
		
		Connection tmsConn, damsConn;
		
		CDIS ingester = new CDIS();
                
		//validate configuration file.
		String configFileName = new String();
                
		if(args.length < 1) {
			System.out.println("Missing parameter: <configFileName>");
			return;
		}
		else {
			configFileName = args[0];
		}
		
		try {
			ingester.loadProperties(configFileName);
		}
		catch(FileNotFoundException fnfe) {
			System.out.println("Config file path invalid.");
			return;
		}
		catch(Exception ex) {
			ex.printStackTrace();
			System.out.println("Exception caught while parsing config file: " + ex.getMessage());
			return;
		}
                
                CDIS_new cdis_new = new CDIS_new();
                
                if (ingester.properties.getProperty("linkRedesigned").equals("true")) {
                    cdis_new.execute("link");
                    return;
                }
                else {
                    System.out.println(ingester.properties.getProperty("linkRedesigned"));
                }
		
                System.out.println("Configuration file verified.");
                
		 // Set up logger
                ingester._log = Logger.getLogger(ingester.getClass().getName());
		ingester._log.setLevel(Level.ALL);
		Handler fh = null;
		DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmm");
		try {
			fh = new FileHandler("log\\CDISLog-" + ingester.properties.getProperty("operationType") + df.format(new Date()) + ".txt");
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			ingester._log.log(Level.SEVERE, "SecurityException in main(): {0}", e.getMessage());
			//e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			ingester._log.log(Level.SEVERE, "IOException in main(): {0}", e.getMessage());
			//e.printStackTrace();
		}
		fh.setFormatter(new SimpleFormatter());
		ingester._log.addHandler(fh);
		
                ingester._log.log(Level.ALL, "Logging Established");
                fh.flush();
		
                // delete old log files
		ingester.deleteOldLogs();
                
		//establish and verify database connections.
		try {
			tmsConn = DataProvider.getConnection(ingester.properties.getProperty("tmsDriver"), 
												ingester.properties.getProperty("tmsUrl"), 
												ingester.properties.getProperty("tmsUser"), 
												ingester.properties.getProperty("tmsPass"));
		}
		catch(Exception ex) {
			ingester._log.log(Level.ALL, "There was a problem connecting to the TMS database: {0}", ex.getMessage());
			//ex.printStackTrace();
			return;
		}
		ingester._log.log(Level.ALL, "Connection to TMS database established.");
		
		
		//establish and verify database connections.
		try {
			damsConn = DataProvider.getConnection(ingester.properties.getProperty("damsDriver"), 
					ingester.properties.getProperty("damsUrl"), 
					ingester.properties.getProperty("damsUser"), 
					ingester.properties.getProperty("damsPass"));
		}
		catch(Exception ex) {
			ingester._log.log(Level.ALL, "There was a problem connecting to the DAMS database: {0}", ex.getMessage());
			//ex.printStackTrace();
			return;
		}
		ingester._log.log(Level.ALL, "Connection to DAMS database established.");
		

                
		ArrayList<String> newRenditions = new ArrayList<String>();
			ArrayList<String> newlySyncedRenditions = new ArrayList<String>();
			
			ArrayList<String> metadataAssets = new ArrayList<String>();
			ArrayList<String> failedMetadata = new ArrayList<String>();
			ArrayList<String> IDSAssets = new ArrayList<String>();
			ArrayList<String> failedIDS = new ArrayList<String>();
			ArrayList<String> ingestedFromDAMS = new ArrayList<String>();
			ArrayList<String> ingestedFromDAMSFailed = new ArrayList<String>();
			
			ArrayList<String> unsyncedRenditions = new ArrayList<String>();
			
			if(ingester.properties.getProperty("operationType").equals("ingest")) {
				
				//if ingestFromDAMS = true, create media records for applicable DAMS assets
                                String objectNumber = null; //for use in common code nov 2014  
                                String objectID = null; //for use in common code nov 2014 
                                String rank = null;
                                
				if(ingester.properties.getProperty("ingestFromDAMS") != null && ingester.properties.getProperty("ingestFromDAMS").equals("true")) {
					//get UOIIDS, UANs for applicable assets
					HashMap<String, String> newAssets = ingester.retrieveNewAssets(damsConn);
					
					ingester._log.log(Level.ALL, "newAssets: {0}", newAssets.size());
					
					//for each
					for(Iterator<String> iter = newAssets.keySet().iterator(); iter.hasNext();) {
                                                ingester._log.log(Level.ALL, "++++++++++++++++++++++++++++++++++++NEXT ASSET+++++++++++++++++++++++++");
						String UOIID = iter.next();
						String UAN = newAssets.get(UOIID);
						
						//pull necessary parameters for media records stored proc
						//pull objectNumber and Rank
						 /*************************************************************/
						//UNCOMMENT THIS FOLLOWING SECTION FOR RENDITION NUMBER INGEST is requested.  
                                                //This was setup was reqiested for CH UNIT ONLY....and perhaps as a temporary request,
                                                // KEEP THIS UNCOMMENTED UNLESS OTHERWISE REQUESTED BY CH UNIT!!!!!
						//get Rendition ID for RenditionNumber
						/*
                                                if (ingester.properties.getProperty("siUnit").equals("CHSDM")) {
                                                
                                                    HashMap<String, String> renditionInfo = ingester.getRenditionInfo(damsConn, tmsConn, UOIID);
						
                                                    if(renditionInfo == null) {
							//NO RENDITION, ABORT, ADD TO FAILED LIST
							ingester._log.log(Level.ALL, "No MediaRendition record found for asset with UAN: " + UAN + ". Skipping...");
							String failedQuery = "update SI_ASSET_METADATA set SOURCE_SYSTEM_ID = 'FOR LATER REVIEW' where UOI_ID = '" + UOIID + "'";
							DataProvider.executeUpdate(damsConn, failedQuery);
							ingestedFromDAMSFailed.add(UAN);
                                                    }
                                                    else {
							//create sync record
							String query = "insert into CDIS(RenditionID, RenditionNumber, UOIID)values('" + renditionInfo.keySet().toArray()[0].toString() + "', '"
									+ renditionInfo.get(renditionInfo.keySet().toArray()[0]) + "', '" + UOIID + "')";
							
							DataProvider.executeInsert(tmsConn, query);
							
							//update MediaInfo for given Rendition
							ingester.syncFilePath(tmsConn, UOIID, UAN);
							
							//update SOURCE_SYSTEM_ID for DAMS asset
							
							query = "update SI_ASSET_METADATA set SOURCE_SYSTEM_ID = '" + renditionInfo.get(renditionInfo.keySet().toArray()[0]) + "' where UOI_ID = '" + UOIID + "'";
							DataProvider.executeUpdate(damsConn, query);
							
							ingestedFromDAMS.add(UAN);
                                                    }
                                                }
                                                */
                                                //end of  RENDITION NUMBER INGEST
                                                /*************************************************************/
                                               
						
						/*************************************************************
						 * COMMENT THE FOLLOWING SECTION WHEN RUNNING RENDITION NUMBER INGEST
						 */
                                                HashMap<String, String> objectInfo;
                                                
                                                if (ingester.properties.getProperty("siUnit").equals("CHSDM")) {
                                                    objectInfo = ingester.getObjectInfoFromBarcode(damsConn, tmsConn, UOIID);
                                                    if(objectInfo != null) {
                                                        objectID = objectInfo.keySet().toArray()[0].toString();
                                                        rank = objectInfo.get(objectID);						}
                                                }
                                                else {
                                                    objectInfo = ingester.getObjectInfoForAsset(damsConn, UOIID);
                                                    if(objectInfo != null) {
							objectNumber = objectInfo.keySet().toArray()[0].toString();
							rank = objectInfo.get(objectNumber);
                                                        objectID = ingester.getObjectIDForAsset(tmsConn, objectNumber);
                                                    }
                                                }
                                                
                                                if(objectInfo != null) {

							ingester._log.log(Level.ALL, "UOIID: {0}", UOIID);
							ingester._log.log(Level.ALL, "UAN: {0}", UAN);
							ingester._log.log(Level.ALL, "IDSPathID: {0}", ingester.properties.getProperty("IDSPathId"));
							ingester._log.log(Level.ALL, "ObjectID: {0}", objectID);
							ingester._log.log(Level.ALL, "Rank: {0}", rank);
							
                                                        // Get the title, this depends on the siUnit
                                                        String title = null;
                                                                                                                  
                                                        if(ingester.properties.getProperty("siUnit").equals("ACM")) {
                                                            title = "acmobj-" + objectNumber.replaceAll("\\.", "") + "-r" + rank;
                                                        }
                                                        else if (ingester.properties.getProperty("siUnit").equals("CHSDM")) {
                                                            title = objectID + "_" + rank;
                                                        }
                                                        else if (ingester.properties.getProperty("siUnit").equals("FSG")) {
                                                            title = objectNumber + "_" + rank;
                                                        }
							else if (ingester.properties.getProperty("siUnit").equals("NMAAHC")) {
                                                            title = objectNumber + "." + rank;
							}
                                                        else {
                                                            ingester._log.log(Level.ALL, "Error, unknown siUnit {0}", ingester.properties.getProperty("siUnit"));
                                                        }
							
                                                       
                                                        ingester._log.log(Level.ALL, "title set to: {0}", title);
                                                        
                                                        HashMap<String, String> dimensions = null;
                                                        String height = null;
                                                        String width = null;
                                                        
							//check if UAN already exists for a MediaFile in TMS
							if(ingester.doesUANExistInTMS(tmsConn, UAN)) {
								//asset file is already being pointed to in TMS, just fix the Rendition Number, add to CDIS table
								boolean result = ingester.replaceTMSRenditionNumber(tmsConn, damsConn, UAN, title, UOIID, objectNumber, objectID);
							
                                                                if(!result) {
									ingestedFromDAMSFailed.add(title);
								}
								else {
									ingestedFromDAMS.add(title);
									ingester._log.log(Level.ALL, "Rendition {0} successfully updated in TMS.", title);
								}
							}
							else {
                                                            boolean MediaCreated;
                                                            if (!ingester.properties.getProperty("siUnit").equals("ACM")) {
                                                                //grab dimension data
                                                                dimensions = ingester.getDimensionData(damsConn, UOIID);
                                                                                                                                  
                                                                    //call media records stored proc
                                                                    //check parameters
                                                                    if(objectID != null) {
									height = dimensions.keySet().toArray()[0].toString();
									width = dimensions.get(height);
									MediaCreated = ingester.createMediaRecords(tmsConn, UOIID, UAN, ingester.properties.getProperty("IDSPathId"), title, objectID, rank, height, width);
									//create BLOB for thumbnail
									if (MediaCreated) {
                                                                            URL assetURL;
                                                                            PreparedStatement stmt = null;
                                                                            BufferedReader input = null;
                                                                            
                                                                            try {
                                                                                
                                                                                    String objectLocation = null;
                                                                                    ResultSet rs = null;
                                                                                    assetURL = null;
                                                                                                                                                                       
                                                                                    try {
                                                                                        String ThumbBlobSql = "select  o.object_name_location from uois u, object_stacks o" +
                                                                                        " where u.uoi_id = '" + UOIID + "'" +
                                                                                        " and u.thumb_nail_obj_id = o.object_id ";
                                
                                                                                        ingester._log.log (Level.ALL, "ThumbBlobSql: {0}", ThumbBlobSql);
		
                                                                                         stmt = damsConn.prepareStatement(ThumbBlobSql);
                                                                                         rs = stmt.executeQuery();
                              
                                                                                        while(rs.next()) {
                                                                                            objectLocation = rs.getString(1);
                                                                                        }
                                                                                        
                                                                                        assetURL = new URL("file:///T:\\" + objectLocation );
                                                                                        
                                                                                        ingester._log.log(Level.ALL, "Object found at {0}", objectLocation);
                                                                                        ingester._log.log(Level.ALL, "URL {0}", assetURL);
                                                                                        
                                                                                    } catch (SQLException e) {
                                                                                    // TODO Auto-generated catch block
                                                                                        ingester._log.log(Level.ALL, "SQLException when getting location name for UOIID: {0}, getting from IDS", UAN); 
                                                                                    }
                                                                                    finally  {
                                                                                       try { if (rs != null) rs.close(); } catch (SQLException se) { ingester._log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                                                                                    }
                                                                                    
                                                                                    ingester._log.log(Level.ALL, "Object at {0}", objectLocation);
                                                                                
                                                                                    URLConnection connection = assetURL.openConnection();
										
                                                                                    input = new BufferedReader(new InputStreamReader(connection.getInputStream()));
										
                                                                                    stmt = tmsConn.prepareStatement("update MediaRenditions set ThumbBLOB = ?, ThumbBlobSize = ? where RenditionID = (select RenditionID from CDIS where UOIID = ?)");
										
                                                                                    stmt.setBinaryStream(1, connection.getInputStream(), (int)connection.getContentLength());
                                                                                    stmt.setInt(2, (int)connection.getContentLength());
                                                                                    stmt.setString(3, UOIID);
										
                                                                                    DataProvider.executeUpdate(tmsConn, stmt);
                                                                                   
                                                                            } catch (MalformedURLException e) {
										// TODO Auto-generated catch block
										ingester._log.log(Level.ALL, "There was a problem retrieving asset with UAN {0} from IDS. No thumbnail will be saved in the database for this asset.", UAN);
                                                                            } catch (IOException e) {
										// TODO Auto-generated catch block
										ingester._log.log(Level.ALL, "IOException when creating thumbnail for asset {0}. TMS Thumbnail not created.", UAN);
                                                                                e.printStackTrace();    
                                                                            } catch (SQLException e) {
										// TODO Auto-generated catch block
										ingester._log.log(Level.ALL, "SQLException when updating thumbnail for asset {0}. Skipping...", UAN);        
                                                                            }
                                                                            finally {
                                                                                try { if (stmt != null) stmt.close(); } catch (SQLException se) { ingester._log.log(Level.ALL, "Error closing the statement " + se.getMessage()); }
                                                                                try { if (input != null) input.close(); } catch (IOException io) { ingester._log.log(Level.ALL, "Error closing the statement " + io.getMessage()); }  
                                                                            }
                                                                                                                                                  
                                                                            //update DAMS.SOURCE_SYSTEM_IDENTIFIER with rendition number
                                                                            ingester.updateDAMSAsset(damsConn, UOIID, title);
                                                                            ingester._log.log(Level.ALL, "Updated: {0}", title);
                                                                            ingestedFromDAMS.add(title);
                                                                    
                                                                        }
                                                                        else {
                                                                            ingester._log.log(Level.ALL, "Error when creating TMS record for {0}, Adding to error report", title);
                                                                            ingestedFromDAMSFailed.add(title);
                                                                        }
                                                                    }
                                                                    else {
									//no Object found for asset in TMS
									ingester._log.log(Level.ALL, "No Object found in TMS for asset {0}. Skipping and adding to error report...", title);
									ingestedFromDAMSFailed.add(title);
                                                                        
                                                                    }
                                                            }
                                                            else  {   // ACM
                                                                if(objectID != null) {
                                                                        ingester._log.log(Level.ALL,"Creating MediaRecords");
                                                                        
                                                                        MediaCreated = ingester.createMediaRecords(tmsConn, UOIID, UAN, ingester.properties.getProperty("IDSPathId"), title, objectID, rank, height, width);
									PreparedStatement stmt = null;
                                                                        
                                                                        if (MediaCreated) {
                                                                            //create BLOB for thumbnail
                                                                            URL assetURL;
                                                                            try {
										assetURL = new URL("http://ids-internal.si.edu/ids/deliveryService/id/" + UAN + "/192");
										InputStream UANStream = assetURL.openStream();
										byte[] bytes = IOUtils.toByteArray(UANStream);
										
                                                                            	stmt = tmsConn.prepareStatement("update MediaRenditions set ThumbBLOB = ? where RenditionID = (select RenditionID from CDIS where UOIID = ?)");
                                                                                
                                                                                stmt.setBytes(1, bytes);                                                     
										stmt.setString(2, UOIID);
                                                                                
                                                                                //ingester._log.log(Level.ALL,"SQL: {0}", stmt);
										
										DataProvider.executeUpdate(tmsConn, stmt);
										
										UANStream.close();
                                                                            } catch (MalformedURLException e) {
										// TODO Auto-generated catch block
										ingester._log.log(Level.ALL, "There was a problem retrieving asset with UAN {0} from IDS. No thumbnail will be saved in the database for this asset.", UAN);
                                                                            } catch (IOException e) {
										// TODO Auto-generated catch block
										ingester._log.log(Level.ALL, "IOException when creating thumbnail for asset {0}. Skipping...", UAN);
                                                                                e.printStackTrace();
                                                                            } catch (SQLException e) {
										// TODO Auto-generated catch block
										ingester._log.log(Level.ALL, "SQLException when updating thumbnail for asset {0}. Skipping...", UAN);
                                                                            }
                                                                            finally {
                                                                                try { if (stmt != null) stmt.close(); } catch (SQLException se) { ingester._log.log(Level.ALL, "Error closing the statement " + se.getMessage()); }                                                                               
                                                                            }  
									
                                                                            //update DAMS.SOURCE_SYSTEM_IDENTIFIER with rendition number
                                                                            ingester.updateDAMSAsset(damsConn, UOIID, title);
                                                                            ingester._log.log(Level.ALL, "Updated: {0}", title);
                                                                            ingestedFromDAMS.add(title);
                                                                        }
                                                                        else {
                                                                            ingester._log.log(Level.ALL, "Error when creating TMS record for {0}, Adding to error report", title);
                                                                            ingestedFromDAMSFailed.add(title);
                                                                        }
								}
                                                                else {
                                                                    ingester._log.log(Level.ALL, "No Object found in TMS for asset {0}. Skipping and adding to error report...", title);
                                                                    ingestedFromDAMSFailed.add(title);
                                                                }    
								
                                                            }
                                                        }
						}
						/*********************************************************************
						 * END OF NON RENDITION NUMBER SECTION for CHSDM
						 */
                                                else {
                                                    ingester._log.log(Level.ALL, "Unable to obtain objectinfo for uoiid: {0}", UOIID);
                                                    ingestedFromDAMSFailed.add(UOIID);
                                                }
						
					}
					ingester._log.log(Level.ALL, "Finished iterating.");
					
					
				}
			
				if(ingester.properties.getProperty("ingestFromTMS").equals("true")) {
					//pull recently created TMS records.
					newRenditions = ingester.retrieveNewRenditions(tmsConn);
				
					//pull CDIS records with no sync - failed transfers
					newRenditions.addAll(ingester.retrieveFailedTransfers(tmsConn));
					//get newly flagged TMS records, check for duplicates with newly created records
					for(Iterator<String> iter = ingester.getRecentlyFlaggedRenditions(tmsConn).iterator(); iter.hasNext();) {
						String tempString = iter.next();
						if(!newRenditions.contains(tempString)) {
							newRenditions.add(tempString);
						}
					}
				}
				
				//check for any other derivatives for these files
				/*for(Iterator<String> iter = newRenditions.iterator(); iter.hasNext();) {
					
				}*/
				//newRenditions.addAll(ingester.getRecentlyFlaggedRenditions(tmsConn));
				ingester._log.log(Level.ALL, "Number of new renditions to migrate: {0}", newRenditions.size());
				if(newRenditions.isEmpty()) {
					ingester._log.log(Level.ALL, "Creating log entry for {0} operation...", ingester.properties.getProperty("operationType"));
					boolean success = ingester.createLogEntry(tmsConn, ingester.properties.getProperty("operationType"));
					
					if(!success) {
                                            ingester._log.log(Level.ALL, "There was an error creating the log entry.");
                                        }
					//ingester._log.log(Level.ALL, "Exiting...");
					//return;
				}
			}
			else if(ingester.properties.getProperty("operationType").equals("sync")) { //operationType=sync
                        
                            
			
                                //find renditions not in CDIS, IsColor = 1, asset in DAMS with <rendition number>.tif name
				//
				/* USED on units where ingestFromDams = True (CH and FSG).... REMOVED UNTIL AFTER RCPP
                                
                                if (!ingester.properties.getProperty("siUnit").equals("ACM")) {
				
                                    ArrayList<String> renditionsInDAMS = ingester.getAssetsAlreadyInDAMS(damsConn, tmsConn);
				 
				
                                    if(!renditionsInDAMS.isEmpty()) {
					//build query for DAMS, find assets with name <rendition number>.tif
					String DAMSQuery = "select a.UOI_ID, b.OWNING_UNIT_UNIQUE_NAME, a.NAME from UOIS a, SI_ASSET_METADATA b " +
									"WHERE a.UOI_ID = b.UOI_ID " +
									"AND TRIM(UPPER(a.CONTENT_STATE)) = 'NORMAL' " +
									"AND TRIM(UPPER(a.CONTENT_TYPE)) != 'SHORTCUT' " +
									"AND b.SOURCE_SYSTEM_ID is null " +
									"AND a.NAME in (";
					for(Iterator<String> iter = renditionsInDAMS.iterator(); iter.hasNext();) {
						String renditionNumber = iter.next();
						DAMSQuery += "'" + renditionNumber + ".tif'";
						if(iter.hasNext()) {
							DAMSQuery += ", ";
						}
						else {
							DAMSQuery += ")";
						}
					}
					
					//System.out.println("DAMSQuery: " + DAMSQuery);
					
					//grab UOIIDS and UANS
					ResultSet rs = DataProvider.executeSelect(damsConn, DAMSQuery);
					HashMap<String, String> renditionUOIIDs = new HashMap<String, String>();
					HashMap<String, String> renditionUANs = new HashMap<String, String>();
					try {
						while(rs.next()) {
							String assetName = rs.getString(3);
							String renditionNumber = assetName.replace(".tif", "");
							String UOIID = rs.getString(1);
							String UAN = rs.getString(2);
							renditionUOIIDs.put(renditionNumber, UOIID);
							renditionUANs.put(renditionNumber, UAN);
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						ingester._log.log(Level.ALL, "There was an exception retrieving UOIIDS and UANS for sync. Skipping...");
					}
					finally {
						try { if (rs != null) rs.close(); } catch (SQLException se) { ingester._log.log(Level.ALL, "Error closing the statement " + se.getMessage()); }
					}
					
					for(Iterator<String> renditionIter = renditionUOIIDs.keySet().iterator(); renditionIter.hasNext();) {
						
						String renditionNumber = renditionIter.next();
						//create CDIS record
						String insertQuery = "insert into CDIS(RenditionID, RenditionNumber, UOIID)values(" +
								"(select RenditionID from MediaRenditions where RenditionNumber = '" + renditionNumber + "'), '" + renditionNumber + "', '" + renditionUOIIDs.get(renditionNumber) + "')";
						
						//System.out.println("InsertQuery: " + insertQuery);
						
						boolean success = DataProvider.executeInsert(tmsConn, insertQuery);
						
						if(success) {
							//replace file path
							ingester.syncFilePath(tmsConn, renditionUOIIDs.get(renditionNumber), renditionUANs.get(renditionNumber)); 
							
							//update DAMS Asset SOURCE_SYSTEM_IDENTIFER with RenditionNumber
							String updateQuery = "update SI_ASSET_METADATA set SOURCE_SYSTEM_ID = '" + renditionNumber + "' where UOI_ID = '" + renditionUOIIDs.get(renditionNumber) + "'";
							
							//System.out.println("UpdateQuery: " + updateQuery);
							
							DataProvider.executeUpdate(damsConn, updateQuery);
							
							IDSAssets.add(renditionNumber);
						}
						else {
							ingester._log.log(Level.ALL, "There was an error syncing IDS derivative for rendition " + renditionNumber + ". Skipping...");
							failedIDS.add(renditionNumber);
						}
                                                
					
					}
                                    }    
                                }*/
				
				
				
				//find un-synced renditions
				//unsyncedRenditions = ingester.retrieveUnsyncedRenditions(tmsConn);
				
				//if(!unsyncedRenditions.isEmpty()) {
				//	for(Iterator<String> iter = unsyncedRenditions.iterator(); iter.hasNext();) {
				//		boolean result = ingester.unsyncRendition(iter.next(), tmsConn, damsConn);
				//	}
				//}
				
				//find renditions with primary file change
				/*ArrayList<String> newPrimaryRenditions = ingester.retrievePrimaryFileChanges(tmsConn);
				
				if(!newPrimaryRenditions.isEmpty()) {
					for(Iterator<String> iter = newPrimaryRenditions.iterator(); iter.hasNext();) {
						String rendition = iter.next();
						boolean result = ingester.unsyncRendition(rendition, tmsConn, damsConn);
						unsyncedRenditions.add(rendition);
						//these renditions will be picked up for re-sync next time the ingest service runs
					}
				}*/
				
				//find renditions ready for IDS sync
				//list of UOIIDS requiring IDS file path sync
				ArrayList<String> renditionsRequiringIDSPath = ingester.renditionsForIDSSync(tmsConn);
				//ingester._log.log(Level.ALL, "Renditions requiring IDS Path: " + renditionsRequiringIDSPath.size());
				
                                if(!ingester.properties.getProperty("syncRedesigned").equals("true")) {
				if(!renditionsRequiringIDSPath.isEmpty()) {
					//find UOIID and UAN mapping for matching assets
				
                                        HashMap<String, String> UOIIDSandUANS = ingester.getUANPairs(damsConn, tmsConn, renditionsRequiringIDSPath);
                                        ingester._log.log(Level.ALL, "UOIID/UAN Pairs: " + UOIIDSandUANS.size());
					
					for(Iterator<String> iter = UOIIDSandUANS.keySet().iterator(); iter.hasNext();) {
						String UOIID = iter.next();
						String UAN = UOIIDSandUANS.get(UOIID);
						boolean result = ingester.syncFilePath(tmsConn, UOIID, UAN);
						if(result) {
							ingester._log.log(Level.ALL, "Asset {0} successfully linked with IDS derivative.", UOIID);
							IDSAssets.add(UOIID);
						}
						else {
							ingester._log.log(Level.ALL, "There was an error linking asset {0} with IDS derivative. Skipping...", UOIID);
							failedIDS.add(UOIID);
						}
					}
				}
                                }			
				
				//retrieve UOIIDs of newly ingested objects
				ArrayList<String> fileNames = ingester.retrieveRenditionsPendingSync(tmsConn);
				
				if(fileNames != null && fileNames.size() > 0) {
					HashMap<String, String> syncPairs = ingester.retrieveUOIIDSForSync(damsConn, fileNames);
					
					if(syncPairs != null && !syncPairs.isEmpty()) {
						//update CDIS with new UOIIDs
						boolean result = ingester.syncNewRecords(tmsConn, damsConn, syncPairs);
						
						//add renditionIDs to newlySyncedRenditions
						newlySyncedRenditions = ingester.getRenditionsForFileNames(tmsConn, syncPairs);
					}
				}
				else {
					ingester._log.log(Level.ALL, "No new assets to sync UOIIDs with DAMS. Continuing...");
				}
	
				
				//pull recently updated TMS records.
				newRenditions = ingester.retrieveUpdatedRenditions(tmsConn);
				
				//add newly synced renditions
				if(newlySyncedRenditions != null && !newlySyncedRenditions.isEmpty()) {
					for(Iterator<String> iter = newlySyncedRenditions.iterator(); iter.hasNext();) {
						String tempRendition = iter.next();
						if(!newRenditions.contains(tempRendition)) {
							newRenditions.add(tempRendition);
						}
					}
					
				}
                                
                                if(ingester.properties.getProperty("syncRedesigned").equals("true")) {
                                    
                                    MetaData mda = new MetaData();
                                    mda.sync(tmsConn, damsConn, ingester.properties, ingester._log);
                                    
                                }
                                         
				//pull newly created assets ingested from DAMS
				//EnteredDate >= LastRan, PathID = IDSPathId, IsColor = 1
				ArrayList<String> DAMSIngestedAssets = ingester.getAssetsIngestedfromDAMS(tmsConn);
				
				if(DAMSIngestedAssets != null && !DAMSIngestedAssets.isEmpty()) {
					for(Iterator<String> iter = DAMSIngestedAssets.iterator(); iter.hasNext();) {
						String tempRendition = iter.next();
						if(!newRenditions.contains(tempRendition)) {
							newRenditions.add(tempRendition);
						}
					}
					
				}
				
				
				
				ingester._log.log(Level.ALL, "Number of new renditions to migrate: {0}", newRenditions.size());
				if(newRenditions.isEmpty()) {
					ingester._log.log(Level.ALL, "Creating log entry for {0} operation...", ingester.properties.getProperty("operationType"));
					boolean success = ingester.createLogEntry(tmsConn, ingester.properties.getProperty("operationType"));
					
					if(!success) {
                                            ingester._log.log(Level.ALL, "There was an error creating the log entry.");
                                        }
					ingester._log.log(Level.ALL, "Exiting...");
				}
			}
                        
			
                        
                            
                        
			//populate data objects from TMS data.
			ArrayList<TMSMediaRendition> renditionObjects = ingester.loadTMSData(tmsConn, newRenditions, ingester.properties.getProperty("operationType"));
			ingester._log.log(Level.ALL, "Populated data objects from TMS data.");
				
			
			File hotFolder = new File(ingester.properties.getProperty("hotFolder"));
			File workFolder = new File(ingester.properties.getProperty("workFolder"));
			ArrayList<String> successAssets = new ArrayList<String>();
			ArrayList<String> failedAssets = new ArrayList<String>();
			HoleyBagData item;
			String bagName;
			
			try {
				for(Iterator<TMSMediaRendition> iter = renditionObjects.iterator(); iter.hasNext();) {
					TMSMediaRendition tempRendition = iter.next();
					
					String fileName = new String();
					if(tempRendition.getName().contains("\\")) {
						fileName = tempRendition.getName().split("\\\\")[tempRendition.getName().split("\\\\").length-1];
					}
					else {
						fileName = tempRendition.getName();
					}
						////system.out.println("Filename: " + fileName);
					XMLBuilder xml = tempRendition.getMetadataXMP();
						
					String filePath = tempRendition.getStructuralPath() + "\\" + tempRendition.getName();
						
                                        
					if(ingester.properties.getProperty("operationType").equals("ingest")) {
                                            if(ingester.properties.getProperty("ingestFromTMS").equals("true")) { 
						//copy file to hotfolder
						boolean result = ingester.placeInHotFolder(hotFolder, tempRendition, xml, workFolder);
						if(result) {
							ingester._log.log(Level.ALL, "Asset {0} successfully moved to hotfolder at location {1}", new Object[]{tempRendition.getFileName(), hotFolder.getAbsolutePath()});
							successAssets.add(tempRendition.getFileName());
						}
						else {
							ingester._log.log(Level.ALL, "Error occurred while moving asset {0}. Skipping...", tempRendition.getFileName());
							failedAssets.add(tempRendition.getFileName());
						}
							
						//check if asset has a TIFF associated with it
						if(!tempRendition.getName().endsWith("tif")) {
							String tiffPath = ingester.convertMediaPath(tempRendition.getStructuralPath()) + "\\" + tempRendition.getName().replace("jpg", "tif");
							File tiff = new File(tiffPath);
							if(tiff.exists()) {
								//high resolution file available
								//copy file to hotfolder
								//move asset file to workfolder
                                                                Scanner scanner;
                                                                File destFile = new File(workFolder.getAbsolutePath() + "\\" + tempRendition.getName().replace("jpg", "tif"));
                                                                //System.out.println("Copying from " + tiffPath);
                                                                ingester._log.log(Level.ALL, "Beginning file copy to work folder...");
                                                                ingester._log.log(Level.ALL, "Source file size: {0}", tiff.length());
                                                                boolean isCopying = true;
                                                            try {
							    	FileUtils.copyFile(tiff, destFile);
								    
								    ingester._log.log(Level.ALL, "File copy to work folder complete.");
								    ingester._log.log(Level.ALL, "Beginning file copy to hot folder...");
								    //if successful, copy both files to hotfolder
								    
									FileUtils.copyFile(destFile, new File(hotFolder.getAbsolutePath() + "\\" + "MASTER" + "\\" + tempRendition.getName().replace("jpg", "tif")));
                                                            } catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
                                                            }
							ingester._log.log(Level.ALL, "File copy to hot folder complete.");
									
							}
						}
                                            }
                                        }
                                        
                                        
                                        
					if(ingester.properties.getProperty("operationType").equals("sync")) {
                                            if(! ingester.properties.getProperty("syncRedesigned").equals("true")) {
                                                 
						//grab UOIID for rendition
						String UOIID = ingester.getUOIIDForRendition(tmsConn, tempRendition);
						if(UOIID != null) {
							// make metadata updates for changed renditions
							boolean result = ingester.updateMetadata(damsConn, tempRendition, UOIID);
								
							if(result) {
								ingester._log.log(Level.ALL, "Successfully synced metadata changes for rendition {0}", tempRendition.getName());
								metadataAssets.add(tempRendition.getName());
							}
							else {
								ingester._log.log(Level.ALL, "There was an error syncing metadata changes for rendition {0}. Skipping...", tempRendition.getName());
								failedMetadata.add(tempRendition.getName());
							}
							
							
						}
                                            }
                                        }					
	
                                    ingester._log.log(Level.ALL, "Creating updated record for rendition: {0}", tempRendition.getRenditionNumber());
                                    boolean success = ingester.createIngestRecord(tmsConn, tempRendition);
			        
                                    if(!success) {
			        	ingester._log.log(Level.ALL, "There was an error creating the ingest record for rendition: {0}", tempRendition.getRenditionNumber());
                                    }
	
			       
					
				}
				
				ingester._log.log(Level.ALL, "Creating email report for {0} operation...", ingester.properties.getProperty("operationType"));
				boolean emailResult = false;
				if(ingester.properties.getProperty("operationType").equals("ingest")) {
					if(!successAssets.isEmpty() || 
								!failedAssets.isEmpty() || !ingestedFromDAMS.isEmpty() || !ingestedFromDAMSFailed.isEmpty()) {
						//create ready.txt in hotfolder
						File readyFile = new File(hotFolder.getAbsolutePath() + "\\" + "MASTER" + "\\ready.txt");
						try {
							readyFile.createNewFile();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							ingester._log.log(Level.ALL, "An error occurred when creating the ready.txt file. Ingestion will occur the next time the ingest process is run nightly.");
						}
						emailResult = ingester.generateIngestReport(successAssets, failedAssets, ingestedFromDAMS, ingestedFromDAMSFailed);
					}
				}
				else if(ingester.properties.getProperty("operationType").equals("sync")) {
					if(!metadataAssets.isEmpty() || 
							!failedMetadata.isEmpty() || 
							!IDSAssets.isEmpty() ||
							!failedIDS.isEmpty() ||
							!unsyncedRenditions.isEmpty()) {
						emailResult = ingester.generateSyncReport(metadataAssets, IDSAssets, failedMetadata, failedIDS, unsyncedRenditions);
					}
				}
		
                                ingester._log.log(Level.ALL, "Creating log entry for {0} operation...", ingester.properties.getProperty("operationType"));
                                boolean success = ingester.createLogEntry(tmsConn, ingester.properties.getProperty("operationType"));
			
                                if(!success) {
                                    ingester._log.log(Level.ALL, "There was an error creating the log entry.");
                                }

		
                                tmsConn.close();
                                damsConn.close();
                        }
                        catch(SQLException sqlex) {
                            ingester._log.log(Level.ALL, "Exception in main(): {0}", sqlex.getMessage());
                            //sqlex.printStackTrace();
                        }
                        finally {
                              try { if (damsConn != null) damsConn.close(); } catch (SQLException se) { ingester._log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                              try { if (tmsConn != null) tmsConn.close(); } catch (SQLException se) { ingester._log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                                 
                        }
                        
	}

	private HashMap<String, String> getRenditionInfo(Connection damsConn, Connection tmsConn,
			String UOIID) {
		
		String fileName = null;
		String renditionID = null;
		HashMap<String, String> retval = null;
		
		String damsQuery = "select NAME from UOIS where UOI_ID = '" + UOIID + "'";
		
                _log.log(Level.ALL, "SQL getRenditionInfo: {0}", damsQuery );
                
                PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = damsConn.prepareStatement(damsQuery);
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				fileName = rs.getString(1);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		//have the filename
		if(fileName != null) {
			//strip off .tif
			fileName = fileName.replace(".tif", "");
                        fileName = fileName.replace(".jpg", "");
			
			//get RenditionID of MediaRendition with matching RenditionNumber
			String tmsQuery = "select RenditionID from MediaRenditions where RenditionNumber = '" + fileName + "'";
			
			try {
				stmt = tmsConn.prepareStatement(tmsQuery);
				rs = stmt.executeQuery();
				
				while(rs.next()) {
					renditionID = rs.getString(1);
				}
				if(renditionID != null) {
					retval = new HashMap<String, String>();
					retval.put(renditionID, fileName);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
                        finally {
                             try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                              try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                        }
		}
		
		
		
		return retval;
	}

	private ArrayList<String> getAssetsAlreadyInDAMS(Connection damsConn,
			Connection tmsConn) {
		
		//get TMS Media Renditions where IsColor = 1, not found in CDIS table
		String tmsQuery = "select top 5 RenditionNumber from MediaRenditions where IsColor = 1 and RenditionID not in (select RenditionID from CDIS) order by RenditionID asc";
		
		ResultSet rs = DataProvider.executeSelect(tmsConn, tmsQuery);
		
		ArrayList<String> renditionNumbers = new ArrayList<String>();
		
		try {
			while(rs.next()) {
				renditionNumbers.add(rs.getString(1));
			}
		} catch (SQLException e) {
			_log.log(Level.ALL, "There was an exception during getAssetsAlreadyInDAMS. Skipping...");
		}
		finally {
			try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
		}
		
		return renditionNumbers;
	}

	private HashMap<String, String> getDimensionData(Connection damsConn,
			String UOIID) {
			
                String query = "select BITMAP_HEIGHT, BITMAP_WIDTH from UOIS where UOI_ID = '" + UOIID + "'";
		HashMap<String, String> retval = new HashMap<String, String>();
		
                PreparedStatement stmt = null;
                ResultSet rs = null;
                
                try {
                    stmt = damsConn.prepareStatement(query);
                    rs = stmt.executeQuery();

                    while(rs.next()) {
			retval.put(rs.getString(1), rs.getString(2));
                    }
		
                } catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "There was an exception during getDimensionData, asset with UOI_ID: {0}. Skipping...", UOIID);
		}
		finally {
			try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                        try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
		}
		
		return retval;
	}

	private boolean replaceTMSRenditionNumber(Connection tmsConn, Connection damsConn, String UAN,
			String title, String UOIID, String objectNumber, String objectID) {
		
		//make sure FileName is unique
		String uniqueQuery = null;
                
                //grab Rendition information from TMS based on the UAN.  for FSG, we need to strip the directory name off
                if (properties.getProperty("siUnit").equals("FSG")) {
                     uniqueQuery = "select count(*) from MediaFiles " +
                            "where REPLACE(REPLACE(SUBSTRING(FileName,PATINDEX('%\\%',FileName)+1,LEN(FileName)),'.jpg',''),'.tif','') = '" + UAN + "'";
                } 
                else {
                    uniqueQuery = "select count(*) from MediaFiles where FileName = '" + UAN + "'";
                }
                
                _log.log(Level.ALL, "SQL: Count from MediaFiles {0}", uniqueQuery);
                
		ResultSet rs = DataProvider.executeSelect(tmsConn, uniqueQuery);
		
                int rs_count = 0;
                
		try {
			while(rs.next()) {
                            rs_count = rs.getInt(1);
			}
		} catch (SQLException e) {
			_log.log(Level.ALL, "SQLException in replaceTMSRenditionNumber: {0}", e.getMessage());
			return false;
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
                
                if (properties.getProperty("siUnit").equals("CHSDM")) {
                    //Next line commented out Nov 2014, added jpg line also
                    //title = title.replaceAll("_", ".");
                    title = title.replaceAll(".tif", "");
                    title = title.replaceAll(".jpg", "");
                }
                
		if(rs_count == 1) {
                        String query;
			//query = "update MediaRenditions set RenditionNumber = '" + title + "', IsColor = 1 where " +
			//		"RenditionID = (select RenditionID from MediaFiles where FileName = '" + UAN + "')";
			
			//_log.log(Level.ALL, "replaceTMSRenditionNumber query: " + query);
			
			//DataProvider.executeUpdate(tmsConn, query);
                        
                    
			//create CDIS record
			//grab Rendition information based on the UAN.  for FSG, we need to strip the directory name off
			
                        if (properties.getProperty("siUnit").equals("FSG")) {
                            query = "selecT RenditionID, RenditionNumber from MediaRenditions where RenditionID = " +
                                    "(select RenditionID from MediaFiles where REPLACE(REPLACE(SUBSTRING(FileName,PATINDEX('%\\%',FileName)+1,LEN(FileName)),'.jpg',''),'.tif','') = '" + UAN  + "')";
                        }
                        else {
                            query = "select RenditionID, RenditionNumber from MediaRenditions where RenditionID = " +
					"(select RenditionID from MediaFiles where FileName = '" + UAN + "')";
                        }
                        
                        _log.log(Level.ALL, "query: " + query);
			
			rs = DataProvider.executeSelect(tmsConn, query);
			int renditionID = 0;
			String renditionNumber = new String();
			
			try {
				while(rs.next()) {
					renditionID = rs.getInt(1);
					renditionNumber = rs.getString(2);
				}
				
			} catch (SQLException e) {
				_log.log(Level.ALL, "SQLException caught in replaceTMSRenditionNumber: {0}", e.getMessage());
				return false;
			}
                        finally {
                             try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the resultSet {0}", se.getMessage()); }
                        }
			
			query = "insert into CDIS " +
                                "(RenditionID, RenditionNumber, objectID, UOIID)values(" +
				renditionID + "," +
				"'" + renditionNumber + "'," + 
                                "'" + objectID + "'," + 
                        	"'" + UOIID + "')";
                        
                        _log.log(Level.ALL,query);

			DataProvider.executeInsert(tmsConn, query);
			
			//update SOURCE_SYSTEM_ID in DAMS.SI_ASSET_METADATA
                        if (objectNumber!=null) {
                            query = "update SI_ASSET_METADATA set SOURCE_SYSTEM_ID = '" + objectNumber + "' where UOI_ID = '" + UOIID + "'";
                        }
                        else {
                            query = "update SI_ASSET_METADATA set SOURCE_SYSTEM_ID = '" + renditionNumber + "' where UOI_ID = '" + UOIID + "'";
                        }
                        _log.log(Level.ALL,query);
			
			DataProvider.executeUpdate(damsConn, query);
			
                        // This next code was only in CH unit line of code
                        if (properties.getProperty("siUnit").equals("CHSDM")) {
                            //create BLOB for thumbnail
                            URL assetURL;
                            BufferedReader input = null;
                            PreparedStatement stmt = null;
                            
                            try {
                                
                                String ThumbBlobSql = "select  o.object_name_location from uois u, object_stacks o" +
                                                      " where u.uoi_id = '" + UOIID + "'" +
                                                      " and u.thumb_nail_obj_id = o.object_id ";
                                
                                stmt = damsConn.prepareStatement(ThumbBlobSql);
                                rs = stmt.executeQuery();
                                
                                String objectLocation = null;
                                
                                try {
                                    while(rs.next()) {
					objectLocation = rs.getString(1);
                                    }
				
                                } catch (SQLException e) {
                                    _log.log(Level.ALL, "SQLException getting objectLocation from dams: {0}", e.getMessage());
                                    return false;
                                }
                                finally {
                                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the result set {0}", se.getMessage()); }
                                }
                                 _log.log(Level.ALL, "ObjectLocation: {0}", objectLocation );
                                 
                                assetURL = new URL("file:///T:\\" + objectLocation );                            
                                                                                            				
				URLConnection connection = assetURL.openConnection();
				
				input = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				
				stmt = tmsConn.prepareStatement("update MediaRenditions set ThumbBLOB = ?, ThumbBlobSize = ? where RenditionID = (select distinct RenditionID from CDIS where UOIID = ?)");
				                                                              
				stmt.setBinaryStream(1, connection.getInputStream(), (int)connection.getContentLength());
				stmt.setInt(2, (int)connection.getContentLength());
				stmt.setString(3, UOIID);
				
                                _log.log(Level.ALL,"Update MediaRenditions with UOIID: {0}", UOIID);
                                
				DataProvider.executeUpdate(tmsConn, stmt);
                                
                                				
                            } catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				this._log.log(Level.ALL, "There was a problem retrieving asset with UAN {0} from IDS. No thumbnail will be saved in the database for this asset.", UAN);
                            } catch (IOException e) {
				// TODO Auto-generated catch block
				this._log.log(Level.ALL, "IOException when creating thumbnail for asset {0}. Skipping...", UAN);
                            } catch (SQLException e) {
				// TODO Auto-generated catch block
				this._log.log(Level.ALL, "SQLException when updating thumbnail for asset {0}. Skipping...", UAN);
                            } 
                            finally {
                                try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement " + se.getMessage()); }
                                try { if (input != null) input.close(); } catch (IOException io) { _log.log(Level.ALL, "Error closing the statement " + io.getMessage()); }
                            }
                        }
			return true;
		}
                else if (rs_count == 0) {
                    _log.log(Level.ALL, "Could not find TMS asset with filename {0}. Skipping...", UAN);
                    return false;
                }
		else {
                    _log.log(Level.ALL, "More than one Media File exists with filename {0}. Skipping...", UAN);
                    return false;
		}
		
	}

	private boolean doesUANExistInTMS(Connection tmsConn, String UAN) {
		
		String query = null;
                if (properties.getProperty("siUnit").equals("FSG")) {
                    query = "select count(*) from MediaFiles where REPLACE(REPLACE(SUBSTRING(FileName,PATINDEX('%\\%',FileName)+1,LEN(FileName)),'.jpg',''),'.tif','') = '" + UAN  + "'";
                }
                else {
                     query = "select count(*) from MediaFiles where FileName = '" + UAN + "'";
                }
                
		boolean retval = false;
                
                _log.log(Level.ALL, "SQL: doesUANExistInTMS {0}", query);
		
		ResultSet rs = DataProvider.executeSelect(tmsConn, query);
		
		try {
			while(rs.next()) {
				int count = rs.getInt(1);
				if(count > 0) {
					retval = true;
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "There was an exception during doesUANExistInTMS, asset with UAN: {0}. Skipping...", UAN);
		}
		finally {
			try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
		}
		
		return retval;
	}

	private ArrayList<String> getAssetsIngestedfromDAMS(Connection tmsConn) {
		
		ArrayList<String> retval = new ArrayList<String>();
		
		String sql = "select RenditionID from MediaRenditions where " +
				"EnteredDate >= (select top 1 LastRan from CDIS_Log where OperationType = 'sync' order by LastRan desc) AND " +
				"RenditionID in (select RenditionID from CDIS " + 
                                " where OriginalFilePath = '')";
                
                _log.log(Level.ALL,"getAssetsIngestedfromDAMS query {0}", sql);
                	
                ResultSet rs = DataProvider.executeSelect(tmsConn, sql);
                
		try {
				
			while(rs.next()) {
				retval.add(rs.getString(1));
			}
		} catch (SQLException e) {
			_log.log(Level.ALL, "getAssetsIngestedFromDAMS SQL: {0}", sql);
			e.printStackTrace();
		} finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
                             
                return retval;
	}

	private boolean updateDAMSAsset(Connection damsConn, String UOIID, String title) {
		
		String sql = "update SI_ASSET_METADATA set SOURCE_SYSTEM_ID = '" + title + "' where UOI_ID = '" + UOIID + "'";
		//System.out.println("updateDAMSAsset query: " + sql);
                _log.log(Level.ALL,"updateDAMSAsset query: {0}", sql);
                
		return (DataProvider.executeUpdate(damsConn, sql) == 1);

		
	}

	private HashMap<String, String> getObjectInfoForAsset(Connection damsConn,
			String UOIID) {
				
			//object Number = assetTitle demarcated xxxx.xxxx.xxxx
			// ACM: ex acmobj-199100760024-r1 --> object number: 1991.0076.0024, rank=1 (base zero)
			// FSC: ex FSC-P-6655_02 --> object number: FSC-P-6655, rank=2 (base one)
                        // This is not called from CHSDM unit
            
			HashMap<String, String> retval = new HashMap<String, String>();
			
			//get title of asset from DAMS
			String damsSQL = "select NAME from UOIS where UOI_ID = '" + UOIID + "'";
			String title = new String();
			ResultSet rs = null;
			PreparedStatement stmt = null;
			
			try {                              
                                stmt = damsConn.prepareStatement(damsSQL);
                                rs = stmt.executeQuery();
				
				while(rs.next()) {
					title = rs.getString(1);
					_log.log(Level.ALL, "NAME: {0}", title);
					String rank = new String();
                                        String number = new String();
					rank = "0";
					
					if (properties.getProperty("siUnit").equals("ACM")) {
                                                                                               
                                                if (!title.startsWith("acmobj-")) {
                                                    _log.log(Level.ALL, "Error, Invalid Name for ACM unit: {0}", title);
                                                    retval = null;
                                                } 
                                                
                                                if(title.split("-").length > 2) {                                                       
							//rank = title.split("-")[2].split("\\.")[0].replace("r", "");
                                                        
                                                        //The rank is the name between the -r and the '.' filename extension at the end
                                                        // We must strip the -r part of the rank...so we add 2 to the start of the index
                                                        rank = title.substring(title.lastIndexOf("-r")+2, title.lastIndexOf("."));

                                                        //The title is the name after the acmobj- up until the -r for the rank
                                                        title = title.substring(7, title.lastIndexOf("-"));  //new
						}
                                                else {
                                                    //The title is the name after the acmobj- up until the '.' for the filename extension such as .tif
                                                    title = title.substring(7, title.lastIndexOf(".")); //new
                                                }
                                                
                                                if(title.split("_").length < 8 && title.length() > 8) {
                                                  // Add periods after the 4th and 8th characters, this is the format in TMS for ACM client
                                                	retval.put(title.substring(0, 4) + "." + title.substring(4, 8) + "." + title.substring(8), rank);
                                                }
                                                else {
                                                    _log.log(Level.ALL, "Title {0} not in expected format", title);
                                                    retval = null;
                                                }
                                                
                                                
                                                _log.log(Level.ALL, "TITLE: {0}", title);
                                                _log.log(Level.ALL, "RANK: {0}", rank);
   
                                               
					}
					else if (properties.getProperty("siUnit").equals("FSG")) {  
						if(title.split("_").length == 2) {
							rank = title.split("_")[1].split("\\.")[0];
						}
						//grab first characters
						title = title.split("_")[0];
						retval.put(title, rank);
					}
                                        else if (properties.getProperty("siUnit").equals("NMAAHC")) {
                                                rank = "001";
                                                title = title.replaceAll(".jpg", "");
                                                title = title.replaceAll(".JPG", "");
                                                title = title.replaceAll("HCA_", "2007.1.69.");
                                                retval.put(title, rank);
                                        }
                                        else if (properties.getProperty("siUnit").equals("CHSDM")){
                                                _log.log(Level.ALL, "Error: CH unit does not use this processing, but uses BARCODE logic instead");
                                                retval = null;
                                        }
                                        else {
                                             _log.log(Level.ALL, "Error in determing rank, unknown siUnit {0}", properties.getProperty("siUnit"));
                                             retval = null;
                                        }
                                    					
				}
			} catch (SQLException e) {
				_log.log(Level.ALL, "getObjectIDForAsset SQL: {0}", damsSQL);
				e.printStackTrace();
                                retval = null;
                        } catch (Exception e) {
				_log.log(Level.ALL, "There was an exception processing asset {0}. Skipping...", title);
                                e.printStackTrace();
				retval = null;
			}
			finally {
				try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
				try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                        }
			
			return retval;
	}

	private HashMap<String, String> getObjectInfoFromBarcode(Connection damsConn, Connection tmsConn,
            String UOIID) {
			// This is only called from CHSDM unit
            
			//ex asset name = <object number>.<three digit rank>
			//return pairing of object number and rank
			
			HashMap<String, String> retval = new HashMap<String, String>();
			
			//get title of asset from DAMS
			String damsSQL = "select NAME from UOIS where UOI_ID = '" + UOIID + "'";
			String barcode = new String();
			ResultSet rs = null;
			PreparedStatement stmt = null;
			
			try {
				stmt = damsConn.prepareStatement(damsSQL);
				rs = stmt.executeQuery();                                                          
				
				while(rs.next()) {
					barcode = rs.getString(1);
					barcode = barcode.replaceAll(".tif", "");
                                        barcode = barcode.replaceAll(".jpg", "");
					_log.log(Level.ALL, "BARCODE: {0}", barcode);
					//title = title.replaceAll("_", ".");
					//get ObjectID from Components tables
				}
                                
                        } catch (SQLException e) {
				_log.log(Level.ALL, "getObjectInfoForAsset SQL: {0}", damsSQL);
				e.printStackTrace();
                        } finally {
				try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
				try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                        }
                            
                                
			String tmsSQL = "select ObjectID from ObjComponents where ComponentID = (select ID from BCLabels where TableID = 94 and LabelUUID = '" + barcode + "')";
			
                        String number = null;
                        String rank = new String();
                        
                        try {
                            stmt = tmsConn.prepareStatement(tmsSQL);
                        
                            rs = DataProvider.executeSelect(tmsConn, stmt);
                                                          
                            while(rs.next()) {
				number = rs.getString(1);
				rank = "01";
                            }
                            
                        } catch (SQLException e) {
				_log.log(Level.ALL, "getObjectInfoForAsset SQL: {0}", damsSQL);
				e.printStackTrace();
                        }    
                        finally {
				try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                                try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                        }
                                
                                if(number == null) {
                                        
                                        _log.log(Level.ALL, "Splitting filename-rank");
                                        
					//no barcode component was found, use filename as ObjectID
					if(barcode.contains("_")) {
						number = barcode.split("_")[0];
						rank = (barcode.split("_")[1]).split("\\.")[0];
                                                
                                                _log.log(Level.ALL, "barcode: {0}", barcode);
                                                _log.log(Level.ALL, "number: {0}", number);
                                                _log.log(Level.ALL, "rank {0}", rank);
					}
					else {
                                                _log.log(Level.ALL, "assigning default rank of 01");
                                                
						number = barcode;
						rank = "01";
					}
					
				}
				//number = title.split("_")[0];
				//rank = (title.split("_")[1]).split("\\.")[0];
				retval.put(number, rank);
			
			
			
			return retval;
	}
	
	private HashMap<String, String> getObjectInfoFromObjectID(Connection damsConn, Connection tmsConn,
			String UOIID) {
				
			//ex asset name = <object number>.<three digit rank>
			//return pairing of object number and rank
			
			HashMap<String, String> retval = new HashMap<String, String>();
			
			//get title of asset from DAMS
			String damsSQL = "select NAME from UOIS where UOI_ID = '" + UOIID + "'";
			String barcode = new String();
			ResultSet rs = null;
			PreparedStatement stmt = null;
			
			try {
				stmt = damsConn.prepareStatement(damsSQL);
				rs = stmt.executeQuery();
				
				while(rs.next()) {
					barcode = rs.getString(1);
					barcode = barcode.replaceAll(".tif", "");
                                        barcode = barcode.replaceAll(".jpg", "");
					_log.log(Level.ALL, "BARCODE: {0}", barcode);
					//title = title.replaceAll("_", ".");
					//get ObjectID from Components tables
				}
				String tmsSQL = "select ObjectID from ObjComponents where ComponentID = (select ID from BCLabels where TableID = 94 and LabelUUID = '" + barcode + "')";
				stmt = tmsConn.prepareStatement(tmsSQL);
				rs = DataProvider.executeSelect(tmsConn, stmt);
				String number = new String();
				while(rs.next()) {
					number = rs.getString(1);
				}
				//number = title.split("_")[0];
				//rank = (title.split("_")[1]).split("\\.")[0];
				String rank = "01";
				retval.put(number, rank);
			} catch (SQLException e) {
				_log.log(Level.ALL, "getObjectInfoForAsset SQL: {0}", damsSQL);
				e.printStackTrace();
			} catch (Exception e) {
				_log.log(Level.ALL, "There was an exception parsing the name of asset {0}. Skipping...", barcode);
				//e.printStackTrace();
				retval = null;
			}
			finally {
				try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
				try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
			}
			
			
			return retval;
	}

	private String getObjectIDForAsset(Connection tmsConn, String objectNumber) {
		
		String objectID = null;
		
		String sql = "select ObjectID from Objects where ObjectNumber = '" + objectNumber + "'";
		_log.log(Level.ALL, "getObjectIDForAsset SQL: {0}", sql);
                
		ResultSet rs = null;
		
		try {
			rs = DataProvider.executeSelect(tmsConn, sql);
			
			while(rs.next()) {
				objectID = rs.getString(1);
			}
		} catch (SQLException e) {
			_log.log(Level.ALL, "getObjectIDForAsset SQL: {0}", sql);
			e.printStackTrace();
		} finally {
			try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
		}
		
		return objectID;
		
	}

	private boolean createMediaRecords(Connection tmsConn, String UOIID, String UAN, String IDSPathID, String renditionNumber, String objectID, String rank, String height, String width) {

		//call dbo.CreateMediaRecords
		//@UOIID, @UAN, @IDSPathID, @RenditionNumber, @ObjectID, @Rank
                
                _log.log(Level.ALL, "Creating MediaRecord in TMS for Rendition Number {0}", renditionNumber );
                
                
                CallableStatement stmt = null;
                String renditionDate = null;
                
		try {
                    if (properties.getProperty("siUnit").equals("ACM")) {
                         stmt = tmsConn.prepareCall("{ call CreateMediaRecords(?,?,?,?,?,?)}");
                    }
                     else
                    {     
                        //CHSDM and FSG has extra fields assigned
			renditionDate = new String();
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat df1;
                        df1 = new SimpleDateFormat("yyyy-MM-dd");
                        renditionDate = df1.format(cal.getTime());
			renditionNumber = renditionNumber.replaceAll("_", ".");
			
                        _log.log(Level.ALL, "Creating MediaRecord in TMS for ObjectId: " + objectID + " rank: " + rank);
                        
			stmt = tmsConn.prepareCall("{ call CreateMediaRecords(?,?,?,?,?,?,?,?,?,?)}");
                        
                        stmt.setInt(7, Integer.parseInt(height));
                        stmt.setInt(8, Integer.parseInt(width));
                        stmt.setInt(9, (Integer.parseInt(rank) == 1?1:0));
                        stmt.setString(10, renditionDate);
			
                    }
                     
                    stmt.setString(1, UOIID);
                    stmt.setString(2, UAN);
                    stmt.setInt(3, Integer.parseInt(IDSPathID));
                    stmt.setString(4, renditionNumber);
                    stmt.setInt(5, Integer.parseInt(objectID));
                    stmt.setInt(6, Integer.parseInt(rank));
                        
                    stmt.executeUpdate();
                    return true;
                    
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
                        return false;
		} catch (NumberFormatException e) {
                        _log.log(Level.ALL, "Invalid number formatting, TMS record not updated");
                        return false;
                }
		
	}

	private HashMap<String, String> retrieveNewAssets(Connection damsConn) {
		
		//retrieve DAMS assets in the given category that have not been synced with TMS
		
                String sql = null;
                HashMap<String, String> retval = new HashMap<String, String>();
                
                if (properties.getProperty("siUnit").equals("CHSDM")) {
                    sql = "select a.UOI_ID, b.OWNING_UNIT_UNIQUE_NAME from UOIS a, SI_ASSET_METADATA b " +
				"WHERE a.UOI_ID = b.UOI_ID " +
				"AND TRIM(UPPER(a.CONTENT_STATE)) = 'NORMAL' " +
				"AND TRIM(UPPER(a.CONTENT_TYPE)) != 'SHORTCUT' " +
				"AND b.SOURCE_SYSTEM_ID is null  " +
				"AND a.UOI_ID in (select UOI_ID from NODES_FOR_UOIS where NODE_ID = " + this.properties.getProperty("categoryNodeId") + ")";
                }   
                else if (properties.getProperty("siUnit").equals("NMAAHC")) {
                    sql = "select a.UOI_ID, b.OWNING_UNIT_UNIQUE_NAME from UOIS a, SI_ASSET_METADATA b  " +
				"WHERE a.UOI_ID = b.UOI_ID " +
				"AND TRIM(UPPER(a.CONTENT_STATE)) = 'NORMAL' " +
				"AND TRIM(UPPER(a.CONTENT_TYPE)) != 'SHORTCUT' " +
				"AND b.SOURCE_SYSTEM_ID is null " +
				"AND a.UOI_ID in (select UOI_ID from NODES_FOR_UOIS where NODE_ID = " + this.properties.getProperty("categoryNodeId") + ")" +
				"AND a.NAME like 'HCA%'";
                }
                else if (properties.getProperty("siUnit").equals("FSG")) {
                    sql = "select a.UOI_ID, b.OWNING_UNIT_UNIQUE_NAME from UOIS a, SI_ASSET_METADATA b  " +
				"WHERE a.UOI_ID = b.UOI_ID " +
				"AND TRIM(UPPER(a.CONTENT_STATE)) = 'NORMAL' " +
				"AND TRIM(UPPER(a.CONTENT_TYPE)) != 'SHORTCUT' " +
				"AND b.SOURCE_SYSTEM_ID is null " +
				"AND UPPER(PUBLIC_USE) = 'YES' " +
                                "AND b.OWNING_UNIT_UNIQUE_NAME like 'FS-%'" +
                                "AND TO_CHAR(a.METADATA_STATE_DT,'YYYY-MM-DD') = '2015-02-19'";
                }
                else {
                    sql = "select a.UOI_ID, b.OWNING_UNIT_UNIQUE_NAME from UOIS a, SI_ASSET_METADATA b, SI_IDS_EXPORT c " +
				"WHERE a.UOI_ID = b.UOI_ID " +
				"AND TRIM(UPPER(a.CONTENT_STATE)) = 'NORMAL' " +
				"AND TRIM(UPPER(a.CONTENT_TYPE)) != 'SHORTCUT' " +
				"AND b.SOURCE_SYSTEM_ID is null " +
				"AND a.UOI_ID = c.UOI_ID " +
				"AND a.UOI_ID in (select UOI_ID from NODES_FOR_UOIS where NODE_ID = '" + this.properties.getProperty("categoryNodeId") + "')";
                }
		
                _log.log(Level.ALL, "SQL: Retrieving new assets {0}", sql);
                           
                PreparedStatement stmt = null;
                ResultSet rs = null;
                                
		int recordCount = 0;
                int MaxRecords = Integer.parseInt(properties.getProperty("maxDAMSIngest"));
                
		try {
                        stmt = damsConn.prepareStatement(sql);
			rs = stmt.executeQuery();
                                
			while(rs.next()) {
				retval.put(rs.getString(1), rs.getString(2));
				recordCount++;
                                
				if((MaxRecords > 0) && (recordCount > MaxRecords)) {
                                        _log.log(Level.ALL, "Warning: maximum number of Ingest Records specified in config file", sql);
					break;
				}
			}
		} catch (SQLException e) {
			_log.log(Level.ALL, "retrieveNewAssets SQL: {0}", sql);
			e.printStackTrace();
		} finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		return retval;
	}

	private ArrayList<String> retrieveFailedTransfers(
			Connection tmsConn) {
		//retrieve CDIS records that still have '-1' for UOIID
		
		ArrayList<String> retval = new ArrayList<String>();
		String sql = "select RenditionID from CDIS " +
                        " where UOIID = '-1'";
                
                _log.log(Level.ALL,sql);
		
		ResultSet rs = DataProvider.executeSelect(tmsConn, sql);
		
		try {
			while(rs.next()) {
				retval.add(rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
                
		return retval;
	}

	//UOIID, NAME
	private HashMap<String, String> getMatchingAssets(Connection damsConn,
			Connection tmsConn) {
		
		ArrayList<String> fileNames = new ArrayList<String>();
		HashMap<String, String> pairs = new HashMap<String, String>();
		
		//find Renditions that aren't present in CDIS table
		String query = "select b.FileName " +
				"from MediaRenditions a, MediaFiles b " +
				"where a.PrimaryFileID = b.FileID AND " +
				"a.RenditionID not in (select RenditionID from CDIS " +
				" order by b.FileName";
                
                _log.log(Level.ALL,query);
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
                
		try {
			stmt = tmsConn.prepareStatement(query);
			rs = DataProvider.executeSelect(tmsConn, stmt);
			int count = 0;
			while(rs.next() && count < 999) {
				fileNames.add(rs.getString(1));
				count++;
			}
                }	
		catch(SQLException sqlex) {
			sqlex.printStackTrace();
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
                String damsQuery = "select UOI_ID, NAME from UOIS where NAME in (";
			
		for(Iterator<String> iter = fileNames.iterator(); iter.hasNext();) {
			damsQuery += "'" + iter.next() + "'";
			if(iter.hasNext()) {
				damsQuery += ", ";
			}
		}
		damsQuery += ")";
		
                try {
                        stmt = damsConn.prepareStatement(damsQuery);
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				pairs.put(rs.getString(1), rs.getString(2));
			}
                }       
                catch(SQLException sqlex) {
                            sqlex.printStackTrace();
                } finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }	
                        
		//create CDIS records
		//grab RenditionID, FilePath, FileName
		for(Iterator<String> damsIter = pairs.keySet().iterator(); damsIter.hasNext();) {
			
			String UOIID = damsIter.next();
			String assetName = pairs.get(UOIID);
				
			String grabSQL = "select a.RenditionID, a.RenditionNumber, b.FileName, c.Path " +
					"from MediaRenditions a, MediaFiles b, MediaPaths c " +
					"WHERE b.FileName = '" + pairs.get(UOIID) + "' " +
							"AND a.PrimaryFileID = b.FileID " +
							"AND b.PathID = c.PathID";
			
			ResultSet damsRS = null;
                        
                        try {
                        
                            damsRS = DataProvider.executeSelect(tmsConn, grabSQL);
                            if(damsRS.next()) {
				//insert mapping into CDIS
				String insertSQL = "insert into CDIS " +
                                        "(RenditionID, RenditionNumber, UOIID, OriginalFilePath, OriginalFileName)values(" +
					damsRS.getString(1) + ", '" + damsRS.getString(2) + "', '" + UOIID + "', '" + damsRS.getString(4) + "', '" +
					damsRS.getString(3) + "')";
					
                                       _log.log(Level.ALL,insertSQL);
                                        
					DataProvider.executeInsert(tmsConn, insertSQL);
                            }			
			}	
                        catch(SQLException sqlex) {
                            sqlex.printStackTrace();
                        }
                        finally {
                              try { if (damsRS != null) damsRS.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                        }      	
                }
		// TODO Auto-generated method stub
		return null;
	}

	private boolean markAsDeleted(Connection tmsConn, String path, String fileName) {
		
		String query = "update CDIS " +
                                "set Deleted = 'Yes' " +
				"WHERE OriginalFilePath = ? AND OriginalFileName = ?";
		
                 _log.log(Level.ALL,query);
                
		PreparedStatement stmt = null;
		try {
			stmt = tmsConn.prepareStatement(query);
			stmt.setString(1, path);
			stmt.setString(2, fileName);
			
			int rowCount = DataProvider.executeUpdate(tmsConn, stmt);
			if(rowCount != 1) {
				_log.log(Level.ALL, "There was an error marking file {0}as deleted. Rolling back...", fileName);
			}
			else {
				_log.log(Level.ALL, "File {0} successfully marked as deleted.", fileName);
				return true;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
                finally {
                 try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		return false;
	}


	private ArrayList<String> renditionsForIDSSync(Connection tmsConn) {

		ArrayList<String> renditionList = new ArrayList<String>();
		
		String query = "select a.UOIID from CDIS a, MediaRenditions b, MediaFiles c where " +
				"a.RenditionID = b.RenditionID AND " +
				"b.PrimaryFileID = c.FileID AND c.PathID != ? AND " +
				"a.UOIID != '-1'";
                
                _log.log(Level.ALL,query);
		
                ResultSet rs = null;
                PreparedStatement stmt = null;
                
		try {
			stmt = tmsConn.prepareStatement(query);
			stmt.setInt(1, Integer.parseInt(properties.get("IDSPathId").toString()));
			
			rs = DataProvider.executeSelect(tmsConn, stmt);
			while(rs.next()) {
				renditionList.add(rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		return renditionList;
	}

	private ArrayList<String> retrievePrimaryFileChanges(Connection tmsConn) {
		
		ArrayList<String> primaryFileChanges = new ArrayList<String>();
		
		String query = "select RenditionID from CDIS " +
                                "where RenditionID in " +
				"(select RenditionID from MediaRenditions where PrimaryFileID in " +
				"(select FileID from MediaFiles where EnteredDate > ?)) and UOIID != '-1'";
		
		_log.log(Level.ALL,query);
		
		ResultSet rs = null;
                PreparedStatement stmt = null;
                
		try {
			stmt = tmsConn.prepareStatement(query);
			Timestamp lastRan = getLastRanTime(tmsConn, "sync");
			java.sql.Date lastRanDate = new java.sql.Date(lastRan.getTime());
			
			stmt.setDate(1, lastRanDate);
			
			rs = DataProvider.executeSelect(tmsConn, stmt);
			while(rs.next()) {
				primaryFileChanges.add(rs.getString(1));
				//System.out.println("retrievePrimaryFileChanges Adding: " + rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "SQLException caught in retrievePrimaryFileChanges: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		return primaryFileChanges;
	}

	private boolean unsyncRendition(String renditionID, Connection tmsConn,
			Connection damsConn) {
		//retrieve UOIID from CDIS
		String query = "select UOIID, OriginalFileName from CDIS " +
                             "where RenditionID = ?";
                
                _log.log(Level.ALL, query);
                ResultSet rs = null;
                ResultSet rs2 = null;
                PreparedStatement stmt = null;
                
		try {
			stmt = tmsConn.prepareStatement(query);
			stmt.setInt(1, Integer.parseInt(renditionID));
			rs = DataProvider.executeSelect(tmsConn, stmt);
			
			if(rs.next()) {
				String UOIID = rs.getString(1);
				String fileName = rs.getString(2);
				if(!rs.getString(1).equals("-1")) {
					//sync has been made, DAMS changes can be made using UOIID
					query = "update UOIS set CONTENT_STATE = 'DELETED', METADATA_STATE = 'LOCKED', " +
							"CONTENT_STATE_USER_ID = ?, CONTENT_STATE_DT = ?," +
							"METADATA_STATE_USER_ID = ?, METADATA_STATE_DT = ? " +
							"where UOI_ID = ?";
					
					stmt = damsConn.prepareStatement(query);
					stmt.setInt(1, 1);
					stmt.setDate(2, new java.sql.Date(new Date().getTime()));
					stmt.setInt(3, 1);
					stmt.setDate(4, new java.sql.Date(new Date().getTime()));
					stmt.setString(5, UOIID);
				}
				else {
					// UOIID link not present, use filename
					//grab UOIID of new asset
					query = "select UOI_ID from UOIS where NAME = ? and UOI_ID in " +
							"(select UOI_ID from SI_ASSET_METADATA where SOURCE_SYSTEM_ID = 'TMS IMPORT')";
					
					stmt = damsConn.prepareStatement(query);
					stmt.setString(1, fileName);
					_log.log(Level.ALL, "Filename: {0}", fileName);
					
                                        rs2 = stmt.executeQuery();
                                        
					rs2.next();
					UOIID = rs2.getString(1);
					
					// if SOURCE_SYSTEM_ID is not 'TMS IMPORT', we have a problem
					query = "update UOIS set CONTENT_STATE = 'DELETED', METADATA_STATE = 'LOCKED', " +
							"CONTENT_STATE_USER_ID = ?, CONTENT_STATE_DT = ?," +
							"METADATA_STATE_USER_ID = ?, METADATA_STATE_DT = ? " +
							"where UOI_ID = ?";
					
					stmt = damsConn.prepareStatement(query);
					stmt.setInt(1, 1);
					stmt.setDate(2, new java.sql.Date(new Date().getTime()));
					stmt.setInt(3, 1);
					stmt.setDate(4, new java.sql.Date(new Date().getTime()));
					stmt.setString(5, UOIID);
				}
				int result = DataProvider.executeUpdate(damsConn, stmt);
				
				if(result == 1) {
					//update was successful, clear CDIS record
					query = "delete from CDIS " +
                                                 "where UOIID = ?";
                                        
                                        _log.log(Level.ALL,query);
                                        
					stmt = tmsConn.prepareStatement(query);
					stmt.setString(1, rs.getString(1));
					
					DataProvider.executeUpdate(tmsConn, stmt);
					
					_log.log(Level.ALL, "Asset {0} has been successfully unsynced.", fileName);
					
				}
				else {
					_log.log(Level.ALL, "There was an error updating the DAMS UOIS table. Leaving TMS Sync record intact for debugging.");
					_log.log(Level.ALL, "Query: {0}", query);
				}
			}
			else {
				//no sync record present, nothing to do
				return false;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "SQLException in unsyncRendition: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//e.printStackTrace();
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (rs2 != null) rs2.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
               
		return false;
	}

	private ArrayList<String> retrieveUnsyncedRenditions(Connection tmsConn) {
		ArrayList<String> renditionIDs = new ArrayList<String>();
		//retrieve unsynced renditions - renditions with IsColor = 0, and there is a sync record for them
		String query = "select RenditionID from MediaRenditions where IsColor = 0 and RenditionID in (select RenditionID from CDIS)";
                
                _log.log(Level.ALL,query);
                
                ResultSet rs = null;
                PreparedStatement stmt= null;
                
                try {
			stmt = tmsConn.prepareStatement(query);
			
			rs = DataProvider.executeSelect(tmsConn, query);
			while(rs.next()) {
				renditionIDs.add(rs.getString(1));
				_log.log(Level.ALL, "Unsyncing rendition {0} in retrieveUnsyncedRenditions. Query: {1}", new Object[]{rs.getString(1), query});
				//System.out.println("retrieveUnsyncedRenditions Adding: " + rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "Exception in retrieveUnsyncedRenditions: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//e.printStackTrace();
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		return renditionIDs;
	}

	private boolean syncFilePath(Connection tmsConn,
			String UOIID, String UAN) {
		
		//grab original filename and file path
		String fileName = null;
		String filePath = null;
		String renditionID = null;
		/*String query = "select mf.FileName, mp.Path from MediaFiles mf, MediaPaths mp " +
				"where mf.PathID = mp.PathID and " +
				"mf.RenditionID = (select RenditionID from MediaRenditions where RenditionNumber = '" + 
				tempRendition.getRenditionNumber() + "')"; */
		
		String query = "select mf.FileName, mp.Path, mf.RenditionID from MediaFiles mf, MediaPaths mp " +
				"where mf.PathID = mp.PathID and " +
				"mf.RenditionID = (select RenditionID from CDIS " +
                                "where UOIID = '" + UOIID + "')"; 
                                
                                _log.log(Level.ALL, query);
                                
                ResultSet rs = null;   
                PreparedStatement stmt = null;
                
		try {
			stmt = tmsConn.prepareStatement(query);
			rs = stmt.executeQuery();
			
			if(rs.next()) {
				fileName = rs.getString(1);
				filePath = rs.getString(2);
				renditionID = rs.getString(3);
			}
			
			//update CDIS
			/*
			query = "update CDIS "  +
                                    "set OriginalFilePath = '" + filePath + "', OriginalFileName = '" + fileName + "' where UOIID = '" 
					+ UOIID + "'";
			
			stmt = tmsConn.prepareStatement(query);
			
			int rowCount = stmt.executeUpdate();
			
			if(rowCount != 1) {
				_log.log(Level.ALL, "There was an error updating the CDIS table.");
				return false;
			}
			*/
			//update MediaFiles
			query = "update MediaFiles set PathID = " + properties.getProperty("IDSPathId") + ", FileName = '" + UAN
					+ "' WHERE FileID = (select PrimaryFileID from MediaRenditions where RenditionID = " + renditionID + ")";
			
			stmt = tmsConn.prepareStatement(query);
			
			int rowCount = stmt.executeUpdate();
			
			if(rowCount != 1) {
				_log.log(Level.ALL, "There was an error updating MediaFiles table.");
				return false;
			}
			
                         if (properties.getProperty("siUnit").equals("CHSDM")) {
                            //update MediaRenditions.IsColor, in case ingested from DAMS
                            query = "update MediaRenditions set IsColor = 1 where RenditionID = " + renditionID;
                         
                            DataProvider.executeUpdate(tmsConn, query);
			
                            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
                         }
			
			return true;
		
		}
		catch(SQLException sqlex) {
			_log.log(Level.ALL, "Exception in syncFilePath: {0}", sqlex.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//sqlex.printStackTrace();
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
				
		return false;
	}
	
	private HashMap<String, String> getUANPairs(Connection damsConn, Connection tmsConn, ArrayList<String> UOIIDS) {
		
                String query = null;
                
                HashMap<String, String> pairings = new HashMap<String, String>();
                
                if ((properties.getProperty("siUnit").equals("CHSDM")) || (properties.getProperty("siUnit").equals("FSG"))) {
                    query = "select UOI_ID, OWNING_UNIT_UNIQUE_NAME from SI_ASSET_METADATA where UOI_ID in (";
                }
                else {
                    query = "select UOI_ID, UAN from SI_IDS_EXPORT where UOI_ID in (";
                }
                
                for(Iterator<String> iter = UOIIDS.iterator(); iter.hasNext();) {
			query += "'" + iter.next() + "'";
			if(iter.hasNext()) {
				query += ",";
			}
		}
                
                Timestamp lastRan = getLastRanTime(tmsConn, "sync");
                java.sql.Date lastRanDate = new java.sql.Date(lastRan.getTime());   
                
                PreparedStatement stmt = null;
                        
                if ((properties.getProperty("siUnit").equals("CHSDM")) || (properties.getProperty("siUnit").equals("FSG"))) {
                    query += ")";
                }
                else {
                    query += ") AND EXPORT_DATE > ?";
                     
                }
                
                _log.log(Level.ALL, "Query: {0}", query);
                
		HashMap<String, String> retval = new HashMap<String, String>();
		
                ResultSet rs = null;
		try {
			stmt = damsConn.prepareStatement(query);
			if ((!properties.getProperty("siUnit").equals("CHSDM")) && (!properties.getProperty("siUnit").equals("FSG"))) {
                            stmt.setDate(1, lastRanDate);
                        }
                        
                        rs = stmt.executeQuery();
			while(rs.next()) {
				//retval.put(rs.getString(1), rs.getString(2));
                                pairings.put(rs.getString(1), rs.getString(2));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "Exception in eligibleForSync: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//e.printStackTrace();
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		return pairings;
	}

	private boolean requiresIDSSync(Connection tmsConn,
			TMSMediaRendition tempRendition) {
		
		String query = "select PathID from MediaFiles where FileID = (select PrimaryFileID from MediaRenditions where RenditionID = "
				+ tempRendition.getRenditionID() + ")";
		
                _log.log(Level.ALL, "Query: {0}", query);
                
                ResultSet rs = null;
                
		PreparedStatement stmt = null;
		String pathID = null;
		try {
			stmt = tmsConn.prepareStatement(query);
			rs = stmt.executeQuery();
			
			if(rs.next()) {
				pathID = rs.getString(1);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "Exception in requiresIDSSync: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//e.printStackTrace();
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		if(pathID != null && pathID.equals(properties.get("IDSPathId"))) {
			return false;
		}
		else {
			return true;
		}
	}

	private ArrayList<String> getRenditionsForFileNames(Connection tmsConn,
			HashMap<String, String> syncPairs) {
		
		ArrayList<String> renditionIDs = new ArrayList<String>();
		
		//create the query
		String sql = "select RenditionID from CDIS " +
                        "where UOIID in (";
                _log.log(Level.ALL,sql);
                
		for(Iterator<String> iter = syncPairs.keySet().iterator(); iter.hasNext();) {
			String UOIID = syncPairs.get(iter.next());
			sql += "'" + UOIID + "'";
			if(iter.hasNext()) {
				sql += ", ";
			}
		}
		sql += ")";
                
                _log.log(Level.ALL, sql);
                PreparedStatement stmt = null;
                
		ResultSet rs = null;
		try {
			stmt = tmsConn.prepareStatement(sql);
			
			rs = DataProvider.executeSelect(tmsConn, stmt);
			
			while(rs.next()) {
				renditionIDs.add(rs.getString(1));
			}
		}
		catch(SQLException sqlex) {
			_log.log(Level.ALL, "Exception in getRenditionsForFileNames: {0}", sqlex.getMessage());
			_log.log(Level.ALL, "Query: {0}", sql);
			//sqlex.printStackTrace();
			return null;
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		return renditionIDs;
	}

	private ArrayList<String> getRecentlyFlaggedRenditions(
			Connection tmsConn) {
		
		ArrayList<String> renditionIDs = new ArrayList<String>();
		
		//create the query
		String sql = "select RenditionID from MediaRenditions where IsColor = 1 and RenditionID not in (select RenditionID from CDIS)";
		
                _log.log(Level.ALL,sql);
                        
                PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = tmsConn.prepareStatement(sql);
			
			rs = DataProvider.executeSelect(tmsConn, stmt);
			
			while(rs.next()) {
				renditionIDs.add(rs.getString(1));
			}
		}
		catch(SQLException sqlex) {
			_log.log(Level.ALL, "Exception in getRecentlyFlaggedRenditions: {0}", sqlex.getMessage());
			_log.log(Level.ALL, "Query: {0}", sql);
			//sqlex.printStackTrace();
			return null;
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		return renditionIDs;
	}

	private boolean syncNewRecords(Connection tmsConn, Connection damsConn,
			HashMap<String, String> syncPairs) {
		
		String query = "update CDIS " + 
                        "set UOIID = ? where OriginalFileName = ?";
		_log.log(Level.ALL,query);
                
                PreparedStatement stmt = null;                           
                ResultSet rs = null;

		for(Iterator<String> iter = syncPairs.keySet().iterator();iter.hasNext();) {
			String key = iter.next();

			try {
				stmt = tmsConn.prepareStatement(query);
				
				//syncPairs = (fileName, UOIID)
				//check if syncPairs has a filename with same name but with .tif
				String tifFilename = key.replace(".jpg", ".tif");

				if(syncPairs.keySet().contains(tifFilename) && !key.equals(tifFilename)) {
					//tif file was ingested alongside, make the metadata sync with that UOIID
					stmt.setString(1, (String)syncPairs.get(tifFilename));
					//syncPairs.remove(tifFilename);
					//syncPairs.remove(key);
					//boolean linkResult = createLink(damsConn, syncPairs.get(tifFilename).toString(), key);
				}
				else {
					stmt.setString(1, syncPairs.get(key));
					//syncPairs.remove(key);
				}
				
				stmt.setString(2, key);
				//if so, setString with that UOIID, rather than key
				//flag for creating the link
				
				
				//System.out.println("Syncing file " + key + " with UOIID " + syncPairs.get(key).toString());
				
				int result = stmt.executeUpdate();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				_log.log(Level.ALL, "SQLException throwin in syncNewRecords, trying to sync file {0}. Skipping...", key);
				_log.log(Level.ALL, "Message: {0}", e.getMessage());
				
			}
                        finally {
                            try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                             try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                        }
		}
		
		
		return false;
	}

	/*private boolean createLink(Connection damsConn, String parentUOIID, String childUOIID) {
		// TODO Auto-generated method stub
		boolean retval = false;
		
		String query = "insert into TEAMS_LINKS(SRC_TYPE, SRC_VALUE, DEST_TYPE, DEST_VALUE, LINK_TYPE)values" +
				"('UOI', ?, 'UOI', ?, 'PARENT'"
		
		return retval;
		
	}*/

	private HashMap<String, String> retrieveUOIIDSForSync(Connection damsConn,
			ArrayList<String> fileNames) {
		
		HashMap<String, String> pairings = new HashMap<String, String>();
		String query = new String();
		
		Statement stmt = null;
		ResultSet rs = null;
		
		for(Iterator<String> iter = fileNames.iterator(); iter.hasNext();) {
			String fileName = iter.next();
			query = "select a.NAME, a.UOI_ID from UOIS a, SI_ASSET_METADATA b " +
					"where NAME like '" + FilenameUtils.removeExtension(fileName) + "%' " +
					"and b.SOURCE_SYSTEM_ID = 'TMS IMPORT'" +
					"and a.UOI_ID = b.UOI_ID " +
					"and a.CONTENT_STATE != 'DELETED' ";
			
			try {
				stmt = damsConn.createStatement();
				rs = stmt.executeQuery(query);
				                                
				while(rs.next()) {
                                        _log.log(Level.ALL,"Pairings: {0}", rs.getString(2));
                                        
					pairings.put(rs.getString(1), rs.getString(2));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				_log.log(Level.ALL, "SQLException thrown while retrieving UOI_ID for file {0}. Skipping...", fileName);
				_log.log(Level.ALL, "Message: {0}", e.getMessage());
			}
                        finally {
                            try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                            try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                        }
			
		}
		
		return pairings;
	}

	private ArrayList<String> retrieveRenditionsPendingSync(Connection tmsConn) {
		
		ArrayList<String> fileNames = new ArrayList<String>();
		String query = "select OriginalFileName from CDIS " +
                                "where UOIID = '-1'";
                
		_log.log(Level.ALL, query);
                
                ResultSet rs = null;
                
		Statement stmt = null;
		try {
			stmt = tmsConn.createStatement();
			rs = stmt.executeQuery(query);

			while(rs.next()) {
				fileNames.add(rs.getString(1));
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			_log.log(Level.ALL, "Exception in retrieveRenditionsPendingSync: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		return fileNames;
	}

	private String getUOIIDForRendition(Connection tmsConn,
			TMSMediaRendition tempRendition) {
		
		String UOIID = null;
		String query = "select UOIID from CDIS " +
                        "where RenditionID = '" + tempRendition.getRenditionID() + "'";
		
                _log.log(Level.ALL, query);
                
                ResultSet rs = null;
                PreparedStatement stmt = null;
                
		try {
			stmt = tmsConn.prepareStatement(query);
			rs = stmt.executeQuery();
			
			if(rs.next()) {
				UOIID = rs.getString(1);
			}
			else {
				_log.log(Level.ALL, "Rendition {0} is not synced between TMS and DAMS. Skipping...", tempRendition.getRenditionNumber());
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "Exception in getUOIIDForRendition: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//e.printStackTrace();
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		
		return UOIID;
	}

	private boolean updateMetadata(Connection damsConn,
			TMSMediaRendition tempRendition, String UOIID) {
		
		DateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
		String dateString = df1.format(new Date());
		
		//create query - UOIS table
		StringBuffer query = new StringBuffer();
		query.append("update UOIS set ");
		// Nov2014  Name in metadata was incorrectly being updated, next line commented out
		// query.append("NAME = '" + String.valueOf(tempRendition.getRenditionNumber()) + "', ");
		query.append("METADATA_STATE_DT = TO_DATE('" + dateString + "', 'MM/DD/YYYY') ,");
                query.append("metadata_state_user_id = '22246'");
		query.append("where UOI_ID = '" + UOIID + "'");
		
		String queryUOIS = query.toString();
		queryUOIS = queryUOIS.replace('&', '+');
		_log.log(Level.ALL, "UOIS query: {0}", queryUOIS);
		
		//create query - SI_ASSET_METADATA table
		String queryMetadata = tempRendition.getMetadataQuery(UOIID);
		_log.log(Level.ALL, "Metadata query: {0}", queryMetadata);
		
		//PreparedStatement stmt = null;
		int result = DataProvider.executeUpdate(damsConn, queryUOIS);
		result = DataProvider.executeUpdate(damsConn, queryMetadata);
		
                
                
		//if(result == 1) {
			return true;
		//}
		//else {
			//return false;
		//}
		
		
	}

	private boolean placeInHotFolder(File hotFolder,
			TMSMediaRendition tempRendition, XMLBuilder xml, File workFolder) {
		
		String fileName = tempRendition.getFileName().split("\\\\")[tempRendition.getFileName().split("\\\\").length-1];
		
		// Create the metadata xml file into the work folder
		File xmlFile = new File(workFolder.getAbsolutePath() + "/" + tempRendition.getName()+ ".xml");
		Properties outputProperties = new Properties();
		outputProperties.put(javax.xml.transform.OutputKeys.INDENT, "yes");
		
	    try
	    {
	    	FileWriter xmlWriter = new FileWriter(xmlFile);
	    	xml.toWriter(xmlWriter, outputProperties);
	    	//FileUtils.writeStringToFile(xmlFile, xml);
	    	
	    	//move asset file to workfolder
	    	Scanner scanner;
                File assetFile;
                if (properties.getProperty("siUnit").equals("CHSDM")) {
                    assetFile = new File(tempRendition.getStructuralPath() + "/" + tempRendition.getFileName());
                }
                else {
                    assetFile = new File(convertMediaPath(tempRendition.getStructuralPath()) + tempRendition.getFileName());
                    System.out.println("Copying from " + convertMediaPath(tempRendition.getStructuralPath()) + "/" + tempRendition.getFileName());
                }    
		    
		    File destFile = new File(workFolder.getAbsolutePath() + "/" + fileName);
		    _log.log(Level.ALL, "Beginning file copy to work folder...");
		    _log.log(Level.ALL, "Source file size: {0}", assetFile.length());
                    _log.log(Level.ALL, "Structural Path: {0}", tempRendition.getStructuralPath());
                    _log.log(Level.ALL, "Source file size: {0}", assetFile.length());
                    
		    boolean isCopying = true;
		    FileUtils.copyFile(assetFile, destFile);
		    
		    _log.log(Level.ALL, "File copy to work folder complete.");
		    _log.log(Level.ALL, "Beginning file copy to hot folder...");
		    //if successful, copy both files to hotfolder
		    FileUtils.copyFile(destFile, new File(hotFolder.getAbsolutePath() + "/" + "MASTER" + "/" + fileName));
		    FileUtils.copyFile(xmlFile, new File(hotFolder.getAbsolutePath() + "/" + "METADATA" + "/" + fileName + ".xml"));
		    
		    _log.log(Level.ALL, "File copy to hot folder complete.");
		    
		    return true;
	    }
	    catch (IOException ioe)
	    {
	    	//ioe.printStackTrace();
	      _log.log(Level.ALL, "FileUtils threw IOException while attempting to move {0} under {1}.\nIOException message: {2}", new Object[]{tempRendition.getFileName(), xmlFile.getAbsolutePath(), ioe.getMessage()});
	      	return false;
	    } catch (TransformerException e) {
			// TODO Auto-generated catch block
	    	_log.log(Level.ALL, "TransformerException in placeInHotFolder: {0}", e.getMessage());
			return false;
            }
	    
	}

	private String convertMediaPath(String structuralPath) {
		
		//get the mediaDrive path specified in the config file
		String mediaDrive = this.properties.getProperty("mediaDrive");
		String driveLetter = mediaDrive.split(":")[0];
		String uncPath = mediaDrive.split(":")[1];
		
		if(structuralPath.startsWith(driveLetter)) {
			//replace the mapped drive path with the unc path
			return structuralPath.replace(driveLetter + ":", uncPath);
		}
		else {
			return structuralPath;
		}
	}

	private boolean createIngestRecord(Connection tmsConn,
			TMSMediaRendition tempRendition) {
		
		//check to make sure the rendition doesn't already exist in the CDIS
		String sql = "select count(*) from CDIS where RenditionID = " + tempRendition.getRenditionID();
		
		ResultSet rs = DataProvider.executeSelect(tmsConn, sql);
		try {
			rs.next();
			if(rs.getInt(1) != 0) {
				_log.log(Level.ALL, "Rendition {0} already synced. Skipping...", tempRendition.getRenditionNumber());
				return true;
			}
		}
		catch(SQLException sqlex) {
			_log.log(Level.ALL, "There was an error retrieving the sync data for rendition {0}. Skipping...", tempRendition.getRenditionNumber());
			return false;
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
                
		
		String filePath = tempRendition.getStructuralPath();
		String fileName = new String();
		String[] tempStrings = tempRendition.getFileName().split("\\\\");
		
		for(int i = 0; i < tempStrings.length; i++) {
			if(i == tempStrings.length-1) {
				fileName = tempStrings[i].split("/")[tempStrings[i].split("/").length-1];
			}
			else {
				filePath = filePath.concat(File.separator);
				filePath = filePath.concat(tempStrings[i]);
			}
		}
		
		//insert mapping into CDIS
		sql = "insert into CDIS (RenditionID, RenditionNumber, UOIID, OriginalFilePath, OriginalFileName)values(" +
				tempRendition.getRenditionID() + ", '" + tempRendition.getRenditionNumber() + "', -1, '" + filePath + "', '" +
				fileName + "')";
		
                _log.log(Level.ALL, sql);
                
		boolean success = false;
		
		success = DataProvider.executeInsert(tmsConn, sql);
		
		return success;
		
	}
	
	private boolean createLogEntry(Connection tmsConn, String operationType) {
		
		//insert entry into CDIS_Log
		String sql = "insert into CDIS_Log(OperationType, LastRan)values('" + operationType + "', CURRENT_TIMESTAMP)";
				
		boolean success = DataProvider.executeInsert(tmsConn, sql);
		
                _log.log(Level.ALL, sql);
                
		return success;
				
	}

	/* private File buildBag(Logger logger, Connection conn, File workFolder, String bagname, TMSMediaRendition asset, HoleyBagData item, XMLBuilder xml) throws BagException
	  {
	    // Create folder structure of the bag:
	    File bagFolder = Bagger.makeFolderStructure(logger, workFolder, bagname);
	    
	    // Create the metadata xml file into the data folder
	    File xmlFile = new File(bagFolder.getAbsolutePath() + File.separator+"data"+File.separator + item.xmlFilename);
	    Properties outputProperties = new Properties();
		outputProperties.put(javax.xml.transform.OutputKeys.INDENT, "yes");
	    try
	    {
	    	FileWriter xmlWriter = new FileWriter(xmlFile);
	    	xml.toWriter(xmlWriter, outputProperties);
	    	//FileUtils.writeStringToFile(xmlFile, xml);
	    }
	    catch (IOException ioe)
	    {
	      throw new BagException("FileUtils.writeStringToFile threw IOException while attempting to write to "
	          +item.xmlFilename+" under "+xmlFile.getAbsolutePath()
	          +".\nIOException message: "+ioe.getMessage());
	    } catch (TransformerException e) {
			// TODO Auto-generated catch block
	    	_log.log(Level.ALL, "TransformerException in buildBag: {0}", e.getMessage());
			//e.printStackTrace();
		}
	    
	    String mdChecksum = "";
	    try
	    {
	      mdChecksum = ChecksumUtils.getMD5ChecksumAsString(xmlFile);
	    }
	    catch (ChecksumException e)
	    {
	      throw new BagException("ChecksumException while attempting to get checksum for "+xmlFile.getAbsolutePath());
	    }
	    
	    HashMap<String,String> manifestData = new HashMap<String, String>();
	    manifestData.put(item.checksum, "data"+File.separator+item.filename);
	    manifestData.put(mdChecksum, "data"+File.separator+item.xmlFilename);
	    try
	    {
	      Bagger.writeManifest(logger, bagFolder, manifestData);
	    }
	    catch (IOException ioe)
	    {
	      throw new BagException("IOException while writing manifest file!\nIOException message: "+ioe.getMessage());
	    }
	    
	    try
	    {
	      Bagger.writeFetchTxt(logger, bagFolder, item.url, "data/"+item.filename);
	    }
	    catch (IOException ioe)
	    {
	      throw new BagException("IOException while writing fetch.txt!\nIOException message: "+ioe.getMessage());
	    }
	    
	    try
	    {
	      Bagger.writeBagitFile(logger, bagFolder);
	    }
	    catch (IOException ioe)
	    {
	      throw new BagException("IOException while writing bagit file!\nIOException message: "+ioe.getMessage());
	    }
	    
	    return bagFolder;
	  }
        */
        
	/**
	 * @param Connection tmsConn - connection to the TMS database
	 * @param ArrayList<String> newRenditions - the rendition numbers of the new renditions
	 * 											to be loaded.
	 * @return ArrayList of populated TMSMediaRendition objects. null if an error occurs.
	 * 
	 * This function will pull data for the MediaRendition RenditionNumbers found in the 
	 * supplied ArrayList, and will populate TMSMediaRendition objects with
	 */
	private ArrayList<TMSMediaRendition> loadTMSData(Connection tmsConn, ArrayList<String> newRenditions, String action) {
		
		ArrayList<TMSMediaRendition> renditionObjects = new ArrayList<TMSMediaRendition>();
		TMSMediaRenditionFactory renditionFactory = new TMSMediaRenditionFactory();
		TMSMediaRendition tempRendition = new TMSMediaRendition();
		
		for(Iterator<String> iter = newRenditions.iterator(); iter.hasNext();) {
			tempRendition = renditionFactory.retrieveRendition(iter.next(), properties, tmsConn, _log);
			if(tempRendition != null) {
				////system.out.println("Retrieved asset: " + tempRendition);
				tempRendition.setAction(action);
				renditionObjects.add(tempRendition);
			}
			else {
				_log.log(Level.ALL, "Error retrieving asset. Skipping...");
			}
			
		}
		
		
		return renditionObjects;
	}

	/**
	 * @param tmsConn
	 * @return List of media rendition IDs for newly created renditions.
	 * 
	 * This method will pull the timestamp of the last time the program was run from the CDIS_LOG table.
	 * It will then find the MediaRendition records which have an EnteredDate > the CDIS_LOG timestamp.
	 * It will then return all the MediaRendition RenditionIDs in an ArrayList.
	 */
	private ArrayList<String> retrieveNewRenditions(Connection tmsConn) {
		
		ArrayList<String> renditionIDs = new ArrayList<String>();
		
		Timestamp lastRan = getLastRanTime(tmsConn, "ingest");
		
		//create the query
		String sql = "select RenditionID from MediaRenditions where EnteredDate >= ? AND IsColor = 1 AND PrimaryFileID not in " +
                                " (select FileID from MediaFiles where PathID = " + properties.getProperty("IDSPathId") + ")";
		
		_log.log(Level.ALL, "SQL: Get TMS records {0}", sql);
		
                PreparedStatement stmt = null;
                
		ResultSet rs = null;
		try {
			stmt = tmsConn.prepareStatement(sql);
			
			stmt.setTimestamp(1, lastRan);
			
			rs = DataProvider.executeSelect(tmsConn, stmt);
			
                        int recordCount = 0;
                        
                        int MaxRecords = Integer.parseInt(properties.getProperty("maxTMSIngest"));
                        
			while(rs.next()) {
				renditionIDs.add(rs.getString(1));
                                recordCount++;
                                if((MaxRecords > 0) && (recordCount > MaxRecords)) {
                                    _log.log(Level.ALL, "Warning: maximum number of TMS Ingest Records specified in config file");
                                    break;
                                }
			}
		}
		catch(SQLException sqlex) {
			_log.log(Level.ALL, "Exception in retrieveNewRenditions: {0}", sqlex.getMessage());
			_log.log(Level.ALL, "Query: {0}", sql);
			//sqlex.printStackTrace();
			return null;
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		return renditionIDs;
	}
	
	private ArrayList<String> retrieveUpdatedRenditions(Connection tmsConn) {
		
		ArrayList<String> renditionIDs = new ArrayList<String>();
		
		HashMap<String, ArrayList<String>> auditValues = new HashMap<String, ArrayList<String>>();
		Timestamp lastRan = getLastRanTime(tmsConn, "sync");
		//get the AuditTrail table values
		ResultSet rs = null;
		String sql = "select TableName, ObjectID from AuditTrail where EnteredDate >= ?";
		PreparedStatement stmt = null;
		try {
			stmt = tmsConn.prepareStatement(sql);
			stmt.setTimestamp(1, lastRan);
			rs = DataProvider.executeSelect(tmsConn, stmt);
			
			//populate the AuditTrail values
			while(rs.next()) {
				String tableName = rs.getString(1);
				String objectID = rs.getString(2);
				////system.out.println("tableName: " + tableName);
				////system.out.println("objectID: " + objectID);
				if(auditValues.get(tableName) == null) {
					ArrayList<String> tempList = new ArrayList<String>();
					tempList.add(objectID);
					auditValues.put(tableName, tempList);
				}
				else {
					ArrayList<String> tempList = auditValues.get(tableName);
					if(!tempList.contains(objectID)) {
						tempList.add(objectID);
					}
					auditValues.put(tableName, tempList);
				}
			}
			
			//run the queries to get the RenditionIDs
			AuditTrailReader auditReader = new AuditTrailReader(this.properties.getProperty("auditConfigFile"));
			
			for(Iterator<?> iter = auditReader.getTableNames().iterator(); iter.hasNext();) {
				String table = iter.next().toString();
				////system.out.println("Table: " + table);
				String values = new String();
				if(auditValues.containsKey(table)) {
					for(Iterator<String> valueIter = auditValues.get(table).iterator();valueIter.hasNext();) {
						values += valueIter.next().toString();
						if(valueIter.hasNext()) {
							values += ",";
						}
					}
					
					sql = auditReader.getQuery(table);
					////system.out.println("SQL before the replace: " + sql);
					sql = sql.replace("?", values);
					////system.out.println("query: " + sql);
					rs = DataProvider.executeSelect(tmsConn, sql);
					
					while(rs.next()) {
						renditionIDs.add(rs.getString(1));
					}
				}
			}
		}
		catch(SQLException e) {
			_log.log(Level.ALL, "Exception in retrieveUpdatedRenditions: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", sql);
			//sqlex.printStackTrace();
			return null;
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                    try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		return renditionIDs;
	}

	private Timestamp getLastRanTime(Connection tmsConn, String opType) {
		
		Timestamp retval = null;
		
		String sql = "select top 1 LastRan from CDIS_Log where OperationType = '" + opType + "' order by LastRan desc";
		
                ResultSet rs = null;
		try {
			rs = DataProvider.executeSelect(tmsConn, sql);
		
			if(rs.next()) {
				retval = rs.getTimestamp(1);
			}
			else {
				//no rows, default to 24 hours prior
				retval = new Timestamp(System.currentTimeMillis() - 24 * 60 * 60 * 1000L);
			}
			
		}catch(SQLException e) {
			_log.log(Level.ALL, "Exception in retrieveUpdatedRenditions: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", sql);
			//sqlex.printStackTrace();
			return new Timestamp(System.currentTimeMillis() - 24 * 60 * 60 * 1000L);
		}
                finally {
                    try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
                }
		
		return retval;
	}

	/**
	 * @param configFileName
	 * @throws FileNotFoundException
	 * @throws Exception
	 * 
	 * This method verifies whether the filename passed exists in the file system. Then it 
	 * loads the properties from the file into a Properties class. The file should be formatted as
	 * below:
	 * 		key=value
	 * 
	 * After loading the properties, it then cycles through an array of required properties, and throws
	 * an exception if one is missing from the properties.
	 */
	private void loadProperties(String configFileName) throws FileNotFoundException, Exception {
		
		//verify file exists
		if(configFileName == null) {
			throw new FileNotFoundException();
		}
		
		//load properties from the file
		File configFile = new File(configFileName);
		FileInputStream inStream = new FileInputStream(configFile);
		BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
		
		String lineHolder = new String();
		lineHolder = reader.readLine();
		while(lineHolder != null) {
			if(!lineHolder.startsWith("#") && !lineHolder.trim().equals("") && lineHolder.split("=").length > 1) {
				//properties.setProperty(lineHolder.split("=")[0].trim(), lineHolder.split("=")[1].trim());
				properties.setProperty(lineHolder.split("=")[0].trim(), lineHolder.substring(lineHolder.indexOf("=")+1));
			}
			lineHolder = reader.readLine();
		}
		
		//verify that required fields are present
		for(int i = 0; i < requiredProps.length; i++) {
			String reqProp = requiredProps[i];
			if(!properties.containsKey(reqProp)) {
				throw new Exception(reqProp);
			}
		}
		
	}
	
	private HoleyBagData getHoleyBagData(TMSMediaRendition rendition, Connection tmsConn) throws ChecksumException {
		HoleyBagData item;
		
		String path = rendition.getStructuralPath() + File.separator + rendition.getFileName();
		
		String fileName = new String();
		if(rendition.getName().contains("\\")) {
			fileName = rendition.getName().split("\\\\")[rendition.getName().split("\\\\").length-1];
		}
		else {
			fileName = rendition.getName();
		}
		
		File tempFile = new File(path);
		
		String checksum = new String();
		
		if(rendition.isSynced(tmsConn, _log)) {
			//grab the checksum from the CDIS table
			checksum = rendition.getChecksum(tmsConn, _log);	
		}
		else { 
			checksum = ChecksumUtils.getMD5ChecksumAsString(tempFile);
		}
		
		item = new HoleyBagData(0, path, fileName, checksum, path, fileName + ".xml");
		
		
		return item;
	}
	
	public boolean generateIngestReport(ArrayList<String> successAssets, ArrayList<String> failedAssets, ArrayList<String> ingestedFromDAMS, ArrayList<String> ingestedFromDAMSFailed) {
                	
		String server = (String)properties.get("emailServer");
		String emailTo = (String)properties.get("emailTo");
		String emailFrom = (String)properties.get("emailFrom");
		
		Date testDate = new Date();
		SimpleDateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
		String subject = properties.getProperty("siUnit") + ": TMS/DAMS Integration Ingest Report for " + df1.format(testDate);
                int totalReportItems = 0;
		
		StringBuffer bodyBuffer = new StringBuffer();
		
		bodyBuffer.append(properties.getProperty("siUnit") + ": TMS/DAMS Integration Ingest Report for " + df1.format(testDate));
		bodyBuffer.append("<br/><br/>");
		
                int assetCount = 0;
		if(!successAssets.isEmpty()) {
			bodyBuffer.append("The following assets were ingested successfully from TMS: ");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = successAssets.iterator(); iter.hasNext();) {
				assetCount++;
                                bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");                                
			}
                        totalReportItems = assetCount;
                        bodyBuffer.append("<br/>");
                        bodyBuffer.append("Number of assets ingested from TMS: ");
                        bodyBuffer.append(assetCount);
                        bodyBuffer.append("<br/><br/>");
		}
                
                assetCount = 0;
		bodyBuffer.append("<br/>");
		if(!ingestedFromDAMS.isEmpty()) {
			bodyBuffer.append("The following DAMS assets were created successfully in TMS: ");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = ingestedFromDAMS.iterator(); iter.hasNext();) {
                                assetCount++;
				bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");                                
			}
                        totalReportItems = totalReportItems + assetCount;
                }
                bodyBuffer.append("<br/>");
                bodyBuffer.append("Number of DAMS assets created in TMS: ");
                bodyBuffer.append(assetCount);
                bodyBuffer.append("<br/><br/>");
                
                assetCount = 0;
		bodyBuffer.append("<br/>");
		if(!failedAssets.isEmpty()) {
			bodyBuffer.append("<br/><br/>");
			bodyBuffer.append("The following assets experienced errors during the ingest process, and were skipped as a result:");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = failedAssets.iterator(); iter.hasNext();) {
				assetCount++;
                                bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
                        totalReportItems = totalReportItems + assetCount;
                        
                        bodyBuffer.append("<br/>");
                        bodyBuffer.append("Number of assets errored during ingest process: ");
                        bodyBuffer.append(assetCount);
                        bodyBuffer.append("<br/><br/>");
		}
                
                assetCount = 0;
		bodyBuffer.append("<br/>");
		if(!ingestedFromDAMSFailed.isEmpty()) {
			bodyBuffer.append("The following DAMS assets experienced errors during TMS Media record creation, and were skipped as a result: ");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = ingestedFromDAMSFailed.iterator(); iter.hasNext();) {
				assetCount++;
                                bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");                                
			}
                        totalReportItems = totalReportItems + assetCount;
                }
                bodyBuffer.append("<br/>");
                bodyBuffer.append("Number of assets errored during TMS media Creation: ");
                bodyBuffer.append(assetCount);
                bodyBuffer.append("<br/><br/>");
		
		bodyBuffer.append("<br/><br/>");
		bodyBuffer.append("Please contact the DAMS Team with any issues related to TMS/DAMS Integration.");
		bodyBuffer.append("<br/>");
		
		try {

                        //create report file for attachment
                        DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmm");
			File attachmentFile = new File("rpt\\IngestReport-" + df.format(new Date()) + ".rtf");
                        FileWriter fw = new FileWriter(attachmentFile);
			fw.write(bodyBuffer.toString().replace("<br/>", "\n").replace("<hr/>", "---------------------------------"));
			fw.close();
			
                        //validate server, to, and from values 
                        if(!properties.containsKey("emailServer") ||
				!properties.containsKey("emailTo") ||
				!properties.containsKey("emailFrom")) {
			
                            _log.log(Level.ALL, "One or more email parameters are absent from the config file. Report generated, but email not sent");
                            return false;
                        }
                        
			if(totalReportItems > 0 ) {
                            if(totalReportItems <= 20) {
    				sendEmail(server, emailFrom, emailTo, subject, bodyBuffer.toString(), new String(), new String());
                            }
                            else {		
                                sendEmailWithAttachment(server, emailFrom, emailTo, subject, "Please see the attached CDIS Ingest report.", new String(), new String(), attachmentFile);
                            }
                            return true;
                        }
                        else {
                            return false;
                        }
                
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "MessagingException caught in generateIngestReport: {0}", e.getMessage());
			_log.log(Level.ALL, "Email addresses: {0}", emailTo);
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "IOException caught in generateIngestReport: {0}", e.getMessage());
			return false;
		}
		
	}
	
	public boolean generateSyncReport(ArrayList<String> metadataAssets, ArrayList<String> IDSAssets, ArrayList<String> failedMetadata, ArrayList<String> failedIDS, 
			ArrayList<String> unsyncedRenditions) {
		
		String server = (String)properties.get("emailServer");
		String emailTo = (String)properties.get("emailTo");
		String emailFrom = (String)properties.get("emailFrom");
		
		Date testDate = new Date();
		SimpleDateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
		String subject = properties.getProperty("siUnit") + ": TMS/DAMS Metadata Sync Report for " + df1.format(testDate);
		
		StringBuffer bodyBuffer = new StringBuffer();
		
		bodyBuffer.append(properties.getProperty("siUnit") + ": TMS/DAMS Metadata Sync Report for " + df1.format(testDate));
		bodyBuffer.append("<br/><br/>");
		
                int totalReportItems = 0;
                int assetCount = 0;
                
		if(!metadataAssets.isEmpty()) {
			bodyBuffer.append("The following assets had metadata changes synced from TMS: ");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = metadataAssets.iterator(); iter.hasNext();) {
                                assetCount++;
                                bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
                        totalReportItems = assetCount;
                        bodyBuffer.append("Number of metadata changes synced from TMS: ");
                        bodyBuffer.append(assetCount);
                        bodyBuffer.append("<br/>");
		}
		
                assetCount = 0;
		if(!IDSAssets.isEmpty()) {
			bodyBuffer.append("The following assets are now referencing their IDS derivative files: ");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = IDSAssets.iterator(); iter.hasNext();) {
                                assetCount++;
                                bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
                        totalReportItems = totalReportItems + assetCount;
                        bodyBuffer.append("Number of assets now referencing their IDS derivative: ");
                        bodyBuffer.append(assetCount);
                        bodyBuffer.append("<br/>");
		}
		
		assetCount = 0;
		if(!unsyncedRenditions.isEmpty()) {
			bodyBuffer.append("<br/><br/>");
			bodyBuffer.append("The following assets were unsynced and deleted from the DAMS:");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = unsyncedRenditions.iterator(); iter.hasNext();) {
				assetCount++;
                                bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
                        totalReportItems = totalReportItems + assetCount;
                        bodyBuffer.append("Number of assets unsynced and deleted from the DAMS: ");
                        bodyBuffer.append(assetCount);
		}
		
		if(!failedMetadata.isEmpty()) {
			bodyBuffer.append("<br/><br/>");
			bodyBuffer.append("The following assets experienced errors during the metadata sync process, and were skipped as a result:");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = failedMetadata.iterator(); iter.hasNext();) {
				bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
		}
		
		if(!failedIDS.isEmpty()) {
			bodyBuffer.append("<br/><br/>");
			bodyBuffer.append("The following assets experienced errors during the IDS sync process, and were skipped as a result:");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = failedIDS.iterator(); iter.hasNext();) {
				bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
		}
	
		
		bodyBuffer.append("<br/><br/>");
		bodyBuffer.append("Please contact the DAMS Team with any issues related to TMS/DAMS Integration.");
		bodyBuffer.append("<br/>");
		
		try {
                    
                    //create report file for attachment
                    DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmm");
                    File attachmentFile = new File("rpt\\MetadataSyncReport-" + df.format(new Date()) + ".rtf");
                    FileWriter fw = new FileWriter(attachmentFile);
                    fw.write(bodyBuffer.toString().replace("<br/>", "\n").replace("<hr/>", "---------------------------------"));
                    fw.close();
                                
                    //validate server, to, and from values 
                    if(!properties.containsKey("emailServer") ||
                        !properties.containsKey("emailTo") ||
                        !properties.containsKey("emailFrom")) {
			
                        _log.log(Level.ALL, "One or more email parameters are absent from the config file. Report generated, but email not sent");
                        return false;
                    }
                    
                    if(totalReportItems > 0 ) {
                            if(totalReportItems <= 20) {
                                sendEmail(server, emailFrom, emailTo, subject, bodyBuffer.toString(), new String(), new String());
                            }
                            else {
                                sendEmailWithAttachment(server, emailFrom, emailTo, subject, "Please see the attached CDIS Metadata Sync report.", new String(), new String(), attachmentFile);                    
                            }
                            return true;
                    }
                    else {
                        return false;
                    }
                 
		} catch (MessagingException e) {
			_log.log(Level.ALL, "MessagingException caught in generateSyncReport: {0}", e.getMessage());
			_log.log(Level.ALL, "Email addresses: {0}", emailTo);
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "IOException caught in generateSyncReport: {0}", e.getMessage());
			return false;
		}
	}
	

	public void sendEmail( String emailSrv, String fromAddr, String toEmailAddr, String subject, 
			String body, String emailBodyTop, String emailBodyBottom)
	throws MessagingException {

		Properties mailProps = new Properties();
		mailProps.put("mail.smtp.host", emailSrv);      
		Session session = Session.getDefaultInstance( mailProps, null );

		MimeMessage message = new MimeMessage( session );
		message.setFrom(new InternetAddress(fromAddr));
		String[] toEmailAddrArray = toEmailAddr.split(",");
		for (int i = 0; i < toEmailAddrArray.length; i++) {
			message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmailAddrArray[i]));
		}
		message.setSubject(subject);         

		// create the Multipart and its parts to it
		Multipart parts = new MimeMultipart();

		// create and fill the Body
		String emailContent = emailBodyTop +"<br>"+body+"<br>"+emailBodyBottom;
		MimeBodyPart bodyPart = new MimeBodyPart();
		bodyPart.setContent(emailContent,"text/html");
		parts.addBodyPart(bodyPart);

		// add the Multipart to the message
		message.setContent(parts);
		Transport.send(message);       
	}
	
	private void sendEmailWithAttachment(String emailSrv, String fromAddr,
			String toEmailAddr, String subject, String body, String emailBodyTop,
			String emailBodyBottom, File attachmentFile) 
	throws MessagingException {

		Properties mailProps = new Properties();
		mailProps.put("mail.smtp.host", emailSrv);      
		Session session = Session.getDefaultInstance( mailProps, null );

		MimeMessage message = new MimeMessage( session );
		message.setFrom(new InternetAddress(fromAddr));
		String[] toEmailAddrArray = toEmailAddr.split(",");
		for (int i = 0; i < toEmailAddrArray.length; i++) {
			message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmailAddrArray[i].trim()));
		}
		message.setSubject(subject);         

		// create the Multipart and its parts to it
		Multipart parts = new MimeMultipart();

		// create and fill the Body
		String emailContent = emailBodyTop +"<br>"+body+"<br>"+emailBodyBottom;
		MimeBodyPart bodyPart = new MimeBodyPart();
		bodyPart.setContent(emailContent,"text/html");
		parts.addBodyPart(bodyPart);
		
		//add the attachment
		MimeBodyPart attachmentPart = new MimeBodyPart();
	    DataSource source = new FileDataSource(attachmentFile);
	    attachmentPart.setDataHandler(new DataHandler(source));
	    attachmentPart.setFileName(attachmentFile.getName());
	    parts.addBodyPart(attachmentPart);

		// add the Multipart to the message
		message.setContent(parts);
		Transport.send(message);       
	}
	
	private void deleteOldLogs () {	
            //clear out old log files - older than 30 days
            //FileUtils.isFileOlder(;, date);
            File folderDir = new File("log\\");
            File[] logs = folderDir.listFiles();
				
            for(int i = 0; i < logs.length; i++) {
		File tempFile = logs[i];
		if(tempFile.getName().startsWith("CDISLog-")) {
                    //get 30 days before today
                    Calendar thirtyBack = Calendar.getInstance();
                    thirtyBack.add(Calendar.DAY_OF_MONTH, -30);
                    Calendar fileCal = Calendar.getInstance();
                    fileCal.setTimeInMillis(tempFile.lastModified());
                    if(thirtyBack.after(fileCal)) {
                        //delete log file
			boolean deleted = tempFile.delete();
                            if(deleted) {
                                _log.log(Level.ALL, "Log file {0} successfully deleted.", tempFile.getName());
                            }
                            else {
				_log.log(Level.ALL, "There was an error deleting log file {0}.", tempFile.getName());
                            }
                    }
		
                }
						
            }
        }
        
}