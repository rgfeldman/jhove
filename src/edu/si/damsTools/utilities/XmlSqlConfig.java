/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.utilities;

/**
 *
 * @author rfeldman
 */
import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


public class XmlSqlConfig {
       
    private NodeList opQueryNodeList;
    private String sqlQuery;
    private String fileNameAndPath;
    String destinationTable;
    String multiResultDelim;
    String operationType;
    String queryTag;
    
    private String projectCd;
    
    public String getDestinationTable() {
        return this.destinationTable;
    }
    
    public String getMultiResultDelim() {
        return this.multiResultDelim;
    }
    
    public String getOperationType() {
        return this.operationType;
    }
        
    public NodeList getOpQueryNodeList() {
        return this.opQueryNodeList;
    }
    
    public String getFileNameAndPath() {
        return this.fileNameAndPath;
    }
    
    public String getSqlQuery() {
        return this.sqlQuery;
    }
    
    public void setFileNameAndPath (String fileNameAndPath) {
        this.fileNameAndPath = fileNameAndPath;
    }
    
    public void setOpQueryNodeList(NodeList opQueryNodeList) {
        this.opQueryNodeList = opQueryNodeList;
    }
    
    public void setProjectCd (String projectCd) {
        this.projectCd = projectCd;
    }
    
    public void setQueryTag(String queryTag) {
        this.queryTag = queryTag;
    }
    
    public boolean read(String operationType) {
               
        try {

            File file = new File(fileNameAndPath);
            if (! file.exists()) {
                System.out.println ("Config file not found: " + fileNameAndPath);
            }

            //Set up the doc which will hold the xml file
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(file);
            doc.getDocumentElement().normalize();
            
            //Obtain a node pointing to the current operation type
            Node node = doc.getElementsByTagName(operationType).item(0);
        
            if (!(node.getNodeType() == Node.ELEMENT_NODE)) {
                System.out.println ("Unable to get node for operationType: " + operationType);
                return false;
            }
        
            //assign the operation type tag to an element
            Element elem = (Element) node;   
        
            //get a list of the query tags for the element
            opQueryNodeList = elem.getElementsByTagName("query");
       
        } catch (Exception e) {
           e.printStackTrace();
            return false;
        }   
        return true;
    }
    
    public boolean populateSqlInfoForType (int s) {
        
        Node fstNode = opQueryNodeList.item(s);
        
        if (!(fstNode.getNodeType() == Node.ELEMENT_NODE)) {
        
            System.out.println ("Unable to get node:  ");
            return false;
        }
                   
        Element e = (Element)fstNode;
            
        // The query type is one other than the one we are interested in
        if (! e.getAttribute("type").equals(this.queryTag)) {
            return false;
        }
       
        String rawSqlQuery = sqlQuery = fstNode.getTextContent(); 
        this.sqlQuery = replaceSqlVars (rawSqlQuery);
        
        if (this.queryTag.equals("metadataMap")) {
            this.multiResultDelim = e.getAttribute("multiResultDelim");
            this.destinationTable = e.getAttribute("destTableName");
            this.operationType = e.getAttribute("operationType");
        }
        
        return true;     
    }
    
    public String replaceSqlVars (String rawSqlQuery) {
        
        if (rawSqlQuery.contains("?PROJECT_CD?")) {
            rawSqlQuery = rawSqlQuery.replace("?PROJECT_CD?", projectCd) ;
        }
        
        return rawSqlQuery;
    }
}
