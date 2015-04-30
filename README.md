# cdis
Collections - DAMS Integration System v2.0

CDIS Overview:
CDIS is a suite of batch process that integrates the Digital Asset Management System (DAMS) with the SI units Collection Information System (CIS).
The batch processes are run on a nightly basis, but can also be manually executed.
  
  
The batch process operation types are as follows:

IngestToCIS   (Adds the CIS media record of a DAMS image to an existing object in the CIS)

IngestToDAMS  (Copies images on the CIS' media drive to the DAMS hotfolder for ingest into DAMS)

LinkToCIS     (Establishes a link from the DAMS media back to the CIS)

Sync          (Brings over metadata from the CIS to the DAMS, Syncs the pathname in the CIS to point to the IDS derivative rather than the media drive) 

ThumbnailSync (replaces the thumbnail image in the CIS with the current thumbnail image in the DAMS system)


The java code for ALL of these execution types is found in this repository and is to be compiled into a single .jar file.
Please refer to CDIS2.0 Configuration Guide.docx for details on how to configure and execute the CDIS batch processes.


