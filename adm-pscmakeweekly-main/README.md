# adm-labtnpshadow

## Table of Contents
* [Request](#Request)
* [DAG ID and Schedule](#DAG-ID-and-Schedule)
* [Technologies Used](#technologies-used)
* [Room for Improvement](#room-for-improvement)
* [Created By](#created-by)

## Request
This was originally on the 10.212.17.110 old Linux server.  This project only consists of an R script originally. It queries for all accessions from the past week and gets their psc room numbers.  Then it sends it to 2 shared drive locations

## DAG ID and Schedule
* scheduled to run daily at 9:30 am 30 9 * * *
* It runs frequently will not email it out. 

## Libraries and Dataflow
* Script 
    * Only uses R...not bash
    * Queries kbase
    * R lubridate
    * shared drive access
    
* Queries for PSC and accession info 
* Saves to uscplatxdfs001p (t drive) server
* languages: R

## Room for Improvement

## Created By:
James Carter (jcarter@cpllabs.com) 8/27/2024
