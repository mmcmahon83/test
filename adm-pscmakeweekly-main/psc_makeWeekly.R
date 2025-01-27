library(lubridate)
TODAY=Sys.Date()
secondToLastSunday = floor_date(TODAY, "week") -7  # Note if today is Sunday it counts today as last sunday
lastSunday = secondToLastSunday + 7

firstDay_Sunday_asDate = secondToLastSunday
lastDay_Sunday_asDate = lastSunday

firstDay_Sunday_asString = format(firstDay_Sunday_asDate, format="%Y-%m-%d")
lastDay_Sunday_asString =  format(lastDay_Sunday_asDate, format="%Y-%m-%d")

currentDate_asDate = firstDay_Sunday_asDate

allWeekdf <- NULL
source('/opt/airflow/dags/g.R')
while(currentDate_asDate <= lastDay_Sunday_asDate) {
  currentDate_asString = format(currentDate_asDate, format="%Y-%m-%d")
  print(currentDate_asString)
  print("You can stop query while paused right now")
  Sys.sleep(2)
  print("Query unpaused")
  weeklyQuery <- dbGetQuery(conn, paste0("SELECT ACCESSION                as ACCNUM
      , coalesce(DEMOGRAPHICS_POINTER@COLLECTED_DATE, 'NA')               as COLDAT
      , coalesce(DEMOGRAPHICS_POINTER@COLLECTED_TIME, 'NA')               as COLTIM
      , coalesce(MINI_LOG_DATE, 'NA')                                     as MINDAT
      , coalesce(DEMOGRAPHICS_POINTER@MINI_LOG_TIME, 'NA')                as MINTIM
      , coalesce(DEMOGRAPHICS_POINTER@MINI_LOG_DTG, 'NA')                 as MINDTG
      , coalesce(DEMOGRAPHICS_POINTER@MAXI_LOG_DATE, 'NA')                as MAXDAT
      , coalesce(DEMOGRAPHICS_POINTER@MAXI_LOG_TIME, 'NA')                as MAXTIM
      , coalesce(DATA_NODE_0, 'NA')                                       as PSCNUM
      , coalesce(DEMOGRAPHICS_POINTER@CLIENT_POINTER@CLIENT_NUMBER, 'NA') as CLTNUM
      , coalesce(DEMOGRAPHICS_POINTER@CLIENT_POINTER@CLIENT_NAME, 'NA')   as CLTNAM
    FROM   LABORATORY_III.ACCESSION_BY_MINI_LOG_DATE A
      , LABORATORY_III.ACCESSION_CUSTOM_BY_TYPE   B
      , LABORATORY_III.ACCESSION_ORDER            C
    WHERE A.MINI_LOG_DATE = '",currentDate_asString,"' 
    AND B.ACCESSION = A.ACCESSION
    AND C.ACCESSION = A.ACCESSION
    AND B.CUSTOM_FIELD_NAME = 'TXEROOM'"))
  allWeekdf <- rbind(allWeekdf, weeklyQuery)
  currentDate_asDate = currentDate_asDate + 1
}
dbDisconnect(conn)
allWeekdf2 <- allWeekdf[ (allWeekdf$MINDTG >= paste0(firstDay_Sunday_asString," 06:00:00")) & (allWeekdf$MINDTG <= paste0(lastDay_Sunday_asString," 06:00:00")), ]


source('/opt/airflow/dags/g.R')
arclients <- dbGetQuery(conn, "select  CLIENT_NUMBER as CLTNUM
      , coalesce(REVENUE_CENTER, 'NA')  as REVCEN
      , coalesce(PRIMARY_SALES_ID, 'NA')  as TERRIT
      FROM ACCOUNTS_RECEIVABLE_III.AR3_CLIENT        D
      WHERE ORGANIZATION = 4")
dbDisconnect(conn)

merged <- merge(x=allWeekdf2, y=arclients, by="CLTNUM", all.x=TRUE)
merged <- unique(merged)

merged <- merged[order(merged$REVCEN, merged$TERRIT,merged$CLTNUM),  ]

merged2 <- merged[, c(
 'ACCNUM'
,'COLDAT'
,'COLTIM'
,'MINDAT'
,'MINTIM'
,'MAXDAT'
, 'MAXTIM'
,'REVCEN'
,'TERRIT'
,'PSCNUM'
,'CLTNUM'
,'CLTNAM'
)]


colnames(merged2) <- c(
 'Accession'
,'CollDate'
,'CollTime'
,'MiniDate'
,'MiniTime'
,'MaxiDate'
, 'MaxiTime'
,'RevCent'
,'Terr'
,'PSC/IOP'
,'Client#'
,'Client Name'
)

# Now to make this match up to what Mike Murphy was used to getting straight from the kbase misys application
merged3 <- merged2
merged3[is.na(merged3)] <- ''

oldpattern = "%H:%M:%S"
newpattern = "%I:%M %p"
merged3[ which(!is.na(strptime(merged3$MaxiTime, format=oldpattern))), "MaxiTime"] <- format(strptime(merged3[ which(!is.na(strptime(merged3$MaxiTime, format=oldpattern))), "MaxiTime"], format=oldpattern), format=newpattern)
merged3[ which(!is.na(strptime(merged3$MiniTime, format=oldpattern))), "MiniTime"] <- format(strptime(merged3[ which(!is.na(strptime(merged3$MiniTime, format=oldpattern))), "MiniTime"], format=oldpattern), format=newpattern)

oldpattern = "%Y-%m-%d"
newpattern = "%m/%d/%Y"
merged3[ which(!is.na(strptime(merged3$MaxiDate, format=oldpattern))), "MaxiDate"] <- format(strptime(merged3[ which(!is.na(strptime(merged3$MaxiDate, format=oldpattern))), "MaxiDate"], format=oldpattern), format=newpattern)
merged3[ which(!is.na(strptime(merged3$MiniDate, format=oldpattern))), "MiniDate"] <- format(strptime(merged3[ which(!is.na(strptime(merged3$MiniDate, format=oldpattern))), "MiniDate"], format=oldpattern), format=newpattern)

outputfilename = paste0("testpscReport_",firstDay_Sunday_asString,"_to_",lastDay_Sunday_asString,".txt")

output=file.path(getwd(), outputfilename) 
write.table(merged3, outputfilename,row.names=F,sep="\t", quote=FALSE)

result=sprintf('{{ ti.xcom_push(key="filename",value="%s") }}',output)
cat(result)

