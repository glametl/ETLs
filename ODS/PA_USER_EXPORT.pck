CREATE OR REPLACE PACKAGE PA_USER_EXPORT AS
/******************************************************************************
   NAME:       PA_HADOOP_EXPORT
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        4/15/2011            BB      created this package.
******************************************************************************/

  PROCEDURE WRITE_USER_FILE( p_month_id NUMBER );
  
END PA_USER_EXPORT;
/
CREATE OR REPLACE PACKAGE BODY PA_USER_EXPORT AS
/******************************************************************************
   NAME:       PA_HADOOP_EXPORT
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        4/15/2011            BB      created this package.
******************************************************************************/

PROCEDURE WRITE_USER_FILE( p_month_id NUMBER )
   IS 
      v_file    UTL_FILE.FILE_TYPE;
      v_buffer  VARCHAR2(32767);
      v_name    VARCHAR2(128) := 'FACT_MEDIA_USER_MONTHLY_' || p_month_id || '.csv';
      --v_name    VARCHAR2(128) := p_tablename || '-' || p_starthr || '.txt';
      --v_startc  VARCHAR2(55) := 
      v_lines   PLS_INTEGER := 0;
      v_startdate DATE;
      v_enddate DATE;
      v_nexddateid NUMBER;
      c_sep     CONSTANT VARCHAR2(1) := '|';
      c_eol     CONSTANT VARCHAR2(1) := CHR(10);
      c_seplen  CONSTANT PLS_INTEGER := LENGTH(c_sep);
      c_eollen  CONSTANT PLS_INTEGER := LENGTH(c_eol);
      c_maxline CONSTANT PLS_INTEGER := 32767;
      i         PLS_INTEGER := 0;
      

   BEGIN

    -- OPEN OUTPUT FILE
    v_file := UTL_FILE.FOPEN('HADOOP_OUTPUT',v_name,'W',c_maxline);
    
    -- DATE HANDLING STUFF FOR WHEN NOT USING HOUR COLUMN
    /*
    v_nexddateid := to_number(to_char( to_date(p_dateid,'yyyymmdd') + 1,'yyyymmdd'));
    v_startdate := to_date( to_char( p_dateid || ' ' || p_starthr ),'yyyymmdd hh24' );
    IF p_endhr != 24 THEN
        v_enddate := to_date( to_char( p_dateid || ' ' || p_endhr ),'yyyymmdd hh24' );
    ELSE
        v_enddate := to_date( to_char( v_nexddateid || ' 0' ),'yyyymmdd hh24' );
    END IF;
    */

    -- SELECT DATA FOR OUTPUT
    --  TODO: OPTIMIZE FOR MULTIPLE FILES/PARALLEL QUERIES - REFACTOR THIS TO SEPERATE PROC FOR BACKGROUNDING
    FOR r IN (SELECT 
                    MONTH_ID || c_sep ||
                    ADVERTISER_ID || c_sep ||
                    ORDER_ID || c_sep ||
                    AD_ID || c_sep ||
                    USER_ID || c_sep ||
                    SITE_ID || c_sep ||
                    AFFILIATE_ID || c_sep ||
                    DMA_ID || c_sep ||
                    COUNTRY_ID || c_sep ||
                    STATE_PROVINCE || c_sep ||
                    CITY_ID || c_sep ||
                    CLICKS || c_sep ||
                    IMPRESSIONS || c_sep ||
                    PAGE_ID || c_sep ||
                    SOURCE_ID || c_sep ||
                    AGENT_TYPE || c_sep ||
                    INTERNAL_IP || c_sep ||
                    GA_CITY || c_sep ||
                    CREATIVE_ID || c_sep ||
                    GA_OS_ID || c_sep ||
                    GA_BROWSER_ID || c_sep ||
                    AD_GROUP_ID || c_sep ||
                    NAD_DIMENSION || c_sep ||
                    COUNTRY_CODE || c_sep ||
                    USERID_22_22 || c_sep ||
                    IP AS CSV0
              FROM CAMPAIGN.FACT_MEDIA_USER_MONTHLY
             WHERE MONTH_ID = p_month_id
               --AND ROWNUM < 1001
             )

    -- LOOP THROUGH OUTPUT CURSOR
    LOOP
        IF i < 4 AND LENGTH(v_buffer) + c_eollen + LENGTH(r.csv0) + c_seplen < c_maxline
        THEN
            v_buffer := v_buffer || c_eol || r.CSV0;
            i := i+1;
        ELSE
            IF v_buffer IS NOT NULL THEN
                UTL_FILE.PUT_LINE(v_file, v_buffer);
            END IF;
            v_buffer := r.CSV0;
            i := 0;
        END IF;
    END LOOP;

    UTL_FILE.PUT_LINE(v_file, v_buffer);
    UTL_FILE.FCLOSE(v_file);

   END;

END PA_USER_EXPORT;
/
