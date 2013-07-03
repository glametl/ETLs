create or replace package pkg_ga_processor as

  procedure ga_imps_insert
  (
    v_pid           number,
    v_ext_tablename varchar2,
    v_servername    varchar2,
    v_logtype       varchar2
  );

  function is_internal_ip(ip_addr varchar2) return number;

  procedure ga_create_ext_table
  (
    v_pid           number,
    v_ext_tablename varchar2,
    v_logfilename   varchar2,
    v_logtype       varchar2
  );

end pkg_ga_processor;
/
create or replace package body pkg_ga_processor as

  /******************************************************************************
     NAME:        PA_DEDUPLICATE
     PURPOSE:     DEDUPLICATES DAILY GLAMADAPT IMPRESSIONS
  
     REVISIONS:
     Ver        Date        Author           Description
     ---------  ----------  ---------------  ------------------------------------
     1.0        09/07/2010  BBERESFORD       1. Created this package body.
  ******************************************************************************/
  procedure ga_imps_insert
  (
    v_pid           number,
    v_ext_tablename varchar2,
    v_servername    varchar2,
    v_logtype       varchar2
  ) is
  begin
    -- FOLLOW PATH FOR ghost LOGTYPE
    if (v_logtype = 'ghost') then
      -- INSERT ALL GHOST IMPS TO GHOST TABLE
      ---- SEQ 0: SELECTION CALL IMPRESSION NOT SERVED
      ---- SEQ 6: TIMED OUT REMNANT CALL
      execute immediate '
        insert /*+ APPEND */ into adaptive_data.ga_raw_ghost_imps
            (time_stamp, ip, request_seq, request_status,
             request_id, user_id, affiliate_id, site_id, page_id,
             ad_size_request, ad_id, creative_id,
             content_partner_id, data_id, developer_id,
             module_id, tile, sequence_nbr, ord, tag_type,
             atf_value, flags, url_enc, country_id, dma_id,
             state_province, areacode, zip_code, city_id,
             connection_type_id, ad_group_id, agent_type,
             browser_version, site_data_ex, cb_advertiser_id,
             connection_status, request_duration, http_status,
             process_id, logtype, server_name, key_values,
             page_url, user_agent, placeholder, build_id,
             internal_ip, geo_string)
          select /*+ ORDERED_PREDICATES */
             to_date(substr(time_stamp, 1, instr(time_stamp, ''-'') - 2), ''dd/mon/yyyy:hh24:mi:ss'') +
              ((to_number(substr(time_stamp, instr(time_stamp, ''-'', 10))) / -100) / 24),
             ip, request_seq, request_status, request_id,
             user_id, coalesce(affiliate_id, -1),
             coalesce(site_id, -1), page_id, ad_size_request,
             coalesce(ad_id, 0), creative_id, content_partner_id,
             data_id, developer_id, coalesce(module_id, -1),
             tile, sequence_nbr, ord, tag_type, atf_value, flags,
             url_enc, country_id, dma_id, state_province,
             areacode, zip_code, city_id, connection_type_id,
             os_id, agent_type, browser_version,
             null as site_data_ex, cb_advertiser_id,
             connection_status, request_duration, http_status, ' ||
                        v_pid || ' as process_id, ''' ||
                        v_logtype || ''' as logtype, ''' ||
                        v_servername || ''' as server_name,
             key_values, page_url, user_agent, placeholder,
             build_id, is_internal_ip(ip), geo_string
          from  ' || v_ext_tablename || ' a
          where  request_seq in (0, 6)
          and    length(time_stamp) = 26
          and    regexp_substr(time_stamp, ''[0-9]{2}/[A-Za-z]{3}/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} -[0-9]{4}'') is not null';
    end if;
    commit;
  end;

  function is_internal_ip(ip_addr varchar2) return number is
    -- Possible meanings for the return value:
    -- 0: IS NOT INTERNAL
    -- 1: IS INTERNAL
    internal_ip_list varchar2(255) := '202.214.182.129 203.76.169.58 206.173.192.2 208.80.216.26 220.225.43.170 61.12.121.26 66.104.42.18 79.121.208.40 82.135.35.34 83.244.243.226 88.217.171.198';
  begin
    if (instr(internal_ip_list, ip_addr) > 0) then
      return 1;
    end if;
    return 0;
  end;

  procedure ga_create_ext_table
  (
    v_pid           number,
    v_ext_tablename varchar2,
    v_logfilename   varchar2,
    v_logtype       varchar2
  ) is
  begin
    -- CREATE EXT TABLE BASED ON LOG
    execute immediate 'CREATE TABLE ' || v_ext_tablename || ' (
      TIME_STAMP VARCHAR2(55)
    , IP VARCHAR2(55)
    , REQUEST_SEQ NUMBER
    , REQUEST_STATUS NUMBER
    , REQUEST_ID CHAR(40)
    , USER_ID NUMBER
    , AFFILIATE_ID NUMBER
    , SITE_ID NUMBER
    , PAGE_ID VARCHAR2(255)
    , AD_SIZE_REQUEST VARCHAR2(255)
    , AD_ID NUMBER
    , CREATIVE_ID NUMBER
    , CONTENT_PARTNER_ID NUMBER
    , DATA_ID NUMBER
    , DEVELOPER_ID NUMBER
    , MODULE_ID NUMBER
    , TILE NUMBER
    , SEQUENCE_NBR NUMBER
    , ORD NUMBER
    , TAG_TYPE VARCHAR2(55)
    , ATF_VALUE VARCHAR2(55)
    , FLAGS NUMBER
    , URL_ENC VARCHAR2(55)
    , COUNTRY_ID VARCHAR2(8)
    , DMA_ID NUMBER
    , STATE_PROVINCE VARCHAR2(55)
    , AREACODE VARCHAR2(55)
    , ZIP_CODE VARCHAR2(55)
    , CITY_ID VARCHAR2(255)
    , CONNECTION_TYPE_ID NUMBER
    , OS_ID NUMBER
    , AGENT_TYPE NUMBER
    , BROWSER_VERSION NUMBER
    , SITE_DATA_EX VARCHAR2(1000)
    , CB_ADVERTISER_ID NUMBER
    , CONNECTION_STATUS VARCHAR2(55)
    , REQUEST_DURATION NUMBER
    , HTTP_STATUS NUMBER
    , BUILD_ID NUMBER
    , KEY_VALUES VARCHAR2(4000)
    , PLACEHOLDER VARCHAR2(55)
    , PAGE_URL VARCHAR2(4000)
    , USER_AGENT VARCHAR2(4000)
    , GEO_STRING VARCHAR2(4000)
     )
ORGANIZATION EXTERNAL
    (   TYPE ORACLE_LOADER DEFAULT
        DIRECTORY GA_IMPS_TMPLOGS
        ACCESS PARAMETERS (
            RECORDS DELIMITED BY NEWLINE
            NOBADFILE
            NODISCARDFILE
            NOLOGFILE
            FIELDS TERMINATED BY ''^''
            MISSING FIELD VALUES ARE NULL
                (
                      TIME_STAMP
                    , IP
                    , REQUEST_SEQ
                    , REQUEST_STATUS
                    , REQUEST_ID
                    , USER_ID NULLIF USER_ID = ''reserved'' OR USER_ID = ''none'' OR USER_ID = ''default'' OR USER_ID = ''null'' or USER_ID = ''None''
                    , AFFILIATE_ID NULLIF AFFILIATE_ID = ''reserved'' OR AFFILIATE_ID = ''none'' OR AFFILIATE_ID = ''default'' OR AFFILIATE_ID = ''null'' or AFFILIATE_ID = ''None'' or AFFILIATE_ID = ''undefined''
                    , SITE_ID NULLIF SITE_ID = ''reserved'' OR SITE_ID = ''none'' OR SITE_ID = ''default'' OR SITE_ID = ''null'' or SITE_ID = ''None''
                    , PAGE_ID
                    , AD_SIZE_REQUEST
                    , AD_ID
                    , CREATIVE_ID
                    , CONTENT_PARTNER_ID
                    , DATA_ID
                    , DEVELOPER_ID
                    , MODULE_ID NULLIF MODULE_ID = ''reserved'' OR MODULE_ID = ''none'' OR MODULE_ID = ''default'' OR MODULE_ID = ''null'' or MODULE_ID = ''None'' or MODULE_ID = ''undefined''
                    , TILE
                    , SEQUENCE_NBR
                    , ORD NULLIF ORD = ''reserved'' OR ORD = ''none'' OR ORD = ''default'' OR ORD = ''null'' or ORD = ''None''
                    , TAG_TYPE
                    , ATF_VALUE
                    , FLAGS
                    , URL_ENC
                    , COUNTRY_ID
                    , DMA_ID NULLIF DMA_ID = ''reserved'' or DMA_ID = ''none'' OR DMA_ID = ''default'' OR DMA_ID = ''null'' or DMA_ID = ''None''
                    , STATE_PROVINCE NULLIF STATE_PROVINCE = ''reserved'' or STATE_PROVINCE = ''none'' OR STATE_PROVINCE = ''default'' OR STATE_PROVINCE = ''null'' or STATE_PROVINCE = ''None''
                    , AREACODE
                    , ZIP_CODE
                    , CITY_ID
                    , CONNECTION_TYPE_ID NULLIF CONNECTION_TYPE_ID = ''reserved'' OR CONNECTION_TYPE_ID = ''none'' OR CONNECTION_TYPE_ID = ''default'' OR CONNECTION_TYPE_ID = ''null'' or CONNECTION_TYPE_ID = ''None''
                    , OS_ID
                    , AGENT_TYPE
                    , BROWSER_VERSION
                    , SITE_DATA_EX CHAR(1000)
                    , CB_ADVERTISER_ID NULLIF CB_ADVERTISER_ID = ''reserved'' OR CB_ADVERTISER_ID = ''none'' OR CB_ADVERTISER_ID = ''default'' OR CB_ADVERTISER_ID = ''null'' or CB_ADVERTISER_ID = ''None''
                    , CONNECTION_STATUS
                    , REQUEST_DURATION
                    , HTTP_STATUS
                    , BUILD_ID
                    , KEY_VALUES CHAR(4000)
                    , PLACEHOLDER
                    , PAGE_URL CHAR(4000)
                    , USER_AGENT CHAR(4000)
                    , GEO_STRING CHAR(4000)
                )
            )
        LOCATION (''' || v_logfilename ||
                      ''' )
    )
REJECT LIMIT UNLIMITED';
    -- LOGFILE ''' || v_logtype || '''_issues.log''
  end;

end pkg_ga_processor;
/
