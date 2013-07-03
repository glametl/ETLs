create or replace package test_utl_file as

  --type dump_ot is object(file_name varchar2(128), no_records number, session_id number);
  --type dump_ntt is table of dump_ot;
  procedure write_hadoop_file(p_current_date_hour date);

  function parallel_dump
  (
    p_source    in sys_refcursor,
    p_date_hour varchar2
  ) return dump_ntt
    pipelined
    parallel_enable(partition p_source by any);
PROCEDURE WRITE_QCSEGS_FILE( p_dateid NUMBER );
end; 
/
create or replace package body test_utl_file as

  --type dump_ot as object(file_name varchar2(128), no_records number, session_id number);
  --type dump_ntt as table of dump_ot;
  procedure write_hadoop_file(p_current_date_hour date) is
    l_cnt number;
    c_sep constant varchar2(1) := chr(9);
    l_date_hour varchar2(100);
  begin
    l_date_hour := to_char(p_current_date_hour, 'yyyymmdd') || '_' ||
                   to_number(to_char(p_current_date_hour, 'hh24'));
    select count(1)
    into   l_cnt
    from   table(parallel_dump(cursor
                                (select /*+ PARALLEL(s,5) */
                                  to_char(time_stamp, 'mm/dd/yyyy hh24:mi:ss') ||
                                   c_sep ||
                                   to_char(time_stamp, 'hh24') ||
                                   c_sep || -- date hour
                                   impressions || c_sep ||
                                   clicks || c_sep || ip ||
                                   c_sep || request_seq ||
                                   c_sep || request_status ||
                                   c_sep || trim(request_id) ||
                                   c_sep || user_id || c_sep ||
                                   affiliate_id || c_sep ||
                                   site_id || c_sep || page_id ||
                                   c_sep || ad_size_request ||
                                   c_sep || ad_id || c_sep ||
                                   creative_id || c_sep ||
                                   content_partner_id || c_sep ||
                                   data_id || c_sep ||
                                   developer_id || c_sep ||
                                   module_id || c_sep || tile ||
                                   c_sep || sequence_nbr ||
                                   c_sep || ord || c_sep ||
                                   tag_type || c_sep ||
                                   atf_value || c_sep || flags ||
                                   c_sep || url_enc || c_sep ||
                                   country_id || c_sep ||
                                   dma_id || c_sep ||
                                   state_province || c_sep ||
                                   areacode_old || c_sep ||
                                   zip_code || c_sep ||
                                   connection_type_id || c_sep ||
                                   os_id || c_sep ||
                                   browser_id || c_sep ||
                                   browser_version || c_sep ||
                                   site_data_ex || c_sep ||
                                   site_data || c_sep ||
                                   connection_status || c_sep ||
                                   request_duration || c_sep ||
                                   http_status || c_sep || '' ||
                                   c_sep || --PROCESS_ID
                                   '' || c_sep || --LOGTYPE
                                   '' || c_sep || --SERVER_NAME
                                   placeholder || c_sep ||
                                   build_id || c_sep ||
                                   city_id || c_sep ||
                                   areacode || c_sep ||
                                   cb_advertiser_id || c_sep ||
                                   time_stamp || c_sep ||
                                   agent_type || c_sep ||
                                   internal_ip || c_sep ||
                                   zone_id || c_sep ||
                                   ad_group_id /*|| c_sep ||
                                   key_values || c_sep ||
                                   page_url || c_sep ||
                                   user_agent || c_sep ||
                                   geo_string*/
                                 from   campaign.new_dedup_imps_clicks s
                                 where  time_stamp =
                                        to_date('20110817 11', 'yyyymmdd hh24')), l_date_hour)) nt;
    dbms_output.put_line(l_cnt);
  end;

  function parallel_dump
  (
    p_source    in sys_refcursor,
    p_date_hour varchar2
  ) return dump_ntt
    pipelined
    parallel_enable(partition p_source by any) as
    type row_ntt is table of varchar2(32767);
    v_rows   row_ntt;
    v_file   utl_file.file_type;
    v_buffer varchar2(32767);
    v_sid    number;
    v_name   varchar2(128);
    v_lines  pls_integer := 0;
    c_eol constant varchar2(1) := chr(10);
    --c_eollen  constant pls_integer := length(c_eol);
    --c_maxline constant pls_integer := 32767;
  begin
    select sid into v_sid from v$mystat where rownum = 1;
    v_name := 'HRLY_DEDUP_' || p_date_hour || '_' || v_sid ||
              '.txt';
    v_file := utl_file.fopen('HADOOP_OUTPUT', v_name, 'w', 32767);
    loop
      fetch p_source bulk collect
        into v_rows limit 1000;
      for i in 1 .. v_rows.count loop
        if length(v_buffer || c_eol || v_rows(i)) <= 32767 then
          v_buffer := v_buffer || c_eol || v_rows(i);
        else
          if v_buffer is not null then
            utl_file.put_line(v_file, v_buffer);
          end if;
          v_buffer := v_rows(i);
        end if;
      end loop;
      v_lines := v_lines + v_rows.count;
      exit when p_source%notfound;
    end loop;
    close p_source;
    utl_file.put_line(v_file, v_buffer);
    utl_file.fclose(v_file);
    pipe row(dump_ot(v_name, v_lines, v_sid));
    return;
  end parallel_dump;

PROCEDURE WRITE_QCSEGS_FILE( p_dateid NUMBER )
   IS 
      v_file    UTL_FILE.FILE_TYPE;
      v_buffer  VARCHAR2(32767);
      v_name    VARCHAR2(128) := 'ghost_qcsegs_' || p_dateid || '.txt';
      --v_name    VARCHAR2(128) := p_tablename || '-' || p_starthr || '.txt';
      --v_startc  VARCHAR2(55) := 
      v_lines   PLS_INTEGER := 0;
      v_startdate DATE;
      v_enddate DATE;
      v_nexddateid NUMBER;
      c_sep     CONSTANT VARCHAR2(1) := CHR(9);
      c_eol     CONSTANT VARCHAR2(1) := CHR(10);
      c_seplen  CONSTANT PLS_INTEGER := LENGTH(c_sep);
      c_eollen  CONSTANT PLS_INTEGER := LENGTH(c_eol);
      c_maxline CONSTANT PLS_INTEGER := 32767;
      i         PLS_INTEGER := 0;
      

   BEGIN

    -- OPEN OUTPUT FILE
    v_file := UTL_FILE.FOPEN('HADOOP_OUTPUT',v_name,'W',c_maxline);
    
    -- DATE HANDLING STUFF FOR WHEN NOT USING HOUR COLUMN
    v_nexddateid := to_number(to_char( to_date(p_dateid,'yyyymmdd') + 1,'yyyymmdd'));

    -- SELECT DATA FOR OUTPUT
    --  TODO: OPTIMIZE FOR MULTIPLE FILES/PARALLEL QUERIES - REFACTOR THIS TO SEPERATE PROC FOR BACKGROUNDING
    FOR r IN (  
                select USER_ID, KEY_VALUES
                  from ADAPTIVE_DATA.GA_RAW_GHOST_IMPS
                 where TIME_STAMP >= to_date(p_dateid,'yyyymmdd')
                   AND TIME_STAMP < to_date(v_nexddateid,'yyyymmdd')
                   AND AD_SIZE_REQUEST='555x2'
                   AND KEY_VALUES LIKE '%qcsegs%'
             )

    -- LOOP THROUGH OUTPUT CURSOR
    LOOP
        IF i < 10 AND LENGTH(v_buffer) + c_eollen + LENGTH(r.user_id) + c_seplen + LENGTH(r.key_values) + c_seplen < c_maxline
        THEN
            v_buffer := v_buffer || c_eol || r.user_id || c_sep || r.key_values;
            i := i+1;
        ELSE
            IF v_buffer IS NOT NULL THEN
                UTL_FILE.PUT_LINE(v_file, v_buffer);
            END IF;
            v_buffer := r.user_id || c_sep || r.key_values;
            i := 0;
        END IF;
    END LOOP;

    UTL_FILE.PUT_LINE(v_file, v_buffer);
    UTL_FILE.FCLOSE(v_file);

   END;
end; 
/
