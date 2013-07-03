create or replace package pkg_udm_daily as

  procedure run_udm
  (
    p_process_name varchar2,
    p_process_date number default null
  );

  procedure load_url_detail_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_url_combined
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure create_url_file_new
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_affl_list_data;

  procedure compose_sendemailmessage
  (
    p_run_id       number,
    p_stage        varchar2,
    p_process_date number,
    p_message      varchar2,
    p_process_name varchar2
  );

end pkg_udm_daily;
/
create or replace package body pkg_udm_daily as

  procedure run_udm
  (
    p_process_name varchar2,
    p_process_date number default null
  ) is
    l_run_id       number;
    l_process_date number;
    l_process_name varchar2(10);
    l_error_msg    varchar2(4000);
  begin
    if p_process_date is null then
      l_process_date := to_number(to_char(sysdate - 1, 'yyyymmdd'));
    elsif p_process_date is not null then
      l_process_date := p_process_date;
    end if;
    select max(run_id)
    into   l_run_id
    from   daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) like
           upper(p_process_name) || '-RUN_UDM%';
    if l_run_id is null then
      l_run_id := seq_daily_campaign_run.nextval;
    end if;
    -- make sure we delete the record for the main process name(this has dependency in shell script).
    delete from daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) like
           upper(p_process_name) || '-RUN_UDM%'
    and    run_id = l_run_id
    and    status in ('RUNNING', 'FAIL');
    l_process_name := initcap(p_process_name);
    -- send intimation mail for starting of process
    compose_sendemailmessage(l_run_id, 'START', l_process_date, 'UDM_DAILY Load Data', l_process_name);
    -- log starting of the process
    pkg_log_process.log_process(l_run_id, l_process_date, l_process_name ||
                                 '-run_udm', 'I');
    -- other agg tables based on base table
    load_url_detail_day(l_run_id, l_process_date, l_process_name);
    load_url_combined(l_run_id, l_process_date, l_process_name);
    create_url_file_new(l_run_id, l_process_date, l_process_name);
    -- mark the process as complete in log table
    pkg_log_process.log_process(l_run_id, l_process_date, l_process_name ||
                                 '-run_udm', 'UC');
    -- send completion mail
    compose_sendemailmessage(l_run_id, 'COMPLETE', l_process_date, 'UDM_DAILY Load Data', l_process_name);
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      -- failed steps are handled in compose mail message procedure
      compose_sendemailmessage(l_run_id, 'FAIL', l_process_date, 'UDM_DAILY Load Data', l_process_name);
  end;

  procedure load_url_detail_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name     varchar2(100);
    l_cnt              number := 0;
    l_drop_date        number;
    l_add_date         number;
    l_min_partition    varchar2(55);
    l_new_partition    varchar2(55);
    l_partition_exists number;
  begin
    l_process_name := p_process_name || '-' ||
                      'url_detail_day';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      -- MAKE SURE PARTITIONS ARE STRAIGHT
      l_drop_date     := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 8, 'yyyymmdd'));
      l_add_date      := to_number(to_char(to_date(p_process_date, 'yyyymmdd') + 1, 'yyyymmdd'));
      l_min_partition := 'P_' || l_drop_date;
      l_new_partition := 'P_' || p_process_date;
      -- DROP OLDEST PARTITION IF IT EXISTS
      select count(1)
      into   l_partition_exists
      from   dba_tab_partitions
      where  table_name = 'URL_DETAIL_DAY'
      and    partition_name = l_min_partition;
      if l_partition_exists > 0 then
        execute immediate 'alter table url_detail_day drop partition ' ||
                          l_min_partition || ' ';
      end if;
      -- CREATE NEWEST PARTITION IF IT'S NOT THERE ALREADY
      select count(1)
      into   l_partition_exists
      from   dba_tab_partitions
      where  table_name = 'URL_DETAIL_DAY'
      and    partition_name = l_new_partition;
      if l_partition_exists = 0 then
        execute immediate 'alter table url_detail_day add partition ' ||
                          l_new_partition ||
                          ' values less than (' ||
                          l_add_date ||
                          ') tablespace TBL_UDM_DAY';
      end if;
      execute immediate 'Alter table url_detail_day truncate partition P_' ||
                        p_process_date;
      insert /*+ APPEND */
      into url_detail_day
        (affiliate_id, url_enc, page_url, domain, full_dns,
         date_id, page_rank, category, context,
         all_impressions, sad_impressions, page_views,
         tile_count, ad_size_1, ad_size_2, ad_size_3,
         ad_size_4, ad_size_5, ad_size_6, ad_size_7,
         ad_size_8, ad_size_9, ad_size_10, ad_size_11,
         ad_size_12, ad_size_13, ad_size_14, ad_size_15,
         ad_size_16, ad_size_17, ad_size_19, ad_size_20,
         ad_size_21, ad_size_23, ad_size_24, ad_size_25,
         ad_size_26, ad_size_27, ad_size_28, ad_size_29,
         ad_size_30, ad_size_31)
        select affiliate_id, url_enc, page_url, domain,
               full_dns, date_id, page_rank, category,
               context, all_impressions, sad_impressions,
               page_views, tile_count, ad_size_1, ad_size_2,
               ad_size_3, ad_size_4, ad_size_5, ad_size_6,
               ad_size_7, ad_size_8, ad_size_9, ad_size_10,
               ad_size_11, ad_size_12, ad_size_13,
               ad_size_14, ad_size_15, ad_size_16,
               ad_size_17, ad_size_19, ad_size_20,
               ad_size_21, ad_size_23, ad_size_24,
               ad_size_25, ad_size_26, ad_size_27,
               ad_size_28, ad_size_29, ad_size_30,
               ad_size_31
        from   udm_data.url_detail_day_new@etlsga;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table url_detail_day partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_url_combined
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'url_detail_combined';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table url_detail_combined';
      insert /*+ APPEND */
      into url_detail_combined
        (affiliate_id, url_enc, page_url, domain, full_dns,
         first_date_id, last_date_id, day_count, page_rank,
         category, context, all_impressions, sad_impressions,
         page_views, tile_count, ad_size_1, ad_size_2,
         ad_size_3, ad_size_4, ad_size_5, ad_size_6,
         ad_size_7, ad_size_8, ad_size_9, ad_size_10,
         ad_size_11, ad_size_12, ad_size_13, ad_size_14,
         ad_size_15, ad_size_16, ad_size_17, ad_size_19,
         ad_size_20, ad_size_21, ad_size_23, ad_size_24,
         ad_size_25, ad_size_26, ad_size_27, ad_size_28,
         ad_size_29, ad_size_30, ad_size_31)
        select /*+ PARALLEL(A,5,1) */
         affiliate_id, url_enc, page_url, domain, full_dns,
         min(date_id) first_date_id,
         max(date_id) last_date_id,
         count(distinct(date_id)) day_count, page_rank,
         category, context,
         sum(all_impressions) all_impressions,
         sum(sad_impressions) sad_impressions,
         sum(page_views) page_views,
         max(tile_count) tile_count,
         sum(ad_size_1) ad_size_1, sum(ad_size_2) ad_size_2,
         sum(ad_size_3) ad_size_3, sum(ad_size_4) ad_size_4,
         sum(ad_size_5) ad_size_5, sum(ad_size_6) ad_size_6,
         sum(ad_size_7) ad_size_7, sum(ad_size_8) ad_size_8,
         sum(ad_size_9) ad_size_9,
         sum(ad_size_10) ad_size_10,
         sum(ad_size_11) ad_size_11,
         sum(ad_size_12) ad_size_12,
         sum(ad_size_13) ad_size_13,
         sum(ad_size_14) ad_size_14,
         sum(ad_size_15) ad_size_15,
         sum(ad_size_16) ad_size_16,
         sum(ad_size_17) ad_size_17,
         sum(ad_size_19) ad_size_19,
         sum(ad_size_20) ad_size_20,
         sum(ad_size_21) ad_size_21,
         sum(ad_size_23) ad_size_23,
         sum(ad_size_24) ad_size_24,
         sum(ad_size_24) ad_size_25,
         sum(ad_size_24) ad_size_26,
         sum(ad_size_24) ad_size_27,
         sum(ad_size_24) ad_size_28,
         sum(ad_size_24) ad_size_29,
         sum(ad_size_24) ad_size_30,
         sum(ad_size_24) ad_size_31
        from   url_detail_day a
        where  date_id >=
               to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 7, 'yyyymmdd'))
        and    date_id <= p_process_date
        group  by affiliate_id, url_enc, page_url, domain,
                  full_dns, page_rank, category, context
        order  by affiliate_id, page_url, sad_impressions,
                  page_views;
      commit;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table url_detail_combined estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure create_url_file_new
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    cursor c_data is
      select affiliate_id, url_enc, page_url
      from   url_detail_combined;
    l_file         utl_file.file_type;
    l_name         varchar2(30) := 'url_' || p_process_date;
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'create_url_file_new';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      l_file := utl_file.fopen('CRAW_DIR', l_name, 'w', 32767);
      for cur_rec in c_data loop
        utl_file.put_line(l_file, cur_rec.affiliate_id || '^' ||
                           cur_rec.url_enc || '^' ||
                           cur_rec.page_url);
        l_cnt := l_cnt + 1;
      end loop;
      utl_file.fclose(l_file);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_affl_list_data is
  begin
    null;
    /*delete from campaign.lm_affiliate_list_data@adq;
    insert into campaign.lm_affiliate_list_data@adq
      select object_2 as affiliate_id, object_1 as list_id
      from   list_data.list_element@nap
      where  object_2_type = 35
      and    object_1 in
             (select id
               from   list_data.list@nap
               where  list_type = 1
               and    publish_status = 5);
    commit;
    delete from campaign.lm_affiliate_list@adq;
    insert into campaign.lm_affiliate_list@adq
      select a.id, name, internal_name, key_values, tag,
             adq_name, adq_show
      from   list_data.list@nap a, list_data.list_tags@nap b
      where  list_type = 1
      and    a.id = b.list_id(+)
      and    publish_status = 5;
    commit;
    delete from campaign.lm_affiliate_map@adq;
    insert into campaign.lm_affiliate_map@adq
      (aff_list_col_index, aff_list_disp_col)
      select rownum, adq_name
      from   (select distinct adq_name
               from   campaign.lm_affiliate_list@adq
               where  adq_name is not null
               order  by adq_name);
    commit;*/
  end;

  procedure compose_sendemailmessage
  (
    p_run_id       number,
    p_stage        varchar2,
    p_process_date number,
    p_message      varchar2,
    p_process_name varchar2
  ) is
    l_sender varchar2(100) := 'odsimportreport@glam.com';
    --l_to     varchar2(1000) := 'odsimportreport@glam.com';
    l_to         varchar2(1000) := 'nileshm@glam.com,bryanb@glam.com,sahilt@glam.com';
    l_subject    varchar2(1000) := '';
    l_today      varchar2(8);
    l_crlf       varchar2(2) := chr(13) || chr(10);
    l_hrs        number;
    l_minutes    number;
    l_sec        number;
    l_start_time date;
    l_end_time   date;
    l_complete   number;
    l_message    varchar2(32000);
    l_error_msg  varchar2(4000);
  begin
    l_today := to_char(sysdate, 'yyyymmdd');
    select upper(sys_context('USERENV', 'DB_NAME')) || ': ' ||
            p_message || ' Date: ' || p_process_date || ' ' ||
            p_process_name || ' -> ETL RUN_ID : ' ||
            p_run_id
    into   l_subject
    from   dual;
    if (upper(p_stage) = 'START') then
      l_message := l_crlf || 'Hello ,' || l_crlf || l_crlf ||
                   p_message ||
                   ' load process has Started for ' ||
                   p_process_date || l_crlf ||
                   'Please restrain from running any reports and queries. ' ||
                   l_crlf ||
                   'You will be notified once the process has been completed.';
    elsif upper(p_stage) = 'COMPLETE' then
      select min(start_time), max(end_time)
      into   l_start_time, l_end_time
      from   daily_process_log
      where  run_id = p_run_id
      and    status = 'COMPLETE';
      select count(1)
      into   l_complete
      from   daily_process_log
      where  run_id = p_run_id
      and    status = 'COMPLETE';
      l_hrs     := trunc(((86400 *
                         (l_end_time - l_start_time)) / 60) / 60) -
                   24 *
                   (trunc((((86400 *
                          (l_end_time - l_start_time)) / 60) / 60) / 24));
      l_minutes := trunc((86400 *
                         (l_end_time - l_start_time)) / 60) -
                   60 *
                   (trunc(((86400 *
                          (l_end_time - l_start_time)) / 60) / 60));
      l_sec     := trunc(86400 *
                         (l_end_time - l_start_time)) -
                   60 *
                   (trunc((86400 *
                          (l_end_time - l_start_time)) / 60));
      l_message := l_crlf || 'Hello ,' || l_crlf || l_crlf ||
                   p_message || ' details are as follows: ' ||
                   l_crlf || l_crlf || 'Job Run ID: ' ||
                   p_run_id || l_crlf || 'Data Date : ' ||
                   p_process_date || l_crlf ||
                   'Job Started: ' ||
                   to_char(l_start_time, ' dd Mon YYYY hh24:mi:ss') ||
                   l_crlf || 'Job Ended  : ' ||
                   to_char(l_end_time, ' dd Mon YYYY hh24:mi:ss') ||
                   l_crlf ||
                   'Duration process time (In Hrs) : ' ||
                   l_hrs || l_crlf ||
                   'Duration process time (In Minutes) : ' ||
                   l_minutes || l_crlf ||
                   'Duration process time (In Seconds) : ' ||
                   l_sec || l_crlf ||
                   'Total Time (Hr:Min:Sec) :' || l_hrs || ':' ||
                   l_minutes || ':' || l_sec;
      l_message := l_message || l_crlf || l_crlf ||
                   'Steps Completed: ' || l_complete ||
                   l_crlf || 'All jobs ran successfully ' ||
                   l_crlf || l_crlf || '- Data Team';
      l_subject := l_subject || ' ,Successful on ' ||
                   l_today;
    elsif upper(p_stage) = 'FAIL' then
      select error_reason
      into   l_error_msg
      from   daily_process_log
      where  run_id = p_run_id
      and    process_date = p_process_date
      and    upper(process_name) like
             upper(p_process_name) || '-RUN_UDM%';
      l_message := l_crlf || 'Hello ,' || l_crlf || l_crlf ||
                   p_message || ' details are as follows: ' ||
                   l_crlf || l_crlf || 'Job Run ID : ' ||
                   p_run_id || l_crlf || l_crlf ||
                   'Data Date : ' || p_process_date ||
                   l_crlf || l_crlf || ' Failed on : ' ||
                   l_today || l_crlf || l_crlf ||
                   ' Failed Reason : ' || l_error_msg ||
                   l_crlf || l_crlf || '- Data Team';
      l_subject := l_subject || ' Failed on ' || l_today;
    end if;
    pa_send_email.mail(l_sender, l_to, l_subject, l_message);
  end;

end pkg_udm_daily;
/
