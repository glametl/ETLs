create or replace package bt_data.pkg_monthly_all_audience as

  /******************************************************************************
     NAME:       monthly_all_audience
     PURPOSE:
  
     REVISIONS:
     Ver        Date        Author             Description
     ---------  ----------  ---------------   ------------------------------------
     1.0        02/01/2011   Nilesh Mahajan    Created this package.
  ******************************************************************************/
  procedure run_all_audience(p_start_date_id number);

  procedure populate_raw_data
  (
    p_run_id        number,
    p_start_date_id number,
    p_end_date_id   number
  );

  procedure clear_tables_and_data
  (
    p_start_date_id number,
    p_country       varchar2
  );

  procedure load_generic_data
  (
    p_start_date_id number,
    p_end_date_id   number,
    p_country       varchar2
  );

  procedure compose_sendemailmessage
  (
    p_run_id       number,
    p_stage        varchar2,
    p_process_date number,
    p_message      varchar2,
    p_process_name varchar2
  );

end pkg_monthly_all_audience;
/
create or replace package body bt_data.pkg_monthly_all_audience as

  /******************************************************************************
     name:       monthly_all_audience
     purpose:
  
     revisions:
     ver        date        author             description
     ---------  ----------  ---------------   ------------------------------------
     1.0        02/01/2011   nilesh mahajan            created this package.
  ******************************************************************************/
  procedure run_all_audience(p_start_date_id number) as
    l_month_start number;
    l_month_end   number;
    l_run_id      number;
    l_error_msg   varchar2(4000);
  begin
    l_month_start := to_number(to_char(trunc(to_date(p_start_date_id, 'yyyymmdd'), 'mm'), 'yyyymmdd'));
    l_month_end   := to_number(to_char(last_day(to_date(p_start_date_id, 'yyyymmdd')), 'yyyymmdd'));
    select max(run_id)
    into   l_run_id
    from   daily_process_log
    where  process_date = l_month_start
    and    upper(process_name) =
           'PROCESS-MONTHLY_ALL_AUDIENCE';
    if l_run_id is null then
      l_run_id := bt_data.seq_daily_bt_run.nextval;
    end if;
    -- send intimation mail for starting of process
    compose_sendemailmessage(l_run_id, 'START', l_month_start, 'Monthly All Audience Load', 'PROCESS');
    -- log starting of the process
    pkg_log_process.log_process(l_run_id, l_month_start, 'Process-monthly_all_audience', 'I');
    populate_raw_data(l_run_id, l_month_start, l_month_end);
    -- mark the process as complete in log table
    pkg_log_process.log_process(l_run_id, l_month_start, 'Process-monthly_all_audience', 'UC');
    -- send completion mail
    compose_sendemailmessage(l_run_id, 'COMPLETE', l_month_start, 'Monthly All Audience Load', 'PROCESS');
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_month_start, '', 'UF', 0, l_error_msg);
      --send failuare mail
      compose_sendemailmessage(l_run_id, 'FAIL', l_month_start, 'Monthly All Audience Load', 'PROCESS');
  end;

  procedure populate_raw_data
  (
    p_run_id        number,
    p_start_date_id number,
    p_end_date_id   number
  ) as
    l_process_name varchar2(100);
    l_cnt          number;
  begin
    -- check the existance of the monthly score table
    execute immediate '
      select count(1) from monthly_score_' ||
                      p_start_date_id ||
                      ' where rownum < 10'
      into l_cnt;
    if l_cnt > 0 then
      for i in (select case
                          when country_abbrev = 'UK' then
                           '285,286,287,68'
                          else
                           '''' || country_id || ''''
                        end as country_id,
                       country_abbrev as country
                from   campaign.dim_countries@roods
                where  is_targeted = 1
                and    country_abbrev in
                       ('DE', 'UK', 'US', 'CA', 'JP')) loop
        l_process_name := i.country || '-ALL_AUDIENCE';
        if pkg_log_process.is_not_complete(p_run_id, p_start_date_id, l_process_name) then
          -- start actual process by making the log entry
          pkg_log_process.log_process(p_run_id, p_start_date_id, l_process_name, 'I');
          -- drop table all_audience_raw table if already processed
          begin
            execute immediate 'drop table bt_data.stg_all_audience_' ||
                              i.country || ' purge';
          exception
            when others then
              null;
          end;
          begin
            execute immediate 'drop table bt_data.all_audience_raw_user_' ||
                              i.country || ' purge';
          exception
            when others then
              null;
          end;
          execute immediate '
            create table bt_data.stg_all_audience_' ||
                            i.country ||
                            ' compress for all operations tablespace tbl_monthly_process 
                              parallel(degree 5 instances 1) nologging
                as               
                select /*+ PARALLEL( a,5,1) */
                   a.date_id, a.country_id, a.user_id, sum(a.impressions) as impressions,
                   sum(a.clicks) as clicks, 0 as data_source,
                   ''' ||
                            i.country ||
                            ''' as country
                from   monthly_campaign_raw_new a,
                       adm_ads nad
                where  a.date_id >= ' ||
                            p_start_date_id || '
                and    a.date_id <= ' ||
                            p_end_date_id || '
                /*and    a.country_id in (' ||
                            i.country_id || ')*/
                and    a.country_code in (''' ||
                            i.country || ''')
                and    nad.ad_id = a.ad_id
                and    nad.ad_size_id in
                        (''968x90'',''160x600'',''300x250'',''800x150'',''630x150'',
                         ''968x16'',''928x60'',''270x150'',''984x258'',''468x60'',''728x90'',
                         ''160x160'',''120x600'',''300x600'',''728x91'',''650x35'',''990x26'',
                         ''800x50'')
                and    nad.category not in
                       (''Internal'', ''Ad Server'')
                group by a.date_id, a.country_id, a.user_id ';
          -- GET RID OF CRAZY USERS
          execute immediate '
            create table bt_data.all_audience_raw_user_' ||
                            i.country ||
                            ' compress for all operations tablespace tbl_monthly_process 
                              parallel(degree 5 instances 1) nologging
                as
            select /*+ parallel(d,3,1) */ *
            from   bt_data.stg_all_audience_' ||
                            i.country || ' d
            where  user_id not in
                   (select /*+ parallel(a,3,1) */
                            user_id
                     from   bt_data.stg_all_audience_' ||
                            i.country || ' a
                     group  by user_id
                     having sum(impressions) > 1000)';
          --create country specific score table
          begin
            execute immediate 'drop table bt_data.monthly_score_' ||
                              i.country || ' purge';
          exception
            when others then
              null;
          end;
          execute immediate '
            create table bt_data.monthly_score_' ||
                            i.country ||
                            ' compress for all operations tablespace tbl_monthly_process 
                            parallel(degree 3 instances 1) nologging 
            as select /*+ parallel (a,3,1) */
                      user_id, bt_category_id, sum(bt_score) as bt_score
               from   bt_data.monthly_score_' ||
                            p_start_date_id || ' a
               where  user_id in
                       ( select /*+ parallel (b,3,1) */ distinct user_id
                         from   bt_data.all_audience_raw_user_' ||
                            i.country ||
                            ' b)                          
               group  by user_id, bt_category_id';
          -- delete data if already processed
          clear_tables_and_data(p_start_date_id, i.country);
          -- call procedure to populate the generic table data
          load_generic_data(p_start_date_id, p_end_date_id, i.country);
          pkg_log_process.log_process(p_run_id, p_start_date_id, l_process_name, 'UC');
        end if;
      end loop;
    end if;
  end populate_raw_data;

  procedure clear_tables_and_data
  (
    p_start_date_id number,
    p_country       varchar2
  ) is
    l_cnt number := 0;
  begin
    -- delete the data from generic tables if already processed
    select count(1)
    into   l_cnt
    from   all_audience_ic
    where  start_date_id = p_start_date_id
    and    country_code = p_country;
    if l_cnt > 0 then
      delete from all_audience_ic
      where  start_date_id = p_start_date_id
      and    country_code = p_country;
      commit;
      l_cnt := 0;
    end if;
    select count(1)
    into   l_cnt
    from   all_audience_segments
    where  date_id = p_start_date_id
    and    country = p_country;
    if l_cnt > 0 then
      delete from all_audience_segments
      where  date_id = p_start_date_id
      and    country = p_country;
      commit;
      l_cnt := 0;
    end if;
    select count(1)
    into   l_cnt
    from   all_audience_summary
    where  date_id = p_start_date_id
    and    country_code = p_country;
    if l_cnt > 0 then
      delete from all_audience_summary
      where  date_id = p_start_date_id
      and    country_code = p_country;
      commit;
    end if;
  end;

  procedure load_generic_data
  (
    p_start_date_id number,
    p_end_date_id   number,
    p_country       varchar2
  ) as
  begin
    -- calculate icat aggregate data
    execute immediate '
      insert into bt_data.all_audience_ic
        (start_date_id, bt_category_id, bt_category, uq_users,
         impressions, clicks, ctr, avg_score, time_stamp,
         country_code)
        select :p_start_date_id as start_date_id, a.bt_category_id,
               b.bt_category, uq_users, impressions, clicks, ctr,
               avg_score, sysdate as time_stamp,
               :p_country as country_code
        from   (select /*+ ORDERED PARALLEL( BUSS 4 4 ) PARALLEL( PUSR 4 4 ) */
                  trim(buss.bt_category_id) bt_category_id,
                  count(distinct(pusr.user_id)) uq_users,
                  sum(pusr.impressions) impressions,
                  sum(pusr.clicks) clicks, 0 as ctr,
                  avg(buss.bt_score) avg_score
                 from   bt_data.all_audience_raw_user_' ||
                      p_country ||
                      ' pusr,
                        bt_data.monthly_score_' ||
                      p_country ||
                      ' buss
                 where  buss.user_id = pusr.user_id
                 group  by trim(buss.bt_category_id)
                 --having sum(pusr.impressions) < 10000
                 ) a,
               bt_data.bt_taxonomy b
        where  a.bt_category_id = b.bt_category_id(+)
        order  by a.bt_category_id '
      using p_start_date_id, p_country;
    --calculate segmentwise aggregate data
    for segment_list in (select segmentation,
                                count(1) as i_cat_count
                         from   bt_data.bt_segmentation_taxonomy
                         where  upper(country) = p_country
                         and    display_type = 'DISPLAY'
                         group  by segmentation
                         order  by segmentation) loop
      execute immediate '
        insert into bt_data.all_audience_segments
          (date_id, country, segment, uq_users, impressions, clicks,
           visits, ctr, avg_impressions, avg_clicks, avg_visits,
           avg_score)
          with segment_score as
           (select /*+ PARALLEL( FBST 4 4 ) */
             user_id, :segment as segment, sum(bt_score) segment_sum
            from   bt_data.monthly_score_' ||
                        p_country ||
                        ' fbst
            where  fbst.bt_category_id in
                   (select bt_category_id
                    from   bt_segmentation_taxonomy
                    where  segmentation = :segment
                    and    display_type = ''DISPLAY''
                    and    country = :l_country)
            group  by user_id),
          final_data as
           (select /*+ ORDERED PARALLEL( BUSS 4 4 ) PARALLEL( PUSR 4 4 ) */
             buss.segment, buss.user_id,
             sum(impressions) as impressions, sum(clicks) as clicks,
             count(distinct(date_id)) as visits,
             sum(segment_sum) bt_score
            from   segment_score buss,
                   bt_data.all_audience_raw_user_' ||
                        p_country ||
                        ' pusr
            where  buss.user_id = pusr.user_id
            group  by buss.user_id, buss.segment
            having sum(impressions) < 10000)
          select /*+ PARALLEL( BUSC 4 4 ) */
           :p_start_date_id as date_id, :l_country as country,
           segment, count(distinct(user_id)) uq_users,
           sum(impressions) impressions, sum(clicks) clicks,
           sum(visits) visits,
           ((sum(clicks) / sum(impressions)) * 100) as ctr,
           avg(impressions) avg_impressions, avg(clicks) avg_clicks,
           avg(visits) avg_visits, avg(bt_score) avg_score
          from   final_data
          group  by segment'
        using segment_list.segmentation, segment_list.segmentation, p_country, p_start_date_id, p_country;
      commit;
    end loop;
    -- calculate summary totals
    execute immediate '
      insert into bt_data.all_audience_summary
        (date_id, unique_users, impressions, clicks, time_stamp,
         country_code)
        select /*+ parallel (pusr 4 4) */
         :p_start_date_id, count(distinct(user_id)) unique_users,
         sum(impressions) impressions, sum(clicks) clicks,
         sysdate as time_stamp, :p_country
        from   bt_data.all_audience_raw_user_' ||
                      p_country || ' pusr'
      using p_start_date_id, p_country;
    commit;
  end load_generic_data;

  procedure compose_sendemailmessage
  (
    p_run_id       number,
    p_stage        varchar2,
    p_process_date number,
    p_message      varchar2,
    p_process_name varchar2
  ) is
    l_sender     varchar2(100) := 'btimportreport@glam.com';
    l_to         varchar2(1000) := 'btimportreport@glam.com';
    l_subject    varchar2(1000) := '';
    l_message    varchar2(4000);
    l_today      varchar2(8);
    l_crlf       varchar2(2) := chr(13) || chr(10);
    l_hrs        number;
    l_minutes    number;
    l_sec        number;
    l_start_time date;
    l_end_time   date;
    l_complete   number;
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
      l_message := l_crlf || 'Hello ,' || l_crlf || l_crlf ||
                   p_message || ' details are as follows: ' ||
                   l_crlf || l_crlf || 'Job Run ID : ' ||
                   p_run_id || l_crlf || 'Data Date : ' ||
                   p_process_date || l_crlf || l_crlf ||
                   ' Failed on : ' || l_today || l_crlf ||
                   l_crlf || '- Data Team';
      l_subject := l_subject || ' Failed on ' || l_today;
    end if;
    pa_send_email.mail(l_sender, l_to, l_subject, l_message);
  end;

end pkg_monthly_all_audience;
/
