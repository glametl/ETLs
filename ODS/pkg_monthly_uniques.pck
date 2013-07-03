create or replace package pkg_monthly_uniques as

  procedure run_monthly
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_media_user_day_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_media_user_week_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_media_user_staging
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_media_user_monthly
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure compose_sendemailmessage
  (
    p_run_id       number,
    p_stage        varchar2,
    p_process_date number,
    p_message      varchar2,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_monthly_uniques as

  procedure run_monthly
  (
    p_process_name varchar2,
    p_process_date number
  ) is
    l_run_id        number;
    l_process_date  number;
    l_process_name  varchar2(10);
    l_error_msg     varchar2(4000);
    l_start_process varchar2(100);
    l_mail_msg      varchar2(100);
    l_to            varchar2(1000);
  begin
    l_process_name  := initcap(p_process_name);
    l_start_process := l_process_name || '-run_monthly';
    l_mail_msg      := 'Monthly Uniques Data';
    l_to            := 'nileshm@glam.com,sahilt@glam.com';
    if p_process_date is null then
      l_process_date := to_number(to_char(sysdate - 1, 'yyyymmdd'));
    elsif p_process_date is not null then
      l_process_date := p_process_date;
    end if;
    select max(run_id)
    into   l_run_id
    from   daily_process_log
    where  substr(process_date, 1, 6) =
           substr(l_process_date, 1, 6)
    and    upper(process_name) = upper(l_start_process);
    if l_run_id is null then
      l_run_id := seq_daily_campaign_run.nextval;
      -- log starting of the process
      pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'I');
    end if;
    -- send intimation mail for starting of process
    --compose_sendemailmessage(l_run_id, 'START', l_process_date, 'Monthly Uniques Data', l_process_name);
    pa_send_email1.compose_email(l_run_id, 'START', l_process_date, l_mail_msg, l_start_process, l_to);
    -- set the main process status as Running sothat there are no multiple entries for the main process
    update daily_process_log
    set    process_date = l_process_date,
           start_time = sysdate, status = 'RUNNING',
           error_reason = '', end_time = ''
    where  run_id = l_run_id
    and    upper(process_name) = upper(l_start_process);
    commit;
    --load_media_user_day_agg(l_run_id, l_process_date, l_process_name);
    if to_number(to_char(to_date(l_process_date, 'yyyymmdd'), 'dd')) in
       (8, 16, 24, to_number(to_char(last_day(to_date(l_process_date, 'yyyymmdd')), 'dd'))) then
      load_media_user_week_agg(l_run_id, l_process_date, l_process_name);
    end if;
    -- mark the process as complete in log table
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'UC');
    -- send completion mail
    --compose_sendemailmessage(l_run_id, 'COMPLETE', l_process_date, 'Monthly Uniques Data', l_process_name);
    pa_send_email1.compose_email(l_run_id, 'COMPLETE', l_process_date, l_mail_msg, l_start_process, l_to);
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      -- failed steps are handled in compose mail message procedure
      --compose_sendemailmessage(l_run_id, 'FAIL', l_process_date, 'Monthly Uniques Data', l_process_name);
      pa_send_email1.compose_email(l_run_id, 'FAIL', l_process_date, l_mail_msg, l_start_process, l_to);
  end;

  procedure load_media_user_day_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_day          varchar2(100);
  begin
    --
    l_day          := to_number(substr(p_process_date, -2, 2));
    l_process_name := p_process_name || '-' ||
                      'fact_media_user_day_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'dd'), 'dd') = '01' and
         upper(p_process_name) = 'REPROCESS' then
        execute immediate 'truncate table fact_media_user_day_agg';
        execute immediate 'analyze table fact_media_user_day_agg estimate statistics';
      end if;
      insert /*+ append */
      into fact_media_user_day_agg
        (date_id, dd_extract_date_id, advertiser_id,
         order_id, ad_id, user_id, site_id, affiliate_id,
         dma_id, country_id, state_province, city_id, clicks,
         impressions, page_id, source_id, agent_type,
         internal_ip, ga_city, creative_id, ga_os_id,
         ga_browser_id, ad_group_id, nad_dimension, ip)
        select /*+ parallel(a,5,1) */
         date_id, l_day as dd_extract_date_id, advertiser_id,
         order_id, ad_id, user_id, site_id, affiliate_id,
         dma_id, country_id, state_province, city_id,
         sum(clicks) as clicks,
         sum(impressions) as impressions, page_id, source_id,
         agent_type, internal_ip, ga_city, creative_id,
         ga_os_id, ga_browser_id, ad_group_id, nad_dimension,
         ip
        from   fact_media_user_day a
        where  date_id = p_process_date
        group  by date_id, advertiser_id, order_id, ad_id,
                  user_id, site_id, affiliate_id, dma_id,
                  country_id, state_province, city_id,
                  page_id, source_id, agent_type,
                  internal_ip, ga_city, creative_id,
                  ga_os_id, ga_browser_id, ad_group_id,
                  nad_dimension, ip;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_media_user_day_agg partition(P_' ||
                        l_day || ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_media_user_week_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_week_id      number;
    l_min_date     number;
  begin
    l_week_id  := case
                   to_number(to_char(to_date(p_process_date, 'yyyymmdd'), 'dd'))
                    when 8 then
                     1
                    when 16 then
                     2
                    when 24 then
                     3
                    when
                     to_number(to_char(last_day(to_date(p_process_date, 'yyyymmdd')), 'dd')) then
                     4
                  end;
    l_min_date := case
                    when l_week_id = 4 then
                     substr(p_process_date, 1, 6) || 25
                    else
                     to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 7, 'yyyymmdd'))
                  end;
    --p_process_date   l_process_date  week_id     l_min_date 
    --20121008           20121008      1           20121001
    --20121016           20121016      2           20121009
    --20121024           20121024      3           20121017   
    --20121030           20120930      4           20120925(by default when l_week_id = 4)
    --This step is to ensure that with each new month, whole table should be truncated for safe
    if l_week_id = 1 then
      l_process_name := p_process_name || '-' ||
                        'truncate_fact_media_user_week_agg';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        execute immediate 'truncate table fact_media_user_week_agg';
        execute immediate 'analyze table fact_media_user_week_agg estimate statistics';
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
    end if;
    l_process_name := p_process_name || '-' ||
                      'fact_media_user_week_agg_week_' ||
                      l_week_id;
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'alter table fact_media_user_week_agg truncate partition P_' ||
                        l_week_id;
      insert /*+ append */
      into fact_media_user_week_agg
        (week_id, month_id, advertiser_id, order_id, ad_id,
         user_id, site_id, affiliate_id, dma_id, country_id,
         state_province, city_id, clicks, impressions,
         page_id, source_id, agent_type, internal_ip,
         ga_city, creative_id, ga_os_id, ga_browser_id,
         ad_group_id, nad_dimension, country_code,
         userid_22_22, ip)
        select /*+ parallel(a,5,1) */
         l_week_id,
         to_number(to_char(to_date(p_process_date, 'yyyymmdd'), 'yyyymm')),
         advertiser_id, order_id, ad_id, user_id, site_id,
         affiliate_id, dma_id, country_id, state_province,
         city_id, sum(clicks) clicks,
         sum(impressions) impressions, page_id, source_id,
         agent_type, internal_ip, ga_city, creative_id,
         ga_os_id, ga_browser_id, ad_group_id, nad_dimension,
         decode(country_id, 256, 'US', 257, 'US', 35, 'CA', 12, 'AU', 68, 'UK', 86, 'DE', 114, 'JP', 78, 'FR', 'ROW'),
         case
           when user_id like '22%22' then
            1
           else
            0
         end, ip
        from   fact_media_user_day a
        where  date_id >= l_min_date
        and    date_id <= p_process_date
        group  by advertiser_id, order_id, ad_id, user_id,
                  site_id, affiliate_id, dma_id, country_id,
                  state_province, city_id, page_id,
                  source_id, agent_type, internal_ip,
                  ga_city, creative_id, ga_os_id,
                  ga_browser_id, ad_group_id, nad_dimension,
                  ip log errors
        into   err$_fact_media_user_week_agg reject limit unlimited;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_media_user_week_agg partition(P_' ||
                        l_week_id ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- make sure that you are passing the last date of the previous month i.e. l_process_date
    if l_week_id = 4 then
      load_media_user_staging(p_run_id, p_process_date, p_process_name);
      load_media_user_monthly(p_run_id, p_process_date, p_process_name);
    end if;
  end;

  procedure load_media_user_staging
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'monthly_userid_staging';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      begin
        execute immediate 'drop table monthly_userid_staging purge';
      exception
        when others then
          null;
      end;
      execute immediate '
        create table monthly_userid_staging 
               tablespace tbl_uniques_stg
               nologging
               compress for all operations
               parallel (degree 5 instances 1)
          as
          select /*+ parallel(a,5,1) */ 
                 month_id, advertiser_id, order_id, ad_id,
                 user_id, site_id, affiliate_id, dma_id,
                 country_id, state_province, city_id, sum(clicks) clicks,
                 sum(impressions) impressions, page_id,
                 source_id, agent_type, internal_ip, ga_city,
                 creative_id, ga_os_id, ga_browser_id, ad_group_id,
                 nad_dimension, ip
          from   fact_media_user_week_agg a
          group  by month_id, advertiser_id, order_id, ad_id, user_id,
                    site_id, affiliate_id, dma_id, country_id,
                    state_province, city_id, page_id, source_id, agent_type,
                    internal_ip, ga_city, creative_id, ga_os_id,
                    ga_browser_id, ad_group_id, nad_dimension, ip';
      execute immediate 'analyze table monthly_userid_staging estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_media_user_monthly
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_media_user_monthly';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_media_user_monthly truncate partition P_' ||
                        substr(p_process_date, 1, 6);
      execute immediate '
        insert /*+ append */
        into fact_media_user_monthly
          (month_id, advertiser_id, order_id, ad_id, user_id,
           site_id, affiliate_id, dma_id, country_id,
           state_province, city_id, clicks, impressions,
           page_id, source_id, agent_type, internal_ip,
           ga_city, creative_id, ga_os_id, ga_browser_id,
           ad_group_id, nad_dimension, country_code,
           userid_22_22, ip)
          select /*+ parallel(a,5,1) */
           month_id, advertiser_id, order_id, ad_id, user_id,
           site_id, affiliate_id, dma_id, country_id,
           state_province, city_id, clicks, impressions,
           page_id, source_id, agent_type, internal_ip,
           ga_city, creative_id, ga_os_id, ga_browser_id,
           ad_group_id, nad_dimension, 
           decode(country_id, 256, ''US'', 257, ''US'', 35, ''CA'', 12, ''AU'', 68, ''UK'', 86, ''DE'', 114, ''JP'', 78, ''FR'', ''ROW''),
           case
             when user_id like ''22%22'' then
              1
             else
              0
           end userid_22_22, ip
          from   monthly_userid_staging a';
      /*insert \*+ append *\
      into fact_media_user_monthly
        (month_id, advertiser_id, order_id, ad_id, user_id,
         site_id, affiliate_id, dma_id, country_id,
         state_province, city_id, clicks, impressions,
         page_id, source_id, agent_type, internal_ip,
         ga_city, creative_id, ga_os_id, ga_browser_id,
         ad_group_id, nad_dimension, country_code,
         userid_22_22, ip)
        select \*+ parallel(a,5,1) *\
         month_id, advertiser_id, order_id, ad_id, user_id,
         site_id, affiliate_id, dma_id, country_id,
         state_province, city_id, sum(clicks) clicks,
         sum(impressions) impressions, page_id, source_id,
         agent_type, internal_ip, ga_city, creative_id,
         ga_os_id, ga_browser_id, ad_group_id, nad_dimension,
         country_code, userid_22_22, ip
        from   fact_media_user_week_agg a
        group  by month_id, advertiser_id, order_id, ad_id,
                  user_id, site_id, affiliate_id, dma_id,
                  country_id, state_province, city_id,
                  page_id, source_id, agent_type,
                  internal_ip, ga_city, creative_id,
                  ga_os_id, ga_browser_id, ad_group_id,
                  nad_dimension, country_code, userid_22_22,
                  ip;*/
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_media_user_monthly partition(P_' ||
                        substr(p_process_date, 1, 6) ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
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
    l_to         varchar2(1000) := 'nileshm@glam.com,sahilt@glam.com';
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
             upper(p_process_name) || '-RUN_MONTHLY%';
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

end;
/
