create or replace package pkg_monthly_uniques1 as

  procedure run_monthly
  (
    p_process_name varchar2,
    p_process_date number
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

  procedure load_media_user_monthly_new
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_site_geo_monthly
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_monthly_uniques1 as

  procedure run_monthly
  (
    p_process_name varchar2,
    p_process_date number
  ) is
    l_run_id        number;
    l_process_date  number;
    l_process_name  varchar2(100);
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
    if to_number(to_char(to_date(l_process_date, 'yyyymmdd'), 'dd')) in
       (9, 17, 25, 1) then
      l_process_date := case
                          when to_number(to_char(to_date(l_process_date, 'yyyymmdd'), 'dd')) in
                               (9, 17, 25) then
                           to_number(to_char(to_date(l_process_date, 'yyyymmdd') - 1, 'yyyymmdd'))
                          when to_number(to_char(to_date(l_process_date, 'yyyymmdd'), 'dd')) = 1 then
                           to_number(to_char(to_date(l_process_date, 'yyyymmdd') - 1, 'yyyymmdd'))
                        end;
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
      else
        -- set the main process status as Running sothat there are no multiple entries for the main process
        update daily_process_log
        set    process_date = l_process_date,
               start_time = sysdate, status = 'RUNNING',
               error_reason = '', end_time = ''
        where  run_id = l_run_id
        and    upper(process_name) = upper(l_start_process);
        commit;
      end if;
      -- send intimation mail for starting of process
      pa_send_email1.compose_email(l_run_id, 'START', l_process_date, l_mail_msg, l_start_process, l_to);
      load_media_user_week_agg(l_run_id, l_process_date, l_process_name);
      -- mark the process as complete in log table
      pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'UC');
      -- send completion mail
      pa_send_email1.compose_email(l_run_id, 'COMPLETE', l_process_date, l_mail_msg, l_start_process, l_to);
    end if;
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      -- failed steps are handled in compose mail message procedure
      pa_send_email1.compose_email(l_run_id, 'FAIL', l_process_date, l_mail_msg, l_start_process, l_to);
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
      --load_media_user_staging(p_run_id, p_process_date, p_process_name);
      --load_media_user_monthly(p_run_id, p_process_date, p_process_name);
      load_media_user_monthly_new(p_run_id, p_process_date, p_process_name);
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
                 nad_dimension, ip, country_code, userid_22_22
          from   fact_media_user_week_agg a
          group  by month_id, advertiser_id, order_id, ad_id,
                 user_id, site_id, affiliate_id, dma_id,
                 country_id, state_province, city_id, page_id,
                 source_id, agent_type, internal_ip, ga_city,
                 creative_id, ga_os_id, ga_browser_id, ad_group_id,
                 nad_dimension, ip, country_code, userid_22_22';
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
                      'fact_media_user_monthly_old';
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
           country_code, userid_22_22, ip
          from   monthly_userid_staging a';
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_media_user_monthly partition(P_' ||
                        substr(p_process_date, 1, 6) ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_media_user_monthly_new
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
         state_province, city_id, sum(clicks) clicks,
         sum(impressions) impressions, page_id, source_id,
         agent_type, internal_ip, ga_city, creative_id,
         ga_os_id, ga_browser_id, ad_group_id, nad_dimension,
         ip, country_code, userid_22_22
        from   fact_media_user_week_agg a
        group  by month_id, advertiser_id, order_id, ad_id,
                  user_id, site_id, affiliate_id, dma_id,
                  country_id, state_province, city_id,
                  page_id, source_id, agent_type,
                  internal_ip, ga_city, creative_id,
                  ga_os_id, ga_browser_id, ad_group_id,
                  nad_dimension, country_code, userid_22_22,
                  ip log errors
        into   err$_fact_media_user_monthly reject limit unlimited;
      execute immediate 'analyze table fact_media_user_monthly partition(P_' ||
                        substr(p_process_date, 1, 6) ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_site_geo_monthly
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_month_id     number;
  begin
    l_month_id     := to_number(substr(p_process_date, 1, 6));
    l_process_name := p_process_name || '-' ||
                      'fact_user_site_geo_monthly';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_user_site_geo_monthly truncate partition P_' ||
                        l_month_id;
      execute immediate 'insert /*+ append */
      into fact_user_site_geo_monthly
        (month_id, user_id, site_id, affiliate_id,
         country_id, state_province, dma_id, city_id,
         source_id, created_date, date_id, internal_ip,
         agent_type, imps)
        select /*+ parallel (a,5,1) */
         month_id, user_id, site_id, affiliate_id,
         country_id, upper(state_province), dma_id, city_id,
         source_id, sysdate, p_process_date, internal_ip,
         agent_type, sum(impressions)
        from   monthly_userid_staging a
        where  nad_dimension in
               (''120x60'', ''120x600'', ''125x125'', ''160x160'', ''160x300'', ''160x600'', ''224x126'', ''240x135'', ''270x150'', ''280x330'', ''300x250'', ''300x50'', ''300x600'', ''320x48'', ''320x50'', ''336x280'', ''444x1'', ''444x10'', ''444x2'', ''444x3'', ''468x60'', ''480x270'', ''555x2'', ''630x150'', ''650x35'', ''728x90'', ''728x91'', ''800x150'', ''800x50'', ''888x11'', ''888x12'', ''888x18'', ''888x21'', ''928x60'', ''968x16'', ''968x90'', ''970x66'', ''984x258'', ''990x26'', ''444x160'', ''444x300'', ''444x728'')
        and    to_char(user_id) not like ''22%22''
        group  by user_id, site_id, affiliate_id, country_id,
                  upper(state_province), dma_id, city_id,
                  source_id, internal_ip, agent_type';
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_user_site_geo_monthly partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

end;
/
