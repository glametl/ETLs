create or replace package pkg_campaign_uniques as

  procedure run_campaign_uniques
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_fact_media_user_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_bt_visits
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_bt_monthly_raw
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_user_audit
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_user_site_geo
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure replicate_uniques_to_adq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_user_site_geo_monthly
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure fact_media_monthly_stg_v1
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_campaign_uniques as

  procedure run_campaign_uniques
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
    l_start_process := l_process_name || '-run_uniques';
    l_mail_msg      := 'Campaign Uniques Data';
    l_to            := 'odsimportreport@glam.com';
    if p_process_date is null then
      l_process_date := to_number(to_char(sysdate - 1, 'yyyymmdd'));
    elsif p_process_date is not null then
      l_process_date := p_process_date;
    end if;
    select max(run_id)
    into   l_run_id
    from   daily_process_log
    where  process_date = p_process_date
    and    upper(process_name) = upper(l_start_process);
    if l_run_id is null then
      l_run_id := seq_daily_campaign_run.nextval;
    end if;
    -- make sure we delete the record for the main process name(this has dependency in shell script).
    delete from daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) = upper(l_start_process)
    and    run_id = l_run_id
    and    status in ('RUNNING', 'FAIL');
    commit;
    -- send intimation mail for starting of process
    pa_send_email1.compose_email(l_run_id, 'START', l_process_date, l_mail_msg, l_start_process, l_to);
    -- log starting of the process
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'I');
    -- populate the base table first 
    load_fact_media_user_day(l_run_id, l_process_date, l_process_name);
    -- other agg tables based on base table
    load_fact_bt_visits(l_run_id, l_process_date, l_process_name);
    load_fact_bt_monthly_raw(l_run_id, l_process_date, l_process_name);
    load_fact_user_audit(l_run_id, l_process_date, l_process_name);
    -- replicate the agg tables to ADQ schema
    replicate_uniques_to_adq(l_run_id, l_process_date, l_process_name);
    /*if upper(p_process_name) = 'REPROCESS' then
      load_fact_user_site_geo(l_run_id, l_process_date, l_process_name);
      --Nilesh Commented this thing to have monthly table created first
      --fact_media_monthly_stg_v1(l_run_id, l_process_date, l_process_name);
    end if;*/
    /*if to_char(trunc(to_date(l_process_date, 'yyyymmdd'), 'dd'), 'dd') = '01' and
       upper(p_process_name) = 'PROCESS' then
      load_user_site_geo_monthly(l_run_id, l_process_date, l_process_name);
    end if;*/
    -- mark the process as complete in log table
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'UC');
    -- send completion mail
    pa_send_email1.compose_email(l_run_id, 'COMPLETE', l_process_date, l_mail_msg, l_start_process, l_to);
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      -- failed steps are handled in compose mail message procedure
      pa_send_email1.compose_email(l_run_id, 'FAIL', l_process_date, l_mail_msg, l_start_process, l_to);
  end;

  procedure load_fact_media_user_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_dedup_table  varchar2(100);
  begin
    l_dedup_table := 'ga_imp_clk_' || p_process_name || '_' ||
                     p_process_date;
    /* l_process_name := p_process_name || '-' ||
                      'fact_media_user_day';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_media_user_day truncate partition P_' ||
                        p_process_date;
      execute immediate '
        insert \*+ append *\
        into fact_media_user_day
          (user_id, date_id, advertiser_id, order_id, ad_id,
           site_id, affiliate_id, dma_id, country_id,
           state_province, city_id, ga_city, zip_code,
           browser_id, os_id, connection_type_id, page_id,
           clicks, impressions, source_id, tile, agent_type,
           internal_ip, flags, creative_id, ga_os_id,
           ad_group_id, nad_dimension, country_code, 
           ip, ga_browser_id)
          select \*+ parallel (a,5,1) *\
               user_id, date_id, advertiser_id, order_id, 
               ad_id, site_id, affiliate_id, dma_id, country_id,
               state_province, -1 as city_id, city_id as ga_city, 
               to_char(regexp_substr(zip_code, ''^[0-9]{5}'')) zip_code,
               browser_id, os_id, connection_type_id,
               coalesce(case
                          when decode(bitand(flags, 32), 32, 1, 0) = 0 and
                               decode(bitand(flags, 16), 16, 1, 0) = 0 then
                           -20
                          when decode(bitand(flags, 32), 32, 1, 0) = 0 and
                               decode(bitand(flags, 16), 16, 1, 0) = 1 then
                           -21
                          when decode(bitand(flags, 32), 32, 1, 0) = 1 and
                               decode(bitand(flags, 16), 16, 1, 0) = 0 then
                           -22
                          when decode(bitand(flags, 32), 32, 1, 0) = 1 and
                               decode(bitand(flags, 16), 16, 1, 0) = 1 then
                           -23
                        end, -20) as page_id, 
               sum(clicks) as clicks, sum(impressions) impressions,
               request_status as source_id, tile, agent_type, internal_ip, 
               flags, creative_id, ga_os_id, ad_group_id, 
               nad_dimension, country_code, ip, ga_browser_id
          \*from   adaptive_data.' ||
                        l_dedup_table ||
                        ' a*\
          from   ga_imp_clk a
          where  date_id = ' ||
                        p_process_date || '
          and    process_name = lower(''' ||
                        p_process_name || ''')
          group  by user_id, date_id, affiliate_id, browser_id, os_id,
                    connection_type_id, advertiser_id, order_id,
                    ad_id, site_id, dma_id, country_id, state_province, 
                    city_id, to_char(regexp_substr(zip_code, ''^[0-9]{5}'')), 
                    tile, agent_type, internal_ip, flags, request_status,
                    creative_id, ga_os_id, ad_group_id,
                    nad_dimension, country_code, ip, ga_browser_id';*/
    --
    l_process_name := p_process_name || '-' ||
                      'fact_media_user_day';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_media_user_day truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into fact_media_user_day
        (user_id, date_id, advertiser_id, order_id, ad_id,
         site_id, affiliate_id, dma_id, country_id,
         state_province, city_id, ga_city, zip_code,
         browser_id, os_id, connection_type_id, page_id,
         clicks, impressions, source_id, tile, agent_type,
         internal_ip, flags, creative_id, ga_os_id,
         ad_group_id, nad_dimension, country_code, ip,
         ga_browser_id)
        select /*+ parallel (a,5,1) */
         user_id, date_id, advertiser_id, order_id, ad_id,
         site_id, affiliate_id, dma_id, country_id,
         state_province, -1 as city_id, city_id as ga_city,
         to_char(regexp_substr(zip_code, '^[0-9]{5}')) zip_code,
         browser_id, os_id, connection_type_id,
         coalesce(case
                    when decode(bitand(flags, 32), 32, 1, 0) = 0 and
                         decode(bitand(flags, 16), 16, 1, 0) = 0 then
                     -20
                    when decode(bitand(flags, 32), 32, 1, 0) = 0 and
                         decode(bitand(flags, 16), 16, 1, 0) = 1 then
                     -21
                    when decode(bitand(flags, 32), 32, 1, 0) = 1 and
                         decode(bitand(flags, 16), 16, 1, 0) = 0 then
                     -22
                    when decode(bitand(flags, 32), 32, 1, 0) = 1 and
                         decode(bitand(flags, 16), 16, 1, 0) = 1 then
                     -23
                  end, -20) as page_id,
         sum(clicks) as clicks, sum(impressions) impressions,
         request_status as source_id, tile, agent_type,
         internal_ip, flags, creative_id, ga_os_id,
         ad_group_id, nad_dimension, country_code, ip,
         ga_browser_id
        from   ga_imp_clk a
        where  date_id = p_process_date
        and    process_name = lower(p_process_name)
        group  by user_id, date_id, affiliate_id, browser_id,
                  os_id, connection_type_id, advertiser_id,
                  order_id, ad_id, site_id, dma_id,
                  country_id, state_province, city_id,
                  to_char(regexp_substr(zip_code, '^[0-9]{5}')),
                  tile, agent_type, internal_ip, flags,
                  request_status, creative_id, ga_os_id,
                  ad_group_id, nad_dimension, country_code,
                  ip, ga_browser_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_media_user_day partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_fact_bt_visits
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_bt_visits_new';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('PROCESS') then
        for i in (select partition_name
                  from   user_tab_partitions
                  where  table_name = 'FACT_BT_VISITS_NEW'
                  and    to_number(replace(partition_name, 'P_', '')) <
                         to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 45, 'yyyymmdd'))
                  order  by partition_position) loop
          begin
            execute immediate 'Alter table fact_bt_visits_new drop partition ' ||
                              i.partition_name;
          exception
            when others then
              null;
          end;
        end loop;
      end if;
      execute immediate 'Alter table fact_bt_visits_new truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into fact_bt_visits_new
        (date_id, user_id, site_id, affiliate_id,
         page_visits, source_id, ga_os_id)
        select /*+ parallel (a,5,1) */
         date_id, user_id, site_id, affiliate_id,
         count(1) as page_visits, 1 as source_id, ga_os_id
        from   fact_media_user_day a
        where  date_id = p_process_date
        and    source_id != 0
        and    tile = 1
        group  by date_id, user_id, site_id, affiliate_id,
                  ga_os_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_bt_visits_new partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_fact_bt_monthly_raw
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_bt_monthly_raw';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('PROCESS') then
        for i in (select partition_name
                  from   user_tab_partitions
                  where  table_name = 'FACT_BT_MONTHLY_RAW'
                  and    to_number(replace(partition_name, 'P_', '')) <
                         to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 45, 'yyyymmdd'))
                  order  by partition_position) loop
          begin
            execute immediate 'Alter table fact_bt_monthly_raw drop partition ' ||
                              i.partition_name;
          exception
            when others then
              null;
          end;
        end loop;
      end if;
      execute immediate 'Alter table fact_bt_monthly_raw truncate partition P_' ||
                        p_process_date;
      insert /*+append */
      into fact_bt_monthly_raw
        (date_id, ad_id, user_id, country_id, impressions,
         clicks, order_id, country_code)
        select /*+ parallel(a,5,1) */
         p_process_date as date_id, ad_id, user_id,
         country_id, sum(impressions) as impressions,
         sum(clicks) as clicks, order_id, country_code
        from   campaign.fact_media_user_day a
        where  date_id = p_process_date
        group  by user_id, country_id, ad_id, order_id,
                  country_code;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_bt_monthly_raw partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_fact_user_audit
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_user_audit_new';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_user_audit_new truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into fact_user_audit_new
        (date_id, user_id, affiliate_id, source_id, imps,
         clicks, agent_type, internal_ip)
        select /*+ parallel(a,5,1) */
         a.date_id, a.user_id, a.affiliate_id, a.source_id,
         sum(a.impressions) imps, sum(a.clicks) clicks,
         a.agent_type, a.internal_ip
        from   campaign.fact_media_user_day a
        where  a.date_id = p_process_date
        and    nad_dimension in
               ('160x600', '300x250', '300x600', '728x90', '800x150', '630x150', '270x150')
        group  by a.date_id, a.user_id, a.affiliate_id,
                  a.source_id, a.agent_type, a.internal_ip;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_user_audit_new partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_fact_user_site_geo
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_user_site_geo_daily';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      /*execute immediate 'Alter table fact_user_site_geo_daily truncate partition P_' ||
                        p_process_date;
      insert \*+ append *\
      into fact_user_site_geo_daily
        (date_id, month_id, user_id, site_id, affiliate_id,
         country_id, state_province, dma_id, city_id,
         source_id, internal_ip, agent_type, imps)
        select \*+ parallel (a,5,1) *\
         p_process_date,
         to_number(substr(p_process_date, 1, 6)), user_id,
         site_id, affiliate_id, country_id, state_province,
         dma_id, city_id, source_id, internal_ip, agent_type,
         sum(impressions)
        from   fact_media_user_day a
        where  date_id = p_process_date
        and    advertiser_id <> 0
        and    order_id <> 0
        and    nad_dimension in
               ('120x60', '120x600', '125x125', '160x160', '160x300', '160x600', '224x126', '240x135', '270x150', '280x330', '300x250', '300x50', '300x600', '320x48', '320x50', '336x280', '444x1', '444x10', '444x2', '444x3', '468x60', '480x270', '555x2', '630x150', '650x35', '728x90', '728x91', '800x150', '800x50', '888x11', '888x12', '888x18', '888x21', '928x60', '968x16', '968x90', '970x66', '984x258', '990x26', '444x160', '444x300', '444x728')
        and    to_char(user_id) not like '22%22'
        group  by user_id, site_id, affiliate_id, country_id,
                  state_province, dma_id, city_id, source_id,
                  internal_ip, agent_type;*/
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure replicate_uniques_to_adq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_sql          varchar2(4000);
  begin
    -- fact_user_audit_new
    l_process_name := p_process_name || '-' ||
                      'fact_user_audit_new@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        --campaign.pkg_generic_ods.truncate_partition@etladq(p_process_date, 'campaign', 'fact_user_audit_new');
        campaign.pkg_generic_ddl.truncate_partition@etladq('campaign', 'fact_user_audit_new', 'P_' ||
                                                            p_process_date);
        l_sql := 'insert /*+ append */ into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql ||
               'campaign.fact_user_audit_new@etladq
        select /*+ PARALLEL (a,3,1) */
         *
        from   campaign.fact_user_audit_new a
        where  date_id = :p_process_date';
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_user_audit_new', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure fact_media_monthly_stg_v1
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
         upper(p_process_name) = 'Reprocess' then
        execute immediate 'Truncate table fact_media_user_day_agg';
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
        where  a.date_id = p_process_date
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

  procedure load_user_site_geo_monthly
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name  varchar2(100);
    l_cnt           number := 0;
    l_start_date_id number;
    l_end_date_id   number;
    l_process_date  number;
    l_month_id      number;
  begin
    l_process_date  := to_number(to_char(add_months(to_date(p_process_date, 'yyyymmdd'), -1), 'yyyymmdd'));
    l_start_date_id := to_number(to_char(trunc(to_date(l_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'));
    l_end_date_id   := to_number(to_char(last_day(to_date(l_process_date, 'yyyymmdd')), 'yyyymmdd'));
    l_month_id      := to_number(substr(l_start_date_id, 1, 6));
    l_process_name  := p_process_name || '-' ||
                       'fact_user_site_geo_monthly';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_user_site_geo_monthly truncate partition P_' ||
                        l_month_id;
      insert /*+ append */
      into fact_user_site_geo_monthly
        (month_id, user_id, site_id, affiliate_id,
         country_id, state_province, dma_id, city_id,
         source_id, created_date, date_id, internal_ip,
         agent_type, imps)
        select /*+ parallel (a,5,1) */
         l_month_id, user_id, site_id, affiliate_id,
         country_id, upper(state_province), dma_id, city_id,
         source_id, sysdate, p_process_date, internal_ip,
         agent_type, sum(imps)
        from   fact_user_site_geo_daily a
        where  date_id >= l_start_date_id
        and    date_id <= l_end_date_id
        group  by user_id, site_id, affiliate_id, country_id,
                  upper(state_province), dma_id, city_id,
                  source_id, internal_ip, agent_type;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    l_process_name := p_process_name || '-' ||
                      'replicate_geo_monthly_to_BEN';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      insert /*+ append */
      into bodata.fact_user_site_geo_mthly@etlben
        (month_id, user_id, site_id, affiliate_id,
         country_id, state_province, dma_id, city_id,
         source_id, created_date, date_id, internal_ip,
         agent_type, imps)
        select /*+ parallel (a,5,1) */
         month_id, user_id, site_id, affiliate_id,
         country_id, state_province, dma_id, city_id,
         source_id, created_date, date_id, internal_ip,
         agent_type, imps
        from   fact_user_site_geo_monthly a
        where  month_id = l_month_id;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      -- send completion mail
      pa_send_email.mail('odsimportreport@glam.com', 'stevenl@glam.com,nileshm@glam.com,bryanb@glam.com', 'Monthly Uniques Data', 'USER_SITE_GEO_MONTHLY Uniques Data has been loaded to BEN.' ||
                          chr(10) ||
                          'Month Processed :' ||
                          to_char(to_date(l_start_date_id, 'yyyymmdd'), 'dd-MON-yyyy'));
    end if;
  end;

end;
/
