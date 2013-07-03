create or replace package pkg_campaign_all as

  procedure run_campaign
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_ga_data_copy
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ga_data_copy1
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_media_day_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_adq_report_tables
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure replicate_adq_report_tables
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2,
    p_schema_name  varchar2
  );

  procedure load_fact_media_camp_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ad_cumulative_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_ad_month_to_date
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_homepage
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ad_aff_coun_agg_video
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_adq_ad_osi
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure report_tables_to_adq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure replicate_to_adq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure replicate_to_ben
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_oms_product
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_lam_average_ctr
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_campaign_all as

  procedure run_campaign
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
    l_max_date      number;
  begin
    l_process_name  := initcap(p_process_name);
    l_start_process := l_process_name || '-run_campaign';
    l_mail_msg      := 'Campaign All Data';
    l_to            := 'odsimportreport@glam.com';
    if p_process_date is null then
      l_process_date := to_number(to_char(sysdate - 1, 'yyyymmdd'));
    elsif p_process_date is not null then
      l_process_date := p_process_date;
    end if;
    select max(run_id)
    into   l_run_id
    from   daily_process_log
    where  process_date = l_process_date
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
    load_ga_data_copy(l_run_id, l_process_date, l_process_name);
    --load_ga_data_copy1(l_run_id, l_process_date, l_process_name);
    load_fact_media_day_agg(l_run_id, l_process_date, l_process_name);
    -- other agg tables based on base table
    load_adq_report_tables(l_run_id, l_process_date, l_process_name);
    load_fact_media_camp_agg(l_run_id, l_process_date, l_process_name);
    -- replicate report tables to ADQ schema
    report_tables_to_adq(l_run_id, l_process_date, l_process_name);
    -- send completion mail
    pa_send_email.mail('odsimportreport@glam.com', 'india-eng-qa@glam.com', l_mail_msg, 'Campaign ADQ ETL is completed.' ||
                        chr(10) ||
                        'Date_id :' ||
                        to_char(to_date(l_process_date, 'yyyymmdd'), 'dd-MON-yyyy'));
    load_adq_ad_osi(l_run_id, l_process_date, l_process_name);
    -- load OMS product for process only
    if upper(p_process_name) = 'PROCESS' then
      load_lam_average_ctr(l_run_id, l_process_date, l_process_name);
      load_oms_product(l_run_id, l_process_date, l_process_name);
    end if;
    -- fact_media_day procedure moved to evolution ETL 
    if upper(p_process_name) = 'MANUAL' or
       (upper(p_process_name) = 'REPROCESS' and
        p_process_date <>
        to_number(to_char(sysdate - 1, 'yyyymmdd'))) then
      for i in (select date_id
                from   dim_date
                where  date_id >= l_process_date
                and    date_id <=
                       to_number(to_char(sysdate - 1, 'yyyymmdd'))
                order  by date_id) loop
        load_ad_cumulative_agg(l_run_id, i.date_id, l_process_name);
        l_max_date := i.date_id;
      end loop;
      load_fact_ad_month_to_date(l_run_id, l_max_date, l_process_name);
    else
      load_ad_cumulative_agg(l_run_id, l_process_date, l_process_name);
      load_fact_ad_month_to_date(l_run_id, l_process_date, l_process_name);
    end if;
    load_fact_homepage(l_run_id, l_process_date, l_process_name);
    load_ad_aff_coun_agg_video(l_run_id, l_process_date, l_process_name);
    -- replicate all to ADQ schema
    replicate_to_adq(l_run_id, l_process_date, l_process_name);
    -- replicate to BEN schema
    replicate_to_ben(l_run_id, l_process_date, l_process_name);
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

  procedure load_ga_data_copy
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_dedup_table  varchar2(30);
    l_cnt          number := 0;
  begin
    l_dedup_table  := 'ga_imp_clk_' || p_process_name || '_' ||
                      p_process_date;
    l_process_name := p_process_name || '-' ||
                      'drop_old_imps_clks_table';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      for i in (select *
                from   (select table_name,
                                substr(table_name, -8) as dt
                         from   dba_tables
                         where  upper(table_name) like
                                upper('GA_IMP_CLK_' ||
                                      p_process_name || '%')
                         and    owner = 'ADAPTIVE_DATA')
                where  to_date(p_process_date, 'yyyymmdd') - 3 >=
                       to_date(dt, 'yyyymmdd')) loop
        execute immediate 'drop table adaptive_data.' ||
                          i.table_name;
      end loop;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', 0);
    end if;
    l_process_name := p_process_name || '-' ||
                      l_dedup_table;
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate '
        create table adaptive_data.' ||
                        l_dedup_table || ' 
          tablespace ts_imps_old_day 
          parallel(degree 6 instances 1)
          compress for all operations 
          as
          select *
          from   adaptive_data.' ||
                        l_dedup_table || '@etlsga a';
      -- analyze the table  
      execute immediate 'analyze table adaptive_data.' ||
                        l_dedup_table ||
                        ' estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', 0);
    end if;
    ------ga_imp_clk
    if upper(p_process_name) = 'PROCESS' then
      --drop subpartition before 3 days
      for i in (select *
                from   dba_tab_subpartitions
                where  table_name = 'GA_IMP_CLK'
                and    table_owner = 'CAMPAIGN'
                and    subpartition_name like '%PROCESS%'
                and    high_value_length = 9
                and    replace(partition_name, 'P_', '') <
                       to_char(to_date(p_process_date, 'yyyymmdd') - 3, 'yyyymmdd')) loop
        execute immediate ' alter table ' || i.table_owner || '.' ||
                          i.table_name ||
                          ' drop subpartition ' ||
                          i.subpartition_name;
      end loop;
    else
      -- drop partitions before 45 days as this is retention for this table.
      for i in (select *
                from   dba_tab_partitions
                where  table_name = 'GA_IMP_CLK'
                and    table_owner = 'CAMPAIGN'
                and    replace(partition_name, 'P_', '') <
                       to_char(to_date(p_process_date, 'yyyymmdd') - 45, 'yyyymmdd')) loop
        execute immediate ' alter table ' || i.table_owner || '.' ||
                          i.table_name ||
                          ' drop partition ' ||
                          i.partition_name;
      end loop;
    end if;
    l_process_name := p_process_name || '-' || 'ga_imp_clk';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'alter table ga_imp_clk truncate subpartition P_' ||
                        p_process_date || '_' ||
                        upper(p_process_name);
      execute immediate '
        insert /*+ append */
        into ga_imp_clk
          (time_stamp, ad_id, site_id, dma_id, state_province,
           city_id, zip_code, browser_id, os_id, connection_type_id,
           content_partner_id, data_id, module_id, flags, atf_value,
           affiliate_id, request_status, creative_id, agent_type,
           internal_ip, ad_group_id, tile, developer_id,
           ad_size_request, ga_os_id, ga_browser_id, zone_id,
           tag_type, request_seq, user_id, advertiser_id, order_id,
           country_id, country_code, nad_dimension, category,
           date_id, logtype, ip, impressions, clicks,
           filtered_clicks, date_id_coc, coc, process_name)
          select time_stamp, a.ad_id, site_id, dma_id,
                 state_province, city_id, zip_code, browser_id,
                 os_id, connection_type_id, content_partner_id,
                 data_id, module_id, flags, atf_value,
                 a.affiliate_id, request_status, creative_id,
                 agent_type, internal_ip, ad_group_id, tile,
                 developer_id, ad_size_request, ga_os_id, ga_browser_id, 
                 zone_id, tag_type, request_seq, user_id,
                 coalesce(ord.advertiser_id, 0) as advertiser_id,
                 coalesce(ord.order_id, 0) as order_id,
                 coalesce(coun.country_id, 0) as country_id,
                 decode(upper(a.country_id), ''GB'', ''UK'', upper(a.country_id)) as country_code,
                 coalesce(ord.ad_size_id, ''-1'') as nad_dimension,
                 ord.category, a.date_id, logtype, ip, impressions,
                 clicks, filtered_clicks,
                 case
                   when ch.affiliate_id is null then
                    a.date_id
                   else
                    to_number(to_char(trunc(utc_time_stamp +
                                            coalesce(coc_offset, 0) / 24, ''dd''), ''yyyymmdd''))
                 end date_id_coc, ch.country as coc,
                 lower(''' ||
                        p_process_name ||
                        ''') as process_name
          from   adaptive_data.' ||
                        l_dedup_table ||
                        ' a,     
                 ods_metadata.adm_ads ord, dim_countries coun,
                 channel_aggregate_primary ch
          where  a.ad_id = ord.ad_id(+)
          and    upper(a.country_id) = coun.adaptive_code(+)
          and    a.affiliate_id = ch.affiliate_id(+)';
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ga_imp_clk subpartition(P_' ||
                        p_process_date || '_' ||
                        upper(p_process_name) ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ga_data_copy1
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_dedup_table  varchar2(30);
  begin
    l_dedup_table  := 'ga_imp_clk_' || p_process_name || '_' ||
                      p_process_date;
    l_process_name := p_process_name || '-' ||
                      'drop_old_imps_clks_table';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      for i in (select *
                from   (select table_name,
                                substr(table_name, -8) as dt
                         from   dba_tables
                         where  upper(table_name) like
                                upper('GA_IMP_CLK_' ||
                                      p_process_name || '%')
                         and    owner = 'ADAPTIVE_DATA')
                where  to_date(p_process_date, 'yyyymmdd') - 5 >=
                       to_date(dt, 'yyyymmdd')) loop
        execute immediate 'drop table adaptive_data.' ||
                          i.table_name;
      end loop;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', 0);
    end if;
    l_process_name := p_process_name || '-' ||
                      l_dedup_table;
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate '
        create table adaptive_data.' ||
                        l_dedup_table || ' 
          tablespace ts_imps_old_day 
          parallel(degree 6 instances 1)
          compress for all operations 
          as
          select time_stamp, a.ad_id, site_id, dma_id, state_province,
                 city_id, zip_code, browser_id, os_id,
                 connection_type_id, content_partner_id, data_id,
                 module_id, flags, atf_value, a.affiliate_id,
                 request_status, creative_id, agent_type, internal_ip,
                 ad_group_id, tile, developer_id, ad_size_request,
                 ga_os_id, ga_browser_id, zone_id, tag_type,
                 request_seq, user_id, 
                 coalesce(ord.advertiser_id, 0) as advertiser_id,
                 coalesce(ord.order_id, 0) as order_id,
                 coalesce(coun.country_id, 0) as country_id,
                 decode(upper(a.country_id), ''GB'', ''UK'', upper(a.country_id)) as country_code, 
                 coalesce(ord.ad_size_id, ''-1'') as nad_dimension,
                 ord.category, a.date_id, logtype, ip,
                 impressions, clicks, filtered_clicks,
                 case
                   when ch.affiliate_id is null then
                    a.date_id
                   else
                    to_number(to_char(trunc(utc_time_stamp +
                       coalesce(coc_offset, 0) / 24, ''dd''), ''yyyymmdd''))
                 end date_id_coc, ch.country as coc
          from   adaptive_data.' ||
                        l_dedup_table ||
                        '@etlsga a,
                 ods_metadata.adm_ads ord, dim_countries coun,
                 channel_aggregate_primary ch
          where  a.ad_id = ord.ad_id(+)
          and    upper(a.country_id) = coun.adaptive_code(+)
          and    a.affiliate_id = ch.affiliate_id(+)';
      -- analyze the table  
      execute immediate 'analyze table adaptive_data.' ||
                        l_dedup_table ||
                        ' estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', 0);
    end if;
  end;

  procedure load_fact_media_day_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    --l_dedup_table  varchar2(100);
  begin
    /*l_dedup_table  := 'ga_imp_clk_' || p_process_name || '_' ||
                      p_process_date;
    l_process_name := p_process_name || '-' ||
                      'fact_media_day_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_media_day_agg truncate partition P_' ||
                        p_process_date;
      execute immediate '
        insert \*+ append *\
        into fact_media_day_agg
            (date_id, advertiser_id, order_id, ad_id, site_id,
             affiliate_id, dma_id, country_id, state_province,
             city_id, zip_code, browser_id, connection_type_id,
             page_id, os_id, is_bt, is_atf, is_homepage, cid, did,
             mid, flags, total_impressions, total_clicks, source_id,
             creative_id, tile, agent_type, internal_ip, ga_city,
             ad_group_id, ad_size_request, developer_id,
             country_code, ga_os_id, ga_browser_id, zone_id,
             is_marketplace, tag_type, request_seq, nad_dimension,
             filtered_clicks, category, date_id_coc, coc)
          select \*+ parallel (a,8,1) *\
             date_id, advertiser_id, order_id, ad_id, site_id,
             a.affiliate_id, dma_id, country_id, state_province,
             -1 as city_id,
             to_char(regexp_substr(zip_code, ''^[0-9]{5}'')) zip_code,
             browser_id, connection_type_id,
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
                      end, -20) as page_id, os_id,
             case
               when bitand(flags, 2) = 2 then
                ''1''
               else
                ''0''
             end as is_bt, coalesce(atf_value, ''u'') as is_atf,
             case
               when bitand(flags, 1) = 1 then
                ''1''
               else
                ''0''
             end is_homepage, content_partner_id, data_id, module_id,
             flags, sum(impressions) total_impressions,
             sum(clicks) as total_clicks, request_status,
             creative_id, tile, agent_type, internal_ip,
             city_id as ga_city, ad_group_id, ad_size_request,
             developer_id, country_code, ga_os_id, ga_browser_id,
             zone_id,
             case
               when flags > 0 then
                decode(bitand(flags, 16), 16, 1, 0)
               else
                0
             end as is_marketplace, tag_type, request_seq, nad_dimension, 
             sum(filtered_clicks) as filtered_clicks, category,
             date_id_coc, coc
          from   adaptive_data.' ||
                        l_dedup_table || ' a
          --where  a.affiliate_id = b.affiliate_id(+)        
          group  by date_id, advertiser_id, order_id, ad_id,
                    site_id, a.affiliate_id, dma_id, country_id,
                    state_province,
                    to_char(regexp_substr(zip_code, ''^[0-9]{5}'')),
                    browser_id, connection_type_id, os_id,
                    coalesce(atf_value, ''u''), content_partner_id,
                    data_id, module_id, flags, request_status,
                    creative_id, tile, agent_type, internal_ip,
                    city_id, ad_group_id, ad_size_request,
                    developer_id, country_code, ga_os_id,
                    ga_browser_id, zone_id, tag_type, request_seq,
                    nad_dimension, category,
                    date_id_coc, coc';
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_media_day_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;*/
    ---
    l_process_name := p_process_name || '-' ||
                      'fact_media_day_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_media_day_agg truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into fact_media_day_agg
        (date_id, advertiser_id, order_id, ad_id, site_id,
         affiliate_id, dma_id, country_id, state_province,
         city_id, zip_code, browser_id, connection_type_id,
         page_id, os_id, is_bt, is_atf, is_homepage, cid,
         did, mid, flags, total_impressions, total_clicks,
         source_id, creative_id, tile, agent_type,
         internal_ip, ga_city, ad_group_id, ad_size_request,
         developer_id, country_code, ga_os_id, ga_browser_id,
         zone_id, is_marketplace, tag_type, request_seq,
         nad_dimension, filtered_clicks, category,
         date_id_coc, coc)
        select /*+ parallel (a,4,1) */
         date_id, advertiser_id, order_id, ad_id, site_id,
         a.affiliate_id, dma_id, country_id, state_province,
         -1 as city_id,
         to_char(regexp_substr(zip_code, '^[0-9]{5}')) zip_code,
         browser_id, connection_type_id,
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
                  end, -20) as page_id, os_id,
         case
           when bitand(flags, 2) = 2 then
            '1'
           else
            '0'
         end as is_bt, coalesce(atf_value, 'u') as is_atf,
         case
           when bitand(flags, 1) = 1 then
            '1'
           else
            '0'
         end is_homepage, content_partner_id, data_id,
         module_id, flags,
         sum(impressions) total_impressions,
         sum(clicks) as total_clicks, request_status,
         creative_id, tile, agent_type, internal_ip,
         city_id as ga_city, ad_group_id, ad_size_request,
         developer_id, country_code, ga_os_id, ga_browser_id,
         zone_id,
         case
           when flags > 0 then
            decode(bitand(flags, 16), 16, 1, 0)
           else
            0
         end as is_marketplace, tag_type, request_seq,
         nad_dimension,
         sum(filtered_clicks) as filtered_clicks, category,
         date_id_coc, coc
        from   ga_imp_clk a
        where  date_id = p_process_date
        and    process_name = lower(p_process_name)
        --from adaptive_data.ga_imp_clk_process_20130505_1 a
        group  by date_id, advertiser_id, order_id, ad_id,
                  site_id, a.affiliate_id, dma_id,
                  country_id, state_province,
                  to_char(regexp_substr(zip_code, '^[0-9]{5}')),
                  browser_id, connection_type_id, os_id,
                  coalesce(atf_value, 'u'),
                  content_partner_id, data_id, module_id,
                  flags, request_status, creative_id, tile,
                  agent_type, internal_ip, city_id,
                  ad_group_id, ad_size_request, developer_id,
                  country_code, ga_os_id, ga_browser_id,
                  zone_id, tag_type, request_seq,
                  nad_dimension, category, date_id_coc, coc;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_media_day_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_adq_report_tables
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name      varchar2(100);
    l_cnt               number := 0;
    l_ga_ad_counts_imps number := 0;
    l_flag_agg_imps     number := 0;
    l_msg               varchar2(1000);
  begin
    -- fact_adq_flag_agg
    l_process_name := p_process_name || '-' ||
                      'fact_adq_flag_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_flag_agg truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_adq_flag_agg
        (date_id, advertiser_id, order_id, ad_id,
         creative_id, affiliate_id, dma_id, country_id,
         state_province, ga_city, total_impressions,
         total_clicks, ad_group_id, source_id, network_name,
         vertical, primary_channel, country_code, flags,
         ad_dimension, ga_os_id, ga_browser_id, tile,
         tag_type, is_atf, zip_code, filtered_clicks)
      -- ,date_id_coc, coc)
        select /*+ parallel(a,4,1) */
         date_id, advertiser_id, order_id, ad_id,
         creative_id, a.affiliate_id, dma_id, country_id,
         state_province, ga_city, sum(total_impressions),
         sum(total_clicks), ad_group_id, source_id,
         b.network as network_name,
         b.site_uber_category as vertical,
         b.category_name_full as primary_channel,
         country_code, flags, nad_dimension, ga_os_id,
         ga_browser_id, tile, tag_type, is_atf, zip_code,
         sum(filtered_clicks)
        --, date_id_coc, coc
        from   fact_media_day_agg a,
               ods_metadata.channel_aggregate b
        where  date_id = p_process_date
        and    a.affiliate_id = b.affiliate_id(+)
        and    category_type_full(+) = 'Primary'
        group  by date_id, advertiser_id, order_id, ad_id,
                  creative_id, a.affiliate_id, dma_id,
                  country_id, state_province, ga_city,
                  ad_group_id, source_id, b.network,
                  b.site_uber_category, b.category_name_full,
                  country_code, flags, nad_dimension,
                  ga_os_id, ga_browser_id, tile, tag_type,
                  is_atf, zip_code;
      --, date_id_coc, coc;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_flag_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --- add the data verification check 
    -- Run the verification against ga_ad_counts_history table
    l_process_name := p_process_name || '-' ||
                      'verify_total_impressions';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate '
        select sum(impressions_delivered)       
        from   adaptive_data.ga_ad_counts_history@etlsga
        where  date_id = ' ||
                        p_process_date
        into l_ga_ad_counts_imps;
      select sum(total_impressions)
      into   l_flag_agg_imps
      from   fact_adq_flag_agg
      where  date_id = p_process_date;
      if l_ga_ad_counts_imps = l_flag_agg_imps then
        --l_msg := 'Success';
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      else
        l_msg := 'Delta Present in Total Impressions between SGA and ODS';
        pkg_log_process.log_process(p_run_id, p_process_date, '', 'UF', 0, l_msg);
        --creating the fake exception to fail the process
        select to_number('A') into l_cnt from dual;
      end if;
    end if;
    --- Ad Affiliate level Aggregate tables
    -- Ad Affiliate City Agg
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_aff_city_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_ad_aff_city_agg truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_adq_ad_aff_city_agg
        (date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, state_province, city_id,
         ga_city, total_impressions, total_clicks,
         ad_group_id, source_id, creative_id, network_name,
         vertical, primary_channel, dma_id, filtered_clicks)
      --, date_id_coc, coc)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, state_province,
         -1 as city_id, ga_city, sum(total_impressions),
         sum(total_clicks), ad_group_id, source_id,
         creative_id, network_name, vertical,
         primary_channel, dma_id, sum(filtered_clicks)
        --, date_id_coc, coc
        from   fact_adq_flag_agg a
        where  date_id = p_process_date
        group  by date_id, advertiser_id, order_id, ad_id,
                  affiliate_id, country_id, state_province,
                  ga_city, ad_group_id, source_id,
                  creative_id, network_name, vertical,
                  primary_channel, dma_id;
      --, date_id_coc, coc;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_ad_aff_city_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ----
    --Ad Aff Country State Agg
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_aff_state_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_ad_aff_state_agg truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_adq_ad_aff_state_agg
        (date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, state_province,
         total_impressions, total_clicks, ad_group_id,
         source_id, creative_id, network_name, vertical,
         primary_channel)
      --, date_id_coc, coc)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, state_province,
         sum(total_impressions), sum(total_clicks),
         ad_group_id, source_id, creative_id, network_name,
         vertical, primary_channel
        --, date_id_coc, coc
        from   fact_adq_ad_aff_city_agg a
        where  date_id = p_process_date
        group  by date_id, advertiser_id, order_id, ad_id,
                  affiliate_id, country_id, state_province,
                  ad_group_id, source_id, creative_id,
                  network_name, vertical, primary_channel;
      --, date_id_coc, coc;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_ad_aff_state_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- Ad Affiliate Country Agg 
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_aff_country_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_ad_aff_country_agg truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_adq_ad_aff_country_agg
        (date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, total_impressions,
         total_clicks, ad_group_id, source_id, creative_id,
         network_name, vertical, primary_channel)
      --,date_id_coc, coc)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, sum(total_impressions),
         sum(total_clicks), ad_group_id, source_id,
         creative_id, network_name, vertical,
         primary_channel
        --, date_id_coc, coc
        from   fact_adq_ad_aff_city_agg a
        where  date_id = p_process_date
        group  by date_id, advertiser_id, order_id, ad_id,
                  affiliate_id, country_id, ad_group_id,
                  source_id, creative_id, network_name,
                  vertical, primary_channel;
      --  , date_id_coc,coc;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_ad_aff_country_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --Ad Affiliate Agg
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_aff_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_ad_aff_agg truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_adq_ad_aff_agg
        (date_id, advertiser_id, order_id, ad_id,
         affiliate_id, total_impressions, total_clicks,
         ad_group_id, source_id, creative_id, network_name,
         vertical, primary_channel, filtered_clicks)
      -- ,   date_id_coc, coc)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id,
         affiliate_id, sum(total_impressions),
         sum(total_clicks), ad_group_id, source_id,
         creative_id, network_name, vertical,
         primary_channel, sum(filtered_clicks)
        --, date_id_coc,coc
        from   fact_adq_ad_aff_city_agg a
        where  date_id = p_process_date
        group  by date_id, advertiser_id, order_id, ad_id,
                  affiliate_id, ad_group_id, source_id,
                  creative_id, network_name, vertical,
                  primary_channel, date_id_coc, coc;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_ad_aff_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --- Ad level Aggregate tables
    -- Ad City Agg
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_city_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_ad_city_agg truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_adq_ad_city_agg
        (date_id, advertiser_id, order_id, ad_id,
         country_id, state_province, city_id, ga_city,
         total_impressions, total_clicks, ad_group_id,
         source_id, creative_id, dma_id)
      --, date_id_coc, coc)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id, country_id,
         state_province, city_id, ga_city,
         sum(total_impressions), sum(total_clicks),
         ad_group_id, source_id, creative_id, dma_id
        --, date_id_coc, coc
        from   fact_adq_ad_aff_city_agg a
        where  date_id = p_process_date
        group  by date_id, advertiser_id, order_id, ad_id,
                  country_id, state_province, city_id,
                  ga_city, ad_group_id, source_id,
                  creative_id, dma_id;
      --, date_id_coc, coc;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_ad_city_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --Ad Country State Agg
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_state_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_ad_state_agg truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_adq_ad_state_agg
        (date_id, advertiser_id, order_id, ad_id,
         country_id, state_province, total_impressions,
         total_clicks, ad_group_id, source_id, creative_id)
      --, date_id_coc, coc)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id, country_id,
         state_province, sum(total_impressions),
         sum(total_clicks), ad_group_id, source_id,
         creative_id
        --, date_id_coc, coc
        from   fact_adq_ad_aff_city_agg a
        where  date_id = p_process_date
        group  by date_id, advertiser_id, order_id, ad_id,
                  country_id, state_province, ad_group_id,
                  source_id, creative_id;
      --, date_id_coc, coc;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_ad_state_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- Ad Country Agg 
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_country_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_ad_country_agg truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_adq_ad_country_agg
        (date_id, advertiser_id, order_id, ad_id,
         country_id, total_impressions, total_clicks,
         ad_group_id, source_id, creative_id)
      --, date_id_coc,coc)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id, country_id,
         sum(total_impressions), sum(total_clicks),
         ad_group_id, source_id, creative_id
        --, date_id_coc,
        --coc
        from   fact_adq_ad_aff_city_agg a
        where  date_id = p_process_date
        group  by date_id, advertiser_id, order_id, ad_id,
                  country_id, ad_group_id, source_id,
                  creative_id;
      --, date_id_coc, coc;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_ad_country_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --Ad Agg
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_ad_agg truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_adq_ad_agg
        (date_id, advertiser_id, order_id, ad_id,
         total_impressions, total_clicks, ad_group_id,
         source_id, creative_id)
      --, date_id_coc, coc)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id,
         sum(total_impressions), sum(total_clicks),
         ad_group_id, source_id, creative_id
        -- , date_id_coc, coc
        from   fact_adq_ad_aff_city_agg a
        where  date_id = p_process_date
        group  by date_id, advertiser_id, order_id, ad_id,
                  ad_group_id, source_id, creative_id;
      --,date_id_coc, coc;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_ad_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --fact_adq_ad_aff_country_flag
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_aff_country_flag';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_ad_aff_country_flag truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_adq_ad_aff_country_flag
        (date_id, ad_id, advertiser_id, order_id,
         creative_id, affiliate_id, country_id,
         total_impressions, total_clicks, source_id,
         country_code, flags, tag_type, is_atf, tile,
         network_name, vertical, primary_channel, title,
         affiliate_url, atp_rank, coc, channel_name,
         ga_os_id, ga_browser_id, ad_group_id)
        select /*+ parallel(a,5,1) */
         date_id, ad_id, advertiser_id, order_id,
         creative_id, a.affiliate_id, country_id,
         sum(total_impressions), sum(total_clicks),
         source_id, country_code, flags, tag_type, is_atf,
         tile, coalesce(network, 'unknown'),
         site_uber_category as vertical,
         category_name_full as primary_channel, title,
         affiliate_url, atp_rank, country as coc,
         channel_name, ga_os_id, ga_browser_id, ad_group_id
        from   fact_adq_flag_agg a,
               ods_metadata.channel_aggregate b
        where  date_id = p_process_date
        and    a.affiliate_id = b.affiliate_id(+)
        and    category_type_full(+) = 'Primary'
        group  by date_id, ad_id, advertiser_id, order_id,
                  creative_id, a.affiliate_id, country_id,
                  source_id, country_code, flags, tag_type,
                  is_atf, tile, coalesce(network, 'unknown'),
                  site_uber_category, category_name_full,
                  title, affiliate_url, atp_rank, country,
                  channel_name, ga_os_id, ga_browser_id,
                  ad_group_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_ad_aff_country_flag partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure report_tables_to_adq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_sql          varchar2(4000);
    l_status       varchar2(100);
  begin
    -- load_ad_geo tables to multitenant
    replicate_adq_report_tables(p_run_id, p_process_date, p_process_name, 'campaign');
    --replicate_adq_report_tables(p_run_id, p_process_date, p_process_name, 'ADQ_1001');
    -- fact_adq_ad_aff_country_flag
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_aff_country_flag@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        --campaign.pkg_generic_ods.truncate_partition@etladq(p_process_date, 'campaign', 'fact_adq_ad_aff_country_flag');
        campaign.pkg_generic_ddl.truncate_partition@etladq('campaign', 'fact_adq_ad_aff_country_flag', 'P_' ||
                                                            p_process_date);
        l_sql := 'insert into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql ||
               'fact_adq_ad_aff_country_flag@etladq
        (date_id, ad_id, advertiser_id, order_id,
         creative_id, affiliate_id, country_id,
         total_impressions, total_clicks, source_id,
         country_code, flags, tag_type, is_atf, tile,
         network_name, vertical, primary_channel, title,
         affiliate_url, atp_rank, coc, channel_name,
         ga_os_id, ga_browser_id, ad_group_id)
        select /*+ parallel(a,5,1) */
         date_id, ad_id, advertiser_id, order_id,
         creative_id, affiliate_id, country_id,
         total_impressions, total_clicks, source_id,
         country_code, flags, tag_type, is_atf, tile,
         network_name, vertical, primary_channel, title,
         affiliate_url, atp_rank, coc, channel_name, 
         ga_os_id, ga_browser_id, ad_group_id
        from   fact_adq_ad_aff_country_flag a
        where  date_id = :p_process_date';
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_ad_aff_country_flag', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- check for the sequence aggregate step and call the package
    if upper(p_process_name) in ('PROCESS', 'REPROCESS') then
      select max(upper(status))
      into   l_status
      from   daily_process_log
      where  process_date = p_process_date
      and    upper(process_name) like
             upper(p_process_name) || '-RUN_INVENTORY%';
      if l_status = 'FAIL' then
        update daily_process_log
        set    status = 'COMPLETE'
        where  process_date = p_process_date
        and    upper(process_name) like
               upper(p_process_name) ||
               '-SEQUENCE_FACT_MEDIA_DAY%'
        and    error_reason like 'Campaign-Fact_Media_Day%';
        commit;
        pkg_sequence_aggregate.run_inventory(p_process_name, p_process_date);
      end if;
    elsif upper(p_process_name) in ('MANUAL') then
      pkg_sequence_aggregate.load_adq_request_seq_agg(p_run_id, p_process_date, l_process_name);
      pkg_sequence_aggregate.replicate_to_adq(p_run_id, p_process_date, l_process_name);
    end if;
    ---to retain 365 days of data in fact_adq_flag_agg
    if upper(p_process_name) in ('REPROCESS') then
      l_process_name := p_process_name || '-' ||
                        'Truncate_part_adq_flag_agg@etladq_' ||
                        to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 365, 'yyyymmdd'));
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        campaign.pkg_generic_ddl.truncate_partition@etladq('campaign', 'fact_adq_flag_agg', 'P_' ||
                                                            to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 365, 'yyyymmdd')));
        campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_flag_agg', 'P_' ||
                                                           to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 365, 'yyyymmdd')));
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC');
      end if;
    end if;
    if upper(p_process_name) in ('PROCESS') then
      l_cnt := 0;
      select count(1)
      into   l_cnt
      from   campaign.tz_agg_hist@etladq
      where  trunc(agg_date, 'dd') =
             trunc(to_date(p_process_date, 'yyyymmdd'), 'dd');
      if l_cnt = 0 then
        insert into campaign.tz_agg_hist@etladq
          (id, agg_date, time_zone, is_day_agg,
           is_dedup_agg, start_hour, end_hour)
        values
          (p_run_id, to_date(p_process_date, 'yyyymmdd'),
           'EST', 0, 0,
           to_date(p_process_date, 'yyyymmdd') - 6, '');
        insert into ade_data.adq_tz_agg_hist_est@etlgad
          (id, agg_date, time_zone, is_day_agg,
           is_dedup_agg, start_hour, end_hour)
        values
          (p_run_id, to_date(p_process_date, 'yyyymmdd'),
           'EST', 0, 0,
           to_date(p_process_date, 'yyyymmdd') - 6, '');
        commit;
      end if;
      -- campaign.tz_agg_hist@adq
      l_process_name := p_process_name || '-' ||
                        'campaign.tz_agg_hist@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        update campaign.tz_agg_hist@etladq
        set    id = p_run_id, is_dedup_agg = 1,
               is_day_agg = 1
        where  to_number(to_char(trunc(agg_date, 'dd'), 'yyyymmdd')) =
               p_process_date;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- ade_data.adq_tz_agg_hist_est@gedlink
      l_process_name := p_process_name || '-' ||
                        'ade_data.adq_tz_agg_hist_est@etlgad';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        update ade_data.adq_tz_agg_hist_est@etlgad
        set    id = p_run_id, is_dedup_agg = 1,
               is_day_agg = 1
        where  to_number(to_char(trunc(agg_date, 'dd'), 'yyyymmdd')) =
               p_process_date;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
    end if;
  end;

  procedure replicate_adq_report_tables
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2,
    p_schema_name  varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_source_id    number;
    l_sql          varchar2(30000);
    l_where_cond   varchar2(4000) := ' and source_id in (';
  begin
    l_source_id := case
                     when upper(p_schema_name) = 'CAMPAIGN' then
                      ''
                     when upper(p_schema_name) = 'ADQ_1001' then
                      1001
                   end;
    l_where_cond := l_where_cond || l_source_id || ')';
    -- fact_adq_flag_agg
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.' ||
                      'fact_adq_flag_agg' || '@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        --campaign.pkg_generic_ods.truncate_partition@etladq(p_process_date, p_schema_name, 'fact_adq_flag_agg');
        campaign.pkg_generic_ddl.truncate_partition@etladq('campaign', 'fact_adq_flag_agg', 'P_' ||
                                                            p_process_date);
        l_sql := 'insert /*+ append */ into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql || p_schema_name || '.' ||
               'fact_adq_flag_agg@etladq
            (date_id, advertiser_id, order_id, ad_id,
             creative_id, affiliate_id, dma_id, country_id,
             state_province, ga_city, total_impressions,
             total_clicks, ad_group_id, source_id, network_name,
             vertical, primary_channel, country_code, flags,
             ad_dimension, ga_os_id, ga_browser_id, tile,
             tag_type, is_atf, zip_code)
        select /*+ parallel(a,5,1) */
             date_id, advertiser_id, order_id, ad_id,
             creative_id, affiliate_id, dma_id, country_id,
             state_province, ga_city, total_impressions,
             total_clicks, ad_group_id, source_id, network_name,
             vertical, primary_channel, country_code, flags,
             ad_dimension, ga_os_id, ga_browser_id, tile,
             tag_type, is_atf, zip_code
        from   fact_adq_flag_agg a
        where  date_id = :p_process_date';
      if upper(p_schema_name) <> 'CAMPAIGN' then
        l_sql := l_sql || chr(10) || l_where_cond ||
                 chr(10);
      end if;
      /*l_sql := l_sql || chr(10) ||
      ' group by date_id, advertiser_id, order_id, ad_id,
                 creative_id, affiliate_id, dma_id, country_id,
                 state_province, ga_city, ad_group_id, source_id, 
                 network_name, vertical, primary_channel, 
                 country_code, flags, ad_dimension, ga_os_id, 
                 ga_browser_id, tile, tag_type, is_atf, zip_code';*/
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_flag_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --- Ad Affiliate level Aggregate tables
    -- Ad Affiliate City Agg
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.' ||
                      'fact_adq_ad_aff_city_agg' ||
                      '@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        /*l_sql := 'delete from ' || p_schema_name || '.' ||
                 'fact_adq_ad_aff_city_agg@adq
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;*/
        --campaign.pkg_generic_ods.truncate_partition@etladq(p_process_date, p_schema_name, 'fact_adq_ad_aff_city_agg');
        campaign.pkg_generic_ddl.truncate_partition@etladq('campaign', 'fact_adq_ad_aff_city_agg', 'P_' ||
                                                            p_process_date);
        l_sql := 'insert /*+ append */ into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql || p_schema_name || '.' ||
               'fact_adq_ad_aff_city_agg@etladq
                  (date_id, advertiser_id, order_id, ad_id,
                   affiliate_id, country_id, state_province, city_id,
                   ga_city, total_impressions, total_clicks,
                   ad_group_id, source_id, creative_id, network_name, 
                   vertical, primary_channel, dma_id)
              select /*+ parallel(a,5,1) */
                   date_id, advertiser_id, order_id, ad_id,
                   affiliate_id, country_id, state_province, city_id,
                   ga_city, total_impressions, total_clicks,
                   ad_group_id, source_id, creative_id, network_name, 
                   vertical, primary_channel, dma_id
              from   fact_adq_ad_aff_city_agg a
              where  date_id = :p_process_date';
      if upper(p_schema_name) <> 'CAMPAIGN' then
        l_sql := l_sql || chr(10) || l_where_cond;
      end if;
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_ad_aff_city_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --Ad Aff Country State Agg
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.' ||
                      'fact_adq_ad_aff_state_agg' ||
                      '@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ' || p_schema_name || '.' ||
                 'fact_adq_ad_aff_state_agg@etladq
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
        l_sql := 'insert into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql || p_schema_name || '.' ||
               'fact_adq_ad_aff_state_agg@etladq
            (date_id, advertiser_id, order_id, ad_id,
             affiliate_id, country_id, state_province,
             total_impressions, total_clicks, ad_group_id,
             source_id, creative_id, network_name, vertical,
             primary_channel)
        select /*+ parallel(a,5,1) */
             date_id, advertiser_id, order_id, ad_id,
             affiliate_id, country_id, state_province,
             total_impressions, total_clicks, ad_group_id,
             source_id, creative_id, network_name, vertical,
             primary_channel
        from   fact_adq_ad_aff_state_agg a
        where  date_id = :p_process_date';
      if upper(p_schema_name) <> 'CAMPAIGN' then
        l_sql := l_sql || chr(10) || l_where_cond;
      end if;
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_ad_aff_state_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- Ad Affiliate Country Agg 
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.' ||
                      'fact_adq_ad_aff_country_agg' ||
                      '@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ' || p_schema_name || '.' ||
                 'fact_adq_ad_aff_country_agg@etladq
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
        l_sql := 'insert into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql || p_schema_name || '.' ||
               'fact_adq_ad_aff_country_agg@etladq
            (date_id, advertiser_id, order_id, ad_id,
             affiliate_id, country_id, total_impressions,
             total_clicks, ad_group_id, source_id, creative_id, network_name, vertical,
             primary_channel)
        select /*+ parallel(a,5,1) */
             date_id, advertiser_id, order_id, ad_id,
             affiliate_id, country_id, total_impressions,
             total_clicks, ad_group_id, source_id, creative_id, network_name, vertical,
             primary_channel
        from   fact_adq_ad_aff_country_agg a
        where  date_id = :p_process_date';
      if upper(p_schema_name) <> 'CAMPAIGN' then
        l_sql := l_sql || chr(10) || l_where_cond;
      end if;
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_ad_aff_country_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --Ad Affiliate Agg
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.' ||
                      'fact_adq_ad_aff_agg' || '@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ' || p_schema_name || '.' ||
                 'fact_adq_ad_aff_agg@etladq
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
        l_sql := 'insert into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql || p_schema_name || '.' ||
               'fact_adq_ad_aff_agg@etladq
            (date_id, advertiser_id, order_id, ad_id,
             affiliate_id, total_impressions, total_clicks,
             ad_group_id, source_id, creative_id, network_name, vertical,
         primary_channel,filtered_clicks)
        select /*+ parallel(a,5,1) */
             date_id, advertiser_id, order_id, ad_id,
             affiliate_id, total_impressions, total_clicks,
             ad_group_id, source_id, creative_id, network_name, vertical,
         primary_channel,filtered_clicks
        from   fact_adq_ad_aff_agg a
        where  date_id = :p_process_date';
      if upper(p_schema_name) <> 'CAMPAIGN' then
        l_sql := l_sql || chr(10) || l_where_cond;
      end if;
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_ad_aff_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --- Ad level Aggregate tables
    -- Ad City Agg
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.' ||
                      'fact_adq_ad_city_agg' || '@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ' || p_schema_name || '.' ||
                 'fact_adq_ad_city_agg@etladq
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
        l_sql := 'insert into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql || p_schema_name || '.' ||
               'fact_adq_ad_city_agg@etladq
            (date_id, advertiser_id, order_id, ad_id,
             country_id, state_province, city_id, ga_city,
             total_impressions, total_clicks, ad_group_id,
             source_id, creative_id, dma_id)
        select /*+ parallel(a,5,1) */
             date_id, advertiser_id, order_id, ad_id, country_id,
             state_province, city_id, ga_city, total_impressions,
             total_clicks, ad_group_id, source_id, creative_id, dma_id
        from   fact_adq_ad_city_agg a
        where  date_id = :p_process_date';
      if upper(p_schema_name) <> 'CAMPAIGN' then
        l_sql := l_sql || chr(10) || l_where_cond;
      end if;
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_ad_city_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --Ad Country State Agg
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.' ||
                      'fact_adq_ad_state_agg' || '@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ' || p_schema_name || '.' ||
                 'fact_adq_ad_state_agg@etladq
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
        l_sql := 'insert into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql || p_schema_name || '.' ||
               'fact_adq_ad_state_agg@etladq
            (date_id, advertiser_id, order_id, ad_id,
             country_id, state_province, total_impressions,
             total_clicks, ad_group_id, source_id, creative_id)
        select /*+ parallel(a,5,1) */
             date_id, advertiser_id, order_id, ad_id, country_id,
             state_province, total_impressions, total_clicks,
             ad_group_id, source_id, creative_id
        from   fact_adq_ad_state_agg a
        where  date_id = :p_process_date';
      if upper(p_schema_name) <> 'CAMPAIGN' then
        l_sql := l_sql || chr(10) || l_where_cond;
      end if;
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_ad_state_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- Ad Country Agg 
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.' ||
                      'fact_adq_ad_country_agg' ||
                      '@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ' || p_schema_name || '.' ||
                 'fact_adq_ad_country_agg@etladq
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
        l_sql := 'insert into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql || p_schema_name || '.' ||
               'fact_adq_ad_country_agg@etladq
            (date_id, advertiser_id, order_id, ad_id,
             country_id, total_impressions, total_clicks,
             ad_group_id, source_id, creative_id)
        select /*+ parallel(a,5,1) */
             date_id, advertiser_id, order_id, ad_id, country_id,
             total_impressions, total_clicks, ad_group_id,
             source_id, creative_id
        from   fact_adq_ad_country_agg a
        where  date_id = :p_process_date';
      if upper(p_schema_name) <> 'CAMPAIGN' then
        l_sql := l_sql || chr(10) || l_where_cond;
      end if;
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_ad_country_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --Ad Agg
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.' ||
                      'fact_adq_ad_agg' || '@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ' || p_schema_name || '.' ||
                 'fact_adq_ad_agg@etladq
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
        l_sql := 'insert into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql || p_schema_name || '.' ||
               'fact_adq_ad_agg@etladq
            (date_id, advertiser_id, order_id, ad_id,
             total_impressions, total_clicks, ad_group_id,
             source_id, creative_id)
        select /*+ parallel(a,5,1) */
             date_id, advertiser_id, order_id, ad_id,
             total_impressions, total_clicks, ad_group_id,
             source_id, creative_id
        from   fact_adq_ad_agg a
        where  date_id = :p_process_date';
      if upper(p_schema_name) <> 'CAMPAIGN' then
        l_sql := l_sql || chr(10) || l_where_cond;
      end if;
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_ad_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- fact_media_camp_agg
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.' ||
                      'fact_media_camp_agg@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ' || p_schema_name || '.' ||
                 'fact_media_camp_agg@etladq
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
        l_sql := 'insert into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql || p_schema_name || '.' ||
               'fact_media_camp_agg@etladq
        (date_id, advertiser_id, order_id, ad_id,
         affiliate_id, total_impressions,
         total_clicks, source_id, creative_id, ad_group_id)
        select /*+ parallel (a,5,1) */ 
               date_id, advertiser_id, order_id, ad_id,
               affiliate_id, total_impressions, total_clicks, 
               source_id, creative_id, ad_group_id
        from   fact_media_camp_agg a
        where  date_id = :p_process_date';
      if upper(p_schema_name) <> 'CAMPAIGN' then
        l_sql := l_sql || chr(10) || l_where_cond;
      end if;
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_media_camp_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- fact_atako_adapt_agg_day
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.' ||
                      'fact_atako_adapt_agg_day@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ' || p_schema_name || '.' ||
                 'fact_atako_adapt_agg_day@etladq
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
        l_sql := 'insert into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql || p_schema_name || '.' ||
               'fact_atako_adapt_agg_day@etladq
        (date_id, ad_id, id_type, app_id, install_id,
         publisher_id, country_code, region_code,
         impressions_count, displaytime_total,
         allinteractions_count, views_with_interaction_count,
         interaction_totaltime, clicks, expansion_count,
         expansion_totaltime, video_totaltime, video_starts,
         video_completes, video_1, video_2, video_3, video_4,
         tab_1, tab_2, tab_3, tab_4, posts, refreshes,
         updates, raw_scrolls, scrolls, created_by,
         created_date, tab_5, tab_6, tab_7, tab_8, tab_9,
         tab_10, shares, exit_1, exit_2, exit_3, exit_4,
         exit_5, exit_6, exit_7, exit_8, exit_9, exit_10,
         refresh_changes, logins,
         qualified_interaction_count, all_tabs,
         qualified_interaction_duration, autoexpansion_count,
         autoexpansion_totaltime, fullscreen_count,
         fullscreen_totaltime, video25_count, video50_count,
         video75_count, pause_count, mute_count, error_num)
        select /*+ parallel (a,5,1) */
               date_id, ad_id, id_type, app_id, install_id,
               publisher_id, country_code, region_code,
               impressions_count, displaytime_total,
               allinteractions_count,
               views_with_interaction_count,
               interaction_totaltime, clicks,
               expansion_count, expansion_totaltime,
               video_totaltime, video_starts,
               video_completes, video_1, video_2, video_3,
               video_4, tab_1, tab_2, tab_3, tab_4, posts,
               refreshes, updates, raw_scrolls, scrolls,
               created_by, created_date, tab_5, tab_6, tab_7,
               tab_8, tab_9, tab_10, shares, exit_1, exit_2,
               exit_3, exit_4, exit_5, exit_6, exit_7,
               exit_8, exit_9, exit_10, refresh_changes,
               logins, qualified_interaction_count, all_tabs,
               qualified_interaction_duration,
               autoexpansion_count, autoexpansion_totaltime,
               fullscreen_count, fullscreen_totaltime,
               video25_count, video50_count, video75_count,
               pause_count, mute_count, error_num
        from   fact_atako_adapt_agg_day a
        where  date_id = :p_process_date';
      /*if upper(p_schema_name) <> 'CAMPAIGN' then
        l_sql := l_sql || chr(10) || l_where_cond;
      end if;*/
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_atako_adapt_agg_day', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_fact_media_camp_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_media_camp_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_media_camp_agg truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_media_camp_agg
        (date_id, advertiser_id, order_id, ad_id,
         affiliate_id, total_impressions, total_clicks,
         source_id, creative_id, ad_group_id)
        select /*+ PARALLEL (a 5,1) */
         a.date_id, advertiser_id, order_id, ad_id,
         affiliate_id,
         sum(total_impressions) total_impressions,
         sum(total_clicks) total_clicks, source_id,
         creative_id, ad_group_id
        from   fact_adq_ad_aff_country_agg a
        where  a.date_id = p_process_date
        group  by a.date_id, advertiser_id, order_id, ad_id,
                  affiliate_id, source_id, creative_id,
                  ad_group_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_media_camp_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ad_cumulative_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_previous_dt  number;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_ad_cumulative_agg ';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if to_char(to_date(p_process_date, 'yyyymmdd'), 'dd') = '01' then
        l_previous_dt := '';
      else
        l_previous_dt := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd'));
      end if;
      execute immediate 'Alter table fact_ad_cumulative_agg truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into fact_ad_cumulative_agg
        (date_id, ad_id, ad_group_id, source_id,
         total_impressions, total_clicks, month_id)
        select p_process_date, ad_id, ad_group_id, source_id,
               sum(total_impressions), sum(total_clicks),
               to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'))
        from   (select /*+ parallel(a,3,1) */
                  ad_id, ad_group_id, source_id,
                  sum(total_impressions) total_impressions,
                  sum(total_clicks) as total_clicks
                 from   fact_ad_cumulative_agg a
                 where  date_id = l_previous_dt
                 group  by ad_id, ad_group_id, source_id
                 union all
                 select /*+ parallel(a,3,1) */
                  ad_id, ad_group_id, source_id,
                  sum(total_impressions), sum(total_clicks)
                 from   fact_adq_ad_agg a
                 where  date_id = p_process_date
                 and    total_impressions > 0
                 group  by ad_id, ad_group_id, source_id)
        group  by ad_id, ad_group_id, source_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_ad_cumulative_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_fact_ad_month_to_date
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_month_start  number;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_ad_month_to_date';
    l_month_start  := to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd');
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_ad_month_to_date truncate partition P_' ||
                        l_month_start;
      insert /*+ append */
      into fact_ad_month_to_date
        (month_id, ad_id, ad_group_id, source_id,
         total_impressions, total_clicks, process_date)
        select /*+ parallel(nrd,4,1) */
         month_id, ad_id, ad_group_id, source_id,
         total_impressions, total_clicks, p_process_date
        from   fact_ad_cumulative_agg
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_ad_month_to_date partition(P_' ||
                        l_month_start ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_fact_homepage
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_homepage';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_homepage truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_homepage
        (date_id, advertiser_id, order_id, ad_id, site_id,
         affiliate_id, page_id, ad_dimension, country_id,
         total_impressions, total_clicks, source_id, dma_id,
         is_atf, flags, ad_group_id, country_code,
         ad_category)
        select /*+ PARALLEL (a,5,1) */
         date_id, a.advertiser_id, a.order_id, a.ad_id,
         site_id, affiliate_id, page_id, nad_dimension,
         country_id,
         sum(total_impressions) total_impressions,
         sum(total_clicks) total_clicks, a.source_id,
         a.dma_id, a.is_atf, a.flags, ad_group_id,
         country_code, category
        from   fact_media_day_agg a
        where  a.date_id = p_process_date
        and    is_homepage = '1'
        and    nad_dimension in
               ('160x600', '300x250', '300x600', '728x90', '224x126', '240x135', '800x150', '630x150', '270x150', '800x50', '160x160', '320x48', '650x35', '984x258', '990x26', '728x91', '120x600', '444x1', '444x10', '444x2', '468x60', '968x16', '968x90', '444x3', '480x270', '888x12', '928x60', '970x66', '300x50', '888x21')
        group  by date_id, a.advertiser_id, a.order_id,
                  a.ad_id, site_id, affiliate_id, page_id,
                  nad_dimension, country_id, a.source_id,
                  a.dma_id, a.is_atf, a.flags, ad_group_id,
                  country_code, category;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_homepage partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ad_aff_coun_agg_video
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    -- Ad Affiliate Country Agg ( Video ads) GA_OS_ID added.
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_aff_coun_os_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_ad_aff_coun_os_agg truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_adq_ad_aff_coun_os_agg
        (date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, total_impressions,
         total_clicks, ad_group_id, source_id, ga_os_id)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, sum(total_impressions),
         sum(total_clicks), ad_group_id, source_id, ga_os_id
        from   fact_adq_flag_agg a
        where  date_id = p_process_date
        group  by date_id, advertiser_id, order_id, ad_id,
                  affiliate_id, country_id, ad_group_id,
                  source_id, ga_os_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_ad_aff_coun_os_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_adq_ad_osi
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    cursor cur_adm_ads is
      select date_id, total_imps, total_clk, a.ad_id,
             source_id, ad_type, active, delivery_type,
             start_date, end_date, booked_impressions,
             launch_timezone, null, null, null, null
      from   ods_metadata.adm_ads a,
             (select date_id, ad_id,
                      sum(total_impressions) total_imps,
                      sum(total_clicks) total_clk
               from   fact_adq_ad_agg
               where  date_id = p_process_date
               group  by date_id, ad_id) b
      where  a.ad_id = b.ad_id;
    --where ad_id in (5000087288);
    type t_number is table of number;
    type t_vc2 is table of varchar2(255);
    type t_date is table of date;
    t_date_id            t_number := t_number();
    t_total_imps         t_number := t_number();
    t_total_clk          t_number := t_number();
    t_ad_id              t_number := t_number();
    t_source_id          t_number := t_number();
    t_ad_type            t_vc2 := t_vc2();
    t_active             t_number := t_number();
    t_delivery_type      t_vc2 := t_vc2();
    t_start_date         t_date := t_date();
    t_end_date           t_date := t_date();
    t_booked_impressions t_number := t_number();
    t_launch_timezone    t_vc2 := t_vc2();
    t_serv_tw            t_number := t_number();
    t_ad_tot_tw          t_number := t_number();
    t_osi                t_number := t_number();
    t_exp_del            t_number := t_number();
    l_today_date         date := sysdate;
    l_timestamp          timestamp := systimestamp;
    c_tw_span_min        number(2) := 15;
    l_tw_ratio           number;
    l_impr_ratio         number;
    l_remain_tw          number;
    l_elap_tw            number;
    ln_ad_id             number;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_osi';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_ad_osi truncate partition P_' ||
                        p_process_date;
      open cur_adm_ads;
      loop
        fetch cur_adm_ads bulk collect
          into t_date_id, t_total_imps, t_total_clk, t_ad_id,
               t_source_id, t_ad_type, t_active,
               t_delivery_type, t_start_date, t_end_date,
               t_booked_impressions, t_launch_timezone,
               t_serv_tw, t_ad_tot_tw, t_osi,
               t_exp_del limit 5000;
        exit when t_ad_id.count = 0;
        --dbms_output.put_line('Processing starts...');
        for z in 1 .. t_ad_id.count loop
          begin
            ln_ad_id := t_ad_id(z);
            t_serv_tw(z) := ((l_today_date -
                            t_start_date(z)) /
                            (24 * 60 * 60)) *
                            (24 * 60 / c_tw_span_min);
            t_osi(z) := 100;
            if upper(t_ad_type(z)) in ('EXCLUSIVE', 'BULK') then
              if upper(t_delivery_type(z)) = 'EVEN' then
                --dbms_output.put_line('Checking exclusive or bulk - EVEN');
                t_ad_tot_tw(z) := to_number((t_end_date(z) -
                                            t_start_date(z)) /
                                            (24 * 60 * 60) /
                                            (15 * 60));
                t_exp_del(z) := to_number(((l_today_date -
                                          t_start_date(z)) /
                                          (15 * 60)) *
                                          (t_booked_impressions(z) /
                                          t_ad_tot_tw(z)));
              elsif upper(t_delivery_type(z)) <> 'EVEN' then
                --dbms_output.put_line('Checking exclusive or bulk NOT EVEN');
                t_exp_del(z) := t_booked_impressions(z);
              end if;
            end if;
            if t_exp_del(z) > 0 then
              t_osi(z) := round((t_total_imps(z) /
                                t_exp_del(z)) * 100, 2);
            end if;
            if upper(t_ad_type(z)) in
               ('STANDARD 1', 'STANDARD 2', 'STANDARD 3', 'STANDARD 4') then
              --dbms_output.put_line('checking standard 1234 - std ad');
              if sys_extract_utc(from_tz(to_timestamp(to_char(t_end_date(z), 'dd-mon-yyyy hh:mi:ss pm')), nvl(t_launch_timezone(z), 'US/Eastern'))) <
                 sys_extract_utc(from_tz(to_timestamp(to_char(l_today_date, 'dd-mon-yyyy hh:mi:ss pm')), 'US/Eastern')) then
                --dbms_output.put_line('setting l_remain_tw := 0');
                l_remain_tw := 0;
              elsif sys_extract_utc(from_tz(to_timestamp(to_char(t_end_date(z), 'dd-mon-yyyy hh:mi:ss pm')), nvl(t_launch_timezone(z), 'US/Eastern'))) >=
                    sys_extract_utc(from_tz(to_timestamp(to_char(l_today_date, 'dd-mon-yyyy hh:mi:ss pm')), 'US/Eastern')) then
                select (mdays + mhrs + mins + msec) / 15
                into   l_elap_tw
                from   (select utc,
                                extract(day from utc) * 60 * 24 mdays,
                                extract(hour from utc) * 60 mhrs,
                                extract(minute from utc) mins,
                                extract(second from utc) / 60 as msec
                         from   ((select sys_extract_utc(l_timestamp) -
                                           sys_extract_utc(from_tz(to_timestamp(to_char(t_start_date(z), 'dd-mon-yyyy hh:mi:ss pm')), nvl(t_launch_timezone(z), 'US/Eastern'))) utc
                                   from   dual)));
                select (mdays + mhrs + mins + msec) / 15
                into   l_remain_tw
                from   (select utc,
                                extract(day from utc) * 60 * 24 mdays,
                                extract(hour from utc) * 60 mhrs,
                                extract(minute from utc) mins,
                                extract(second from utc) / 60 as msec
                         from   (select utc
                                  from   (select sys_extract_utc(from_tz(to_timestamp(to_char(t_end_date(z), 'dd-mon-yyyy hh:mi:ss pm')), nvl(t_launch_timezone(z), 'US/Eastern'))) -
                                                   sys_extract_utc(l_timestamp) utc
                                           from   dual)));
              end if;
            end if;
            if l_elap_tw > 0 and
               t_booked_impressions(z) > 0 then
              l_tw_ratio := (l_elap_tw + l_remain_tw) /
                            l_elap_tw;
              l_impr_ratio := t_total_imps(z) /
                              t_booked_impressions(z);
              t_osi(z) := 100 * l_tw_ratio * l_impr_ratio;
              t_osi(z) := round(t_osi(z), 2);
            end if;
          exception
            when others then
              null;
          end;
          -----------------------------------------------------------
        end loop;
        forall z in 1 .. t_ad_id.count
          insert into fact_adq_ad_osi
            (date_id, ad_id, booked_imps, start_date,
             end_date, del_tw, total_tw, osi, processed_on,
             served_imps)
          values
            (t_date_id(z), t_ad_id(z),
             t_booked_impressions(z), t_start_date(z),
             t_end_date(z), t_serv_tw(z), t_ad_tot_tw(z),
             t_osi(z), l_today_date, t_total_imps(z));
        l_cnt := t_ad_id.count;
        commit;
      end loop;
      execute immediate 'analyze table fact_adq_ad_osi partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- adq_ad_osi@adq
    l_process_name := p_process_name || '-' ||
                      'adq_ad_osi@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from campaign.adq_ad_osi@etladq
        where  date_id = p_process_date;
      end if;
      insert into campaign.adq_ad_osi@etladq
        (date_id, ad_id, booked_imps, start_date, end_date,
         del_tw, total_tw, osi, processed_on, served_imps,
         ctr_optimization_enabled, active, contains_survey,
         contain_floating_ad)
        select /*+ parallel(a,4) */
         date_id, ad_id, booked_imps, start_date, end_date,
         del_tw, total_tw, osi, processed_on, served_imps,
         ctr_optimization_enabled, active, contains_survey,
         contain_floating_ad
        from   fact_adq_ad_osi a
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'adq_ad_osi', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_oms_product
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    -- dim_oms_product
    l_process_name := p_process_name || '-' ||
                      'dim_oms_product';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'TRUNCATE TABLE DIM_OMS_PRODUCT';
      insert /*+APPEND */
      into dim_oms_product
        (line_item_id, product_id, product_name, ad_id,
         ad_name, ad_ad_server_id, ad_server_name,
         updated_date)
        select li.id as line_item_id, p.id as product_id,
               p.name as product_name, ad.id as ad_id,
               ad.name as ad_name,
               ad.dfp_id__c as ad_ad_server_id,
               decode(ad.ad_server__c, null, 'DFP', ad.ad_server__c) as ad_server_name,
               sysdate
        from   oms_data.latest_approved_line_item__c@etlnap li,
               oms_data.ad__c@etlnap ad,
               oms_data. proposal_line_item__c@etlnap pli,
               oms_data.ad_product__c@etlnap p
        where  li.line_item_number__c =
               ad.line_item_number__c(+)
        and    li.id = pli.id
        and    pli.ad_product__c = p.id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_oms_product@adq
    l_process_name := p_process_name || '-' ||
                      'dim_oms_product@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from campaign.dim_oms_product@etladq;
      insert into campaign.dim_oms_product@etladq
        select * from dim_oms_product;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_oms_evolution_product@adq
    l_process_name := p_process_name || '-' ||
                      'dim_oms_evolution_product@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      insert into campaign.dim_oms_evolution_product@etladq
        (line_item_id, product_id, product_name, ad_id,
         ad_name, ad_ad_server_id, ad_server_name,
         updated_date, evolution_product_name)
        select line_item_id, product_id, product_name, ad_id,
               ad_name, ad_ad_server_id, ad_server_name,
               updated_date, null as evolution_product_name
        from   campaign.dim_oms_product
        where  ad_ad_server_id in
               (select ad_ad_server_id
                from   campaign.dim_oms_product
                where  ad_ad_server_id is not null
                and    ad_ad_server_id not in
                       (select ad_ad_server_id
                         from   campaign.dim_oms_evolution_product@etladq
                         where  ad_ad_server_id is not null));
      l_cnt := sql%rowcount;
      commit;
      update campaign.dim_oms_evolution_product@etladq a
      set    evolution_product_name =
              (select distinct evolution_product_name
               from   dim_oms_ged_pmap@etladq b
               where  a.product_name = b.oms_product_name)
      where  a.evolution_product_name is null;
      commit;
      update campaign.dim_oms_evolution_product@etladq a
      set    evolution_product_name = 'Others'
      where  evolution_product_name is null;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure replicate_to_adq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_sql          varchar2(30000);
  begin
    -- fact_homepage
    l_process_name := p_process_name || '-' ||
                      'fact_homepage@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        --campaign.pkg_generic_ods.truncate_partition@etladq(p_process_date, 'campaign', 'fact_homepage');
        campaign.pkg_generic_ddl.truncate_partition@etladq('campaign', 'fact_homepage', 'P_' ||
                                                            p_process_date);
        l_sql := 'insert /*+ append */ into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql ||
               'campaign.fact_homepage@etladq
            (date_id, advertiser_id, order_id, ad_id, site_id,
             affiliate_id, page_id, ad_dimension, country_id,
             total_impressions, total_clicks, created_by,
             created_date, updated_by, updated_date, source_id,
             dma_id, is_atf, flags, ad_group_id)
            select /*+ parallel (a,5,1) */
             date_id, advertiser_id, order_id, ad_id, site_id,
             affiliate_id, page_id, ad_dimension, country_id,
             total_impressions, total_clicks, created_by,
             created_date, updated_by, updated_date, source_id,
             dma_id, is_atf, flags, ad_group_id
            from   campaign.fact_homepage a
            where  date_id = :p_process_date';
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_homepage', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- fact_adq_ad_aff_coun_os_agg
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_aff_coun_os_agg@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        --campaign.pkg_generic_ods.truncate_partition@etladq(p_process_date, 'campaign', 'fact_adq_ad_aff_coun_os_agg');
        campaign.pkg_generic_ddl.truncate_partition@etladq('campaign', 'fact_adq_ad_aff_coun_os_agg', 'P_' ||
                                                            p_process_date);
        l_sql := 'insert /*+ append */ into ';
      else
        l_sql := 'insert /*+ append */ into ';
      end if;
      l_sql := l_sql ||
               'fact_adq_ad_aff_coun_os_agg@etladq
        (date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, total_impressions,
         total_clicks, ad_group_id, source_id, ga_os_id)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, total_impressions,
         total_clicks, ad_group_id, source_id, ga_os_id
        from   fact_adq_ad_aff_coun_os_agg a
        where  date_id = :p_process_date';
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_ad_aff_coun_os_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_zone
    l_process_name := p_process_name || '-' ||
                      'dim_zone@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      --l_cnt := 0;
      delete from campaign.dim_zone@etladq;
      insert into campaign.dim_zone@etladq
        select * from campaign.dim_zone;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure replicate_to_ben
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    -- fact_adq_ad_aff_city_agg@etlben
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_aff_city_agg@etlben';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        --bo.pkg_generic_ben.truncate_partition@etlben(p_process_date, 'bo', 'fact_adq_ad_aff_city_agg');
        bo.pkg_generic_ddl.truncate_partition@etlben('bo', 'fact_adq_ad_aff_city_agg', 'P_' ||
                                                      p_process_date);
      end if;
      insert /*+ append */
      into bo.fact_adq_ad_aff_city_agg@etlben
        (date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, state_province, city_id,
         ga_city, total_impressions, total_clicks,
         ad_group_id, source_id, dma_id)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, state_province, city_id,
         ga_city, total_impressions, total_clicks,
         ad_group_id, source_id, dma_id
        from   fact_adq_ad_aff_city_agg a
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      bo.pkg_generic_ddl.analyze_partition@etlben('bo', 'fact_adq_ad_aff_city_agg', 'P_' ||
                                                   p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- fact_adq_ad_aff_coun_os_agg@etlben
    l_process_name := p_process_name || '-' ||
                      'fact_adq_ad_aff_coun_os_agg@etlben';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        --bo.pkg_generic_ben.truncate_partition@etlben(p_process_date, 'bo', 'fact_adq_ad_aff_coun_os_agg');
        bo.pkg_generic_ddl.truncate_partition@etlben('bo', 'fact_adq_ad_aff_coun_os_agg', 'P_' ||
                                                      p_process_date);
      end if;
      insert /*+ append */
      into bo.fact_adq_ad_aff_coun_os_agg@etlben
        (date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, total_impressions,
         total_clicks, ad_group_id, source_id, ga_os_id)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, ad_id,
         affiliate_id, country_id, total_impressions,
         total_clicks, ad_group_id, source_id, ga_os_id
        from   fact_adq_ad_aff_coun_os_agg a
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      bo.pkg_generic_ddl.analyze_partition@etlben('bo', 'fact_adq_ad_aff_coun_os_agg', 'P_' ||
                                                   p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_lam_average_ctr
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_adq_avg_ctr';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'alter table fact_adq_avg_ctr truncate partition p_' ||
                        p_process_date;
      insert /*+ append */
      into fact_adq_avg_ctr
        (date_id, ad_id, affiliate_id, total_impressions,
         total_clicks)
        select p_process_date, a.ad_id, affiliate_id,
               sum(total_impressions), sum(total_clicks)
        from   (select /*+ parallel(a,5,1) */
                  ad_id, affiliate_id, total_impressions,
                  total_clicks
                 from   fact_adq_avg_ctr
                 where  date_id =
                        to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd'))
                 union all
                 select /*+ parallel(a,5,1) */
                  ad_id, affiliate_id, total_impressions,
                  total_clicks
                 from   fact_adq_ad_aff_agg
                 where  date_id = p_process_date) a,
               adm_data.lam_ads@etlgad b
        where  a.ad_id = b.ad_id
        and    coalesce(affiliate_id, 0) <> 0
        and    ctr_optimization_enabled = 1
        group  by a.ad_id, affiliate_id
        having sum(total_impressions) > 100;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    if upper(p_process_name) = 'PROCESS' then
      l_process_name := p_process_name || '-' ||
                        'lam_ads_gpo';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from lam_ads_gpo where key_id = 'DAYWISE';
        insert into lam_ads_gpo
          (affiliate_id, key, ctr, ad_id, impressions,
           clicks, key_id)
          with src as
           (select ad_id, ctr_optimization_enabled
            from   adm_data.lam_ads@etlgad
            where  ctr_optimization_enabled = 1)
          select /*+ parallel (a,5,1) */
           affiliate_id, a.ad_id || '-1d' as key,
           case
             when sum(total_impressions) = 0 then
              0
             else
              round(sum(total_clicks) * 100 /
                    sum(total_impressions), 5)
           end as ctr, a.ad_id,
           sum(total_impressions) as impressions,
           sum(total_clicks) as clicks, 'DAYWISE' as key_id
          from   fact_adq_ad_aff_agg a, src b
          where  date_id = p_process_date
          and    a.ad_id = b.ad_id
          and    coalesce(affiliate_id, 0) <> 0
          group  by affiliate_id, a.ad_id
          having sum(total_impressions) > 100
          union all
          select /*+ parallel (a,5,1) */
           affiliate_id, a.ad_id || '-2d' as key,
           case
             when sum(total_impressions) = 0 then
              0
             else
              round(sum(total_clicks) * 100 /
                    sum(total_impressions), 5)
           end as ctr, a.ad_id,
           sum(total_impressions) as impressions,
           sum(total_clicks) as clicks, 'DAYWISE' as key_id
          from   fact_adq_ad_aff_agg a, src b
          where  date_id >=
                 to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd'))
          and    date_id <= p_process_date
          and    a.ad_id = b.ad_id
          and    coalesce(affiliate_id, 0) <> 0
          group  by affiliate_id, a.ad_id
          having sum(total_impressions) > 100
          union all
          select /*+ parallel (a,5,1) */
           affiliate_id, a.ad_id || '-7d' as key,
           case
             when sum(total_impressions) = 0 then
              0
             else
              round(sum(total_clicks) * 100 /
                    sum(total_impressions), 5)
           end as ctr, a.ad_id,
           sum(total_impressions) as impressions,
           sum(total_clicks) as clicks, 'DAYWISE' as key_id
          from   fact_adq_ad_aff_agg a, src b
          where  date_id >=
                 to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 6, 'yyyymmdd'))
          and    date_id <= p_process_date
          and    a.ad_id = b.ad_id
          and    coalesce(affiliate_id, 0) <> 0
          group  by affiliate_id, a.ad_id
          having sum(total_impressions) > 100
          union all
          select /*+ parallel (a,5,1) */
           affiliate_id, a.ad_id || '-avg' as key,
           case
             when sum(total_impressions) = 0 then
              0
             else
              round(sum(total_clicks) * 100 /
                    sum(total_impressions), 5)
           end as ctr, a.ad_id,
           sum(total_impressions) as impressions,
           sum(total_clicks) as clicks, 'DAYWISE' as key_id
          from   fact_adq_avg_ctr a
          where  date_id = p_process_date
          and    coalesce(affiliate_id, 0) <> 0
          group  by affiliate_id, a.ad_id;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      l_process_name := p_process_name || '-' ||
                        'lam_ads_gpo@etlgad';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from adm_data.lam_ads_gpo@etlgad
        where  key_id = 'DAYWISE';
        insert into adm_data.lam_ads_gpo@etlgad
          (affiliate_id, key, ctr, ad_id, impressions,
           clicks, key_id)
          select /*+ parallel(a,4,1) */
           affiliate_id, key, ctr, ad_id, impressions,
           clicks, key_id
          from   lam_ads_gpo a
          where  key_id = 'DAYWISE';
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
    end if;
    -- replicated here for time being
    -- include new affiliate into new_channel_agg table
    merge into new_channel_agg a
    using (select a.affiliate_id, a.network,
                  a.site_uber_category, a.category_name_full,
                  a.category_type_full, a.country,
                  decode(b.coc_offset, '', 0, b.coc_offset) as coc_offset,
                  coalesce(b.region_code, 'OTHER') as region_code
           from   ods_metadata.channel_aggregate a,
                  (select distinct country, coc_offset,
                                    region_code
                    from   new_channel_agg) b
           where  a.category_type_full = 'Primary'
           and    a.country = b.country) c
    on (a.affiliate_id = c.affiliate_id)
    when not matched then
      insert
        (affiliate_id, network, site_uber_category,
         category_name_full, category_type_full, country,
         coc_offset, region_code)
      values
        (c.affiliate_id, c.network, c.site_uber_category,
         c.category_name_full, c.category_type_full,
         c.country, c.coc_offset, c.region_code);
  end;

end;
/
