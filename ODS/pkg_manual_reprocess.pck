create or replace package pkg_manual_reprocess as

  procedure run_manual
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure run_custom_steps
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_manual_reprocess as

  procedure run_manual
  (
    p_process_name varchar2,
    p_process_date number
  ) is
    l_run_id number := 300;
  begin
    pkg_campaign_all.run_campaign(p_process_name, p_process_date);
    pkg_campaign_uniques.run_campaign_uniques(p_process_name, p_process_date);
    run_custom_steps(l_run_id, p_process_date, p_process_name);
    for i in (select date_id
              from   dim_date
              where  date_id >= 20121103
              and    date_id <=
                     to_number(to_char(sysdate - 1, 'yyyymmdd'))
              order  by date_id) loop
      pkg_ben_import.load_network_revenue_daily_agg(300, i.date_id, p_process_name);
    end loop;
    -- send completion mail
    pa_send_email.mail('odsimportreport@glam.com', 'nileshm@glam.com', 'Manual Reprocess', 'Manual Reprocess for full packages and custom steps is complete');
  exception
    when others then
      pa_send_email.mail('odsimportreport@glam.com', 'nileshm@glam.com', 'Manual Reprocess', 'Manual Reprocess FAIL' ||
                          dbms_utility.format_error_backtrace ||
                          dbms_utility.format_error_stack);
  end;

  procedure run_custom_steps
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_sql          varchar(4000);
  begin
    ---ben import
    pkg_ben_import.load_ben_fact_media_day(p_run_id, p_process_date, p_process_name);
    pkg_ben_import.load_ben_fact_media_inventory(p_run_id, p_process_date, p_process_name);
    pkg_ben_import.load_fact_atac_atp_day(p_run_id, p_process_date, p_process_name);
    pkg_ben_import.load_network_revenue_daily(p_run_id, p_process_date, p_process_name);
    -- network_revenue_daily
    l_process_name := p_process_name || '-' ||
                      'network_revenue_daily@nap';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ap_data.network_revenue_daily where date_id = ' ||
                 p_process_date;
        ap_data.pkg_generic_ddl.sql_from_ods@nap(l_sql);
      end if;
      insert into ap_data.network_revenue_daily@nap
        (view_date, date_id, process_date_id,
         impressions_delivered, clicks, ad_id, country,
         impression_type, affiliate_id, site_id, zone_id,
         content_partner_id, developer_id, module_id,
         data_id, data_source, agent_type, internal_ip,
         flags, atf_value, ad_group_id, total_inventory,
         total_debug)
        select view_date, date_id, process_date_id,
               impressions_delivered, clicks, ad_id, country,
               impression_type, affiliate_id, site_id,
               zone_id, content_partner_id, developer_id,
               module_id, data_id, data_source, agent_type,
               internal_ip, flags, atf_value, ad_group_id,
               total_inventory, total_debug
        from   ben_network_revenue_daily
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- fact_atac_atp_day
    l_process_name := p_process_name || '-' ||
                      'fact_atac_atp_day@nap';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ap_data.fact_atac_atp_day where view_date = to_date(' ||
                 p_process_date || ', ''yyyymmdd'')';
        ap_data.pkg_generic_ddl.sql_from_ods@nap(l_sql);
      end if;
      insert into ap_data.fact_atac_atp_day@nap
        (view_date, affiliate_id, atp_rank, pec,
         ad_dimension, ad_category, flags, is_atf,
         is_marketplace, country_id, total_impressions,
         total_clicks, created_by, created_date, updated_by,
         updated_date, data_source, ad_group_id)
        select view_date, affiliate_id, atp_rank, pec,
               ad_dimension, ad_category, flags, is_atf,
               is_marketplace, country_id, total_impressions,
               total_clicks, created_by, created_date,
               updated_by, updated_date, data_source,
               ad_group_id
        from   ben_fact_atac_atp_day
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ----ben dcm 
    pkg_ben_dcm.load_dart_counts_new(p_run_id, p_process_date, p_process_name);
    pkg_ben_dcm.load_glamcontent_aggregate(p_run_id, p_process_date, p_process_name);
    pkg_ben_dcm.load_video_play_data(p_run_id, p_process_date, p_process_name);
    pkg_ben_dcm.load_module_aggregate(p_run_id, p_process_date, p_process_name);
    -- dart_counts_new
    l_process_name := p_process_name || '-' ||
                      'dart_counts_new@ben';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from bo.dart_counts_new where date_id = ' ||
                 p_process_date;
        bo.pkg_generic_ben.sql_from_ods@ben(l_sql);
      end if;
      insert into bo.dart_counts_new@ben
        (date_id, process_date_id, count_type, view_date,
         count_number, ad_id, site_id, country_id,
         state_province, dma_id, page_id, city_id,
         ad_size_request, view_month, view_year, data_source,
         agent_type, internal_ip, ad_group_id)
        select date_id, process_date_id, count_type,
               view_date, count_number, ad_id, site_id,
               country_id, state_province, dma_id, page_id,
               city_id, ad_size_request, view_month,
               view_year, data_source, agent_type,
               internal_ip, ad_group_id
        from   ben_dart_counts_new
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- fact_atac_atp_day
    l_process_name := p_process_name || '-' ||
                      'fact_atac_atp_day@ben';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from dart_data.fact_atac_atp_day where view_date = to_date(' ||
                 p_process_date || ', ''yyyymmdd'')';
        bo.pkg_generic_ben.sql_from_ods@ben(l_sql);
      end if;
      insert into dart_data.fact_atac_atp_day@ben
        (view_date, affiliate_id, atp_rank, pec,
         ad_dimension, ad_category, flags, is_atf,
         is_marketplace, country_id, total_impressions,
         total_clicks, created_by, created_date, updated_by,
         updated_date, data_source, ad_group_id)
        select view_date, affiliate_id, atp_rank, pec,
               ad_dimension, ad_category, flags, is_atf,
               is_marketplace, country_id, total_impressions,
               total_clicks, created_by, created_date,
               updated_by, updated_date, data_source,
               ad_group_id
        from   ben_fact_atac_atp_day
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- network_revenue_daily
    l_process_name := p_process_name || '-' ||
                      'network_revenue_daily@ben';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from bo.network_revenue_daily where date_id = ' ||
                 p_process_date;
        bo.pkg_generic_ben.sql_from_ods@ben(l_sql);
      end if;
      insert into bo.network_revenue_daily@ben
        (view_date, date_id, process_date_id,
         impressions_delivered, clicks, ad_id, country,
         impression_type, affiliate_id, site_id, zone_id,
         content_partner_id, developer_id, module_id,
         data_id, data_source, agent_type, internal_ip,
         flags, atf_value, ad_group_id, total_inventory,
         total_debug)
        select view_date, date_id, process_date_id,
               impressions_delivered, clicks, ad_id, country,
               impression_type, affiliate_id, site_id,
               zone_id, content_partner_id, developer_id,
               module_id, data_id, data_source, agent_type,
               internal_ip, flags, atf_value, ad_group_id,
               total_inventory, total_debug
        from   ben_network_revenue_daily
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    l_process_name := p_process_name || '-' ||
                      'data_for_traffic_reports@www';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from insider_data.data_for_traffic_reports@www
        where  date_id = p_process_date;
      end if;
      insert into insider_data.data_for_traffic_reports@www
        (affiliate_id, date_id, impressions_delivered,
         clicks, view_date, marketplace_zone, ad_category,
         ad_dimension, ad_id, ad_rate, impression_type)
        select /*+ parallel(nrd,4,1) */
         nrd.affiliate_id, nrd.date_id,
         sum(nrd.impressions_delivered) impressions_delivered,
         sum(nrd.clicks) clicks, nrd.view_date, null,
         nae.category, nae.ad_size_id, nae.ad_id, nae.rate,
         nrd.impression_type
        from   ben_network_revenue_daily nrd,
               ods_metadata.adm_ads nae
        where  date_id = p_process_date
        and    country = 'WW'
        and    nrd.ad_id = nae.ad_id
        group  by nrd.affiliate_id, nrd.date_id,
                  nrd.view_date, nae.category,
                  nae.ad_size_id, nae.ad_id, nae.rate,
                  nrd.impression_type;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- glamcontent_aggregate
    l_process_name := p_process_name || '-' ||
                      'glamcontent_aggregate@www';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from insider_data.glamcontent_aggregate@www
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
      end if;
      insert into insider_data.glamcontent_aggregate@www
        (view_date, cid, did, mid, affiliate_id,
         video_views, adjacent_ads, content_type, date_id,
         process_date_id, created_date)
        select view_date, cid, did, mid, affiliate_id,
               video_views, adjacent_ads, content_type,
               date_id, process_date_id, created_date
        from   ben_glamcontent_aggregate
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- video_play_data
    l_process_name := p_process_name || '-' ||
                      'video_play_data@www';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from insider_data.video_play_data@www
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
      end if;
      insert into insider_data.video_play_data@www
        (view_date, cid, did, video_views, adjacent_ads,
         pre_roll, over, post_roll, date_id, process_date_id,
         created_date, data_source)
        select view_date, cid, did, video_views,
               adjacent_ads, pre_roll, over, post_roll,
               date_id, process_date_id, created_date,
               data_source
        from   ben_video_play_data
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- module_aggregate
    l_process_name := p_process_name || '-' ||
                      'module_aggregate@www';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from insider_data.module_aggregate@www
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
      end if;
      insert into insider_data.module_aggregate@www
        (view_date, mid, ad_name, affiliate_id, country_id,
         impressions, clicks, date_id, process_date_id,
         created_date)
        select view_date, mid, ad_name, affiliate_id,
               country_id, impressions, clicks, date_id,
               process_date_id, created_date
        from   ben_module_aggregate
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ----yield data
    pkg_yield.load_ben_yield_nrd(p_run_id, p_process_date, p_process_name);
    pkg_yield.load_ben_yield_staging(p_run_id, p_process_date, p_process_name);
    ---- yield_970x66_adserver@www
    l_process_name := p_process_name || '-' ||
                      'yield_970x66_adserver@www';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      insider_data.pkg_generic_ddl.truncate_partition@www('insider_data', 'yield_970x66_adserver_1', 'P_' ||
                                                           p_process_date);
      insert /*+ append */
      into insider_data.yield_970x66_adserver_1@www
        (date_id, affiliate_id, ad_type, ad_size,
         impressions_delivered, clicks, preliminary_earnings,
         country, advertiser, affiliate_product_name,
         ad_category, ad_id)
        select date_id, affiliate_id, ad_type, ad_size,
               impressions_delivered, clicks,
               preliminary_earnings, country, advertiser,
               affiliate_product_name, ad_category, ad_id
        from   ben_yield_970x66_adserver_1
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

end;
/
