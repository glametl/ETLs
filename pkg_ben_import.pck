create or replace package pkg_ben_import as

  procedure run_import
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_dimension_table
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ben_fact_media_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ben_fact_media_inventory
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_atac_atp_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_network_revenue_daily
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_network_revenue_daily_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_network_revenue_monthly
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_gpo_avg_ctr_v2
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure replicate_to_nap
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_ben_import as

  procedure run_import
  (
    p_process_name varchar2,
    p_process_date number
  ) is
    l_run_id        number;
    l_process_date  number;
    l_process_name  varchar2(10);
    l_error_msg     varchar2(4000);
    l_max_date      number;
    l_start_process varchar2(100);
    l_mail_msg      varchar2(100);
    l_to            varchar2(1000);
  begin
    l_process_name  := initcap(p_process_name);
    l_start_process := l_process_name || '-run_import';
    l_mail_msg      := 'BEN Process Import Data';
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
    load_dimension_table(l_run_id, l_process_date, l_process_name);
    -- other agg tables based on base table
    load_ben_fact_media_day(l_run_id, l_process_date, l_process_name);
    load_ben_fact_media_inventory(l_run_id, l_process_date, l_process_name);
    load_network_revenue_daily(l_run_id, l_process_date, l_process_name);
    --load_ben_yield_revenue(l_run_id, l_process_date, l_process_name);
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
        load_network_revenue_daily_agg(l_run_id, i.date_id, l_process_name);
        l_max_date := i.date_id;
      end loop;
      load_network_revenue_monthly(l_run_id, l_max_date, l_process_name);
    else
      load_network_revenue_daily_agg(l_run_id, l_process_date, l_process_name);
      load_network_revenue_monthly(l_run_id, l_process_date, l_process_name);
    end if;
    load_gpo_avg_ctr_v2(l_run_id, l_process_date, l_process_name);
    load_fact_atac_atp_day(l_run_id, l_process_date, l_process_name);
    replicate_to_nap(l_run_id, l_process_date, l_process_name);
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

  procedure load_dimension_table
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'dim_ad_size_request';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_ad_size_request';
      insert /*+ append */
      into dim_ad_size_request
        (ad_size_request, request_dimensions)
        select ad_size_request, request_dimensions
        from   dart_data.dart_ad_size_request@etlben;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    l_process_name := p_process_name || '-' ||
                      'dim_network_ad';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_network_ad';
      insert /*+ append */
      into dim_network_ad
        (ad_id, ad_name, ad_start, ad_end, ad_rate,
         ad_dimension, advertiser_id, advertiser_name,
         impressions_delivered, clicks, last_updated)
        select ad_id, ad_name, ad_start, ad_end, ad_rate,
               ad_dimension, advertiser_id, advertiser_name,
               impressions_delivered, clicks, last_updated
        from   ap_data.network_ad@etlnap;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    l_process_name := p_process_name || '-' ||
                      'ods_metadata.channel';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table ods_metadata.channel';
      insert into ods_metadata.channel
        (id, type_id, parent_id, name, zedo_channel_name,
         zedo_channel_id, creation_date, last_update, status,
         name_contact, phone_contact, email_contact,
         node_type, member_id, title, description,
         main_image, feed_id, rss_feed_url, affiliate_url,
         publish_status, dart_site_id, dart_advertiser_id,
         dart_click_url, sforce_id, brand_rank, rank_scores,
         info_tags, network, country, currency,
         affiliate_type, pepe_dart_tags, list_info_tags,
         parent_affiliate_id, atp_rank, pec, expiration_date,
         ctr_opt_tags, ctr_opt_tags_orig, is_site_targeted,
         site_targeted_ads, withdrawn_date, list_bt_tags,
         editorial_rank, social_rank, privacy_policy_url)
        select id, type_id, parent_id, name,
               zedo_channel_name, zedo_channel_id,
               creation_date, last_update, status,
               name_contact, phone_contact, email_contact,
               node_type, member_id, title, description,
               main_image, feed_id, rss_feed_url,
               affiliate_url, publish_status, dart_site_id,
               dart_advertiser_id, dart_click_url, sforce_id,
               brand_rank, rank_scores, info_tags, network,
               country, currency, affiliate_type,
               pepe_dart_tags, list_info_tags,
               parent_affiliate_id, atp_rank, pec,
               expiration_date, ctr_opt_tags,
               ctr_opt_tags_orig, is_site_targeted,
               site_targeted_ads, withdrawn_date,
               list_bt_tags, editorial_rank, social_rank,
               privacy_policy_url
        from   ap_data.channel@etlnap;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ---this dimension table required during the package yield 
    l_process_name := p_process_name || '-' ||
                      'ben_oms_pdm_ad_product';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table ben_oms_pdm_ad_product';
      insert /*+ append */
      into ben_oms_pdm_ad_product
        (ad_id, affiliate_product_name)
        select distinct pliad.ad_id,
                        pdmad.publisher_product_label as affiliate_product_name
        from   pdm_data.pdm_ad_line_item_ad_product@etlnap pliad,
               pdm_data.pdm_ad_product@etlnap pdmad
        where  pliad.ad_product_id = pdmad.ad_product_id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ben_fact_media_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_fact_media_day';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_fact_media_day truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ben_fact_media_day
        (date_id, ad_id, affiliate_id, site_id, country_id,
         dma_id, city_id, page_id, ad_size_request,
         source_id, agent_type, internal_ip, ad_group_id,
         cid, did, mid, flags, is_atf, developer_id,
         is_marketplace, total_impressions, total_clicks,
         zone_id, state_province, country)
        select /*+ parallel(a,5,1) */
         date_id, ad_id, affiliate_id, site_id, country_id,
         dma_id, city_id, page_id, ad_size_request,
         source_id, agent_type, internal_ip, ad_group_id,
         cid, did, mid, flags, is_atf, developer_id,
         is_marketplace,
         sum(total_impressions) as total_impressions,
         sum(total_clicks) as total_clicks, zone_id,
         state_province, country_code
        --from   fact_media_day a
        from   fact_media_day_agg a
        where  date_id = p_process_date
        group  by date_id, ad_id, affiliate_id, site_id,
                  country_id, dma_id, city_id, page_id,
                  ad_size_request, source_id, agent_type,
                  internal_ip, ad_group_id, cid, did, mid,
                  flags, is_atf, developer_id,
                  is_marketplace, zone_id, state_province,
                  country_code;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_fact_media_day partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ben_fact_media_inventory
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_fact_media_inventory';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_fact_media_inventory truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into campaign.ben_fact_media_inventory
        (date_id, ad_id, affiliate_id, site_id, country_id,
         zone_id, cid, did, mid, developer_id, agent_type,
         internal_ip, flags, atf_value, source_id,
         ad_group_id, total_impressions, total_clicks,
         total_inventory, total_debug, country)
        select /*+ parallel(a,4) */
         date_id, ad_id, affiliate_id, site_id, country_id,
         zone_id, cid, did, mid, developer_id, agent_type,
         internal_ip, flags, atf_value, source_id,
         ad_group_id, sum(total_impressions),
         sum(total_clicks), sum(total_inventory),
         sum(total_debug), country
        from   (select /*+ parallel(a,4) */
                  date_id, ad_id, affiliate_id, site_id,
                  country_id, zone_id, cid, did, mid,
                  developer_id, agent_type, internal_ip, flags,
                  is_atf as atf_value, source_id, ad_group_id,
                  total_impressions, total_clicks,
                  0 as total_inventory, 0 as total_debug,
                  country
                 from   ben_fact_media_day a
                 where  date_id = p_process_date
                 union all
                 select /*+ parallel(a,4) */
                  date_id, ad_id, affiliate_id, site_id,
                  to_number(country_id), zone_id,
                  content_partner_id, data_id, module_id,
                  developer_id, agent_type, internal_ip, flags,
                  atf_value, request_status, ad_group_id,
                  0 as total_impressions, 0 as total_clicks,
                  case
                    when request_seq = 0 then
                     impressions
                    else
                     0
                  end as total_inventory,
                  case
                    when request_seq < 0 then
                     impressions
                    else
                     0
                  end as total_debug, country_code
                 from   fact_ghost_imps_day a
                 where  date_id = p_process_date)
        group  by date_id, ad_id, affiliate_id, site_id,
                  country_id, zone_id, cid, did, mid,
                  developer_id, agent_type, internal_ip,
                  flags, atf_value, source_id, ad_group_id,
                  country;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_fact_media_inventory partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  --2867
  procedure load_fact_atac_atp_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_fact_atac_atp_day';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_fact_atac_atp_day truncate partition P_' ||
                        p_process_date;
      /*insert \*+ append *\
      into ben_fact_atac_atp_day
        (date_id, view_date, affiliate_id, atp_rank, pec,
         ad_dimension, ad_category, flags, is_atf,
         is_marketplace, country_id, total_impressions,
         total_clicks, created_by, created_date, updated_by,
         updated_date, data_source, ad_group_id,
         ctr_optimized, ad_sub_category)
        select \*+ parallel(a,4,1) *\
         a.date_id, to_date(a.date_id, 'yyyymmdd'),
         cag.affiliate_id as affiliate_id, cag.atp_rank,
         cag.pec, nad.ad_size_id, nad.category, a.flags,
         a.is_atf,
         case
           when flags > 0 then
            decode(bitand(flags, 16), 16, 1, 0)
           else
            0
         end as is_marketplace, a.country_id,
         sum(total_impressions) as total_impressions,
         sum(total_clicks) as total_clicks,
         'Glam' as created_by, sysdate as created_date,
         'Glam' as updated_by, sysdate as updated_date,
         --decode(a.request_status, 0, 2, a.request_status),
         a.source_id, a.ad_group_id,
         nad.ctr_optimization_enabled, nad.sub_category
        from   ben_fact_media_day a,
               --from   fact_media_day_agg a,
               ods_metadata.adm_ads nad,
               ods_metadata.channel_aggregate cag,
               dim_zone d
        where  date_id = p_process_date
        and    a.ad_id = nad.ad_id
        and    nad.ad_size_id in
               ('160x600', '300x250', '300x600', '728x90', '800x150', '630x150', '270x150', '800x50', '160x160', '650x35', '984x258', '990x26', '728x91', '120x600')
        --and    a.site_id = cag.dart_site_id
        and    a.affiliate_id = cag.affiliate_id
        and    cag.category_type_full = 'Primary'
        and    a.page_id = d.zone_id
        and    d.callback_zone = 0
        group  by a.date_id, cag.affiliate_id, cag.atp_rank,
                  cag.pec, nad.ad_size_id, nad.category,
                  a.flags, a.is_atf, a.country_id,
                  a.source_id, a.ad_group_id,
                  nad.ctr_optimization_enabled,
                  nad.sub_category;*/
      insert /*+ append */
      into ben_fact_atac_atp_day
        (date_id, view_date, affiliate_id, atp_rank, pec,
         ad_dimension, ad_category, flags, is_atf,
         is_marketplace, country_id, total_impressions,
         total_clicks, data_source, ad_group_id,
         ctr_optimized, ad_sub_category)
        select /*+ parallel(a,4,1) */
         a.date_id, to_date(a.date_id, 'yyyymmdd'),
         cag.affiliate_id as affiliate_id, cag.atp_rank,
         cag.pec, nad.ad_size_id, nad.category, a.flags,
         a.is_atf,
         case
           when flags > 0 then
            decode(bitand(flags, 16), 16, 1, 0)
           else
            0
         end as is_marketplace, a.country_id,
         sum(total_impressions) as total_impressions,
         sum(total_clicks) as total_clicks, a.source_id,
         a.ad_group_id, nad.ctr_optimization_enabled,
         nad.sub_category
        from   ben_fact_media_day a,
               ods_metadata.adm_ads nad,
               ods_metadata.channel_aggregate cag
        --, dim_zone d
        where  date_id = p_process_date
        and    a.ad_id = nad.ad_id
        and    nad.ad_size_id in
               ('160x600', '300x250', '300x600', '728x90', '800x150', '630x150', '270x150', '800x50', '160x160', '650x35', '984x258', '990x26', '728x91', '120x600')
              --and    a.site_id = cag.dart_site_id
        and    a.affiliate_id = cag.affiliate_id
        and    cag.category_type_full = 'Primary'
              --and    a.page_id = d.zone_id
              --and    d.callback_zone = 0
        and    page_id in
               (select zone_id
                 from   dim_zone
                 where  zone_id like '-2%'
                 and    callback_zone = 0)
        group  by a.date_id, cag.affiliate_id, cag.atp_rank,
                  cag.pec, nad.ad_size_id, nad.category,
                  a.flags, a.is_atf, a.country_id,
                  a.source_id, a.ad_group_id,
                  nad.ctr_optimization_enabled,
                  nad.sub_category;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_fact_atac_atp_day partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  --2328
  procedure load_network_revenue_daily
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_network_revenue_daily';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table ben_all_network_modules';
      insert into ben_all_network_modules
        select a.mid, name,
               decode(b.mid, null, 'GAP', 'GTV') app_type,
               developer_id, height, width
        from   insider_data.insider_modules@etlwww a,
               insider_data.insider_glamtv_groups@etlwww b
        where  a.mid = b.mid(+);
      execute immediate 'Alter table ben_network_revenue_daily truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ben_network_revenue_daily
        (view_date, date_id, process_date_id,
         impressions_delivered, clicks, ad_id, country,
         impression_type, affiliate_id, site_id, zone_id,
         content_partner_id, developer_id, module_id,
         data_id, data_source, agent_type, internal_ip,
         flags, atf_value, ad_group_id, total_inventory,
         total_debug, imps_type)
        select trunc(to_date(p_process_date, 'yyyymmdd'), 'dd') view_date,
               p_process_date, p_process_date,
               sum(a.impressions_delivered) impressions_delivered,
               sum(a.clicks) clicks, a.ad_id, a.country,
               coalesce(decode(anm.app_type, 'GAP', 'A', 'GTV', 'G', null), a.impression_type) as impression_type,
               coalesce(a.affiliate_id, -1) affiliate_id,
               coalesce(a.site_id, -1) site_id,
               coalesce(a.zone_id, -1) zone_id, a.cid,
               a.developer_id, a.mid, a.did,
               coalesce(a.source_id, 2), a.agent_type,
               a.internal_ip, a.flags, a.atf_value,
               a.ad_group_id, sum(total_inventory),
               sum(total_debug), a.impression_type
        from   (select /*+ parallel(ni,4,1) */
                  sum(total_impressions) impressions_delivered,
                  sum(total_clicks) clicks, ni.ad_id,
                  decode(country_id, 256, 'US', 257, 'US', 35, 'CA', 12, 'AU', 68, 'UK', 86, 'DE', 114, 'JP', 78, 'FR', 'ROW') country,
                  case
                    when decode(bitand(flags, 16), 16, 1, 0) = 1 then
                     'M'
                    else
                     'P'
                  end as impression_type, ni.affiliate_id,
                  ni.site_id, zone_id, ni.cid, ni.developer_id,
                  ni.mid, ni.did, ni.agent_type,
                  ni.internal_ip, ni.flags, atf_value,
                  ni.source_id, ni.ad_group_id,
                  sum(total_inventory) total_inventory,
                  sum(total_debug) total_debug
                 from   ben_fact_media_inventory ni
                 where  ni.date_id = p_process_date
                 /*and    ni.country_id in
                 (256, 257, 35, 12, 68, 86, 114, 78)*/
                 group  by ni.ad_id, ni.country_id,
                           ni.developer_id, ni.affiliate_id,
                           ni.site_id, zone_id, ni.cid, ni.mid,
                           ni.did, ni.agent_type,
                           ni.internal_ip, ni.flags, atf_value,
                           ni.source_id, ni.ad_group_id
                 union all
                 select /*+ parallel(ni,4,1) */
                  sum(total_impressions) impressions_delivered,
                  sum(total_clicks) clicks, ni.ad_id,
                  'WW' country,
                  case
                    when decode(bitand(flags, 16), 16, 1, 0) = 1 then
                     'M'
                    else
                     'P'
                  end as impression_type, ni.affiliate_id,
                  ni.site_id, zone_id, ni.cid, ni.developer_id,
                  ni.mid, ni.did, ni.agent_type,
                  ni.internal_ip, ni.flags, atf_value,
                  ni.source_id, ni.ad_group_id,
                  sum(total_inventory) total_inventory,
                  sum(total_debug) total_debug
                 from   ben_fact_media_inventory ni
                 where  ni.date_id = p_process_date
                 group  by ni.ad_id, ni.affiliate_id,
                           ni.developer_id, ni.site_id,
                           zone_id, ni.cid, ni.mid, ni.did,
                           ni.agent_type, ni.internal_ip,
                           ni.flags, atf_value, ni.source_id,
                           ni.ad_group_id) a,
               ben_all_network_modules anm
        where  a.mid = anm.mid(+)
        and    a.affiliate_id is not null
        and    a.site_id is not null
        and    a.ad_id is not null
        group  by a.ad_id, a.country,
                  coalesce(decode(anm.app_type, 'GAP', 'A', 'GTV', 'G', null), a.impression_type),
                  coalesce(a.affiliate_id, -1),
                  a.developer_id, coalesce(a.site_id, -1),
                  coalesce(a.zone_id, -1), a.cid, a.mid,
                  a.did, a.agent_type, a.internal_ip,
                  a.flags, a.atf_value,
                  coalesce(a.source_id, 2), a.ad_group_id,
                  a.impression_type;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_network_revenue_daily partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_network_revenue_daily_agg
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
                      'ben_network_revenue_daily_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if to_char(to_date(p_process_date, 'yyyymmdd'), 'dd') = '01' then
        l_previous_dt := '';
      else
        l_previous_dt := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd'));
      end if;
      execute immediate 'Alter table ben_network_revenue_daily_agg truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ben_network_revenue_daily_agg
        (view_date, date_id, process_date_id,
         impressions_delivered, clicks, ad_id, country,
         impression_type, affiliate_id, site_id, zone_id,
         content_partner_id, developer_id, module_id,
         data_id, data_source, agent_type, internal_ip,
         flags, atf_value, ad_group_id, total_inventory,
         total_debug)
        select trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'),
               p_process_date, max(process_date_id),
               sum(impressions_delivered), sum(clicks),
               ad_id, country, impression_type, affiliate_id,
               site_id, zone_id, content_partner_id,
               developer_id, module_id, data_id, data_source,
               agent_type, internal_ip, flags, atf_value,
               ad_group_id, sum(total_inventory),
               sum(total_debug)
        from   (select /*+ parallel(nrd,4,1) */
                  ad_id, process_date_id,
                  impressions_delivered, clicks, country,
                  impression_type, affiliate_id, site_id,
                  zone_id, content_partner_id, developer_id,
                  module_id, data_id, data_source, agent_type,
                  internal_ip, flags, atf_value, ad_group_id,
                  total_inventory, total_debug
                 from   ben_network_revenue_daily_agg nrd
                 where  date_id = l_previous_dt
                 union all
                 select /*+ parallel(nrd,4,1) */
                  ad_id, date_id as process_date_id,
                  impressions_delivered, clicks, country,
                  impression_type, affiliate_id, site_id,
                  zone_id, content_partner_id, developer_id,
                  module_id, data_id, data_source, agent_type,
                  internal_ip, flags, atf_value, ad_group_id,
                  total_inventory, total_debug
                 from   ben_network_revenue_daily nrd
                 where  date_id = p_process_date)
        group  by ad_id, country, impression_type,
                  affiliate_id, site_id, zone_id,
                  content_partner_id, developer_id,
                  module_id, data_id, data_source,
                  agent_type, internal_ip, flags, atf_value,
                  ad_group_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_network_revenue_daily_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  --2328
  procedure load_network_revenue_monthly
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
                      'ben_network_revenue_monthly';
    l_month_start  := to_char(to_date(p_process_date, 'yyyymmdd'), 'yyyymm') || '01';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_network_revenue_monthly truncate partition P_' ||
                        l_month_start;
      insert /*+ append */
      into ben_network_revenue_monthly
        (view_date, date_id, process_date_id, max_date_id,
         impressions_delivered, clicks, ad_id, country,
         impression_type, affiliate_id, site_id, zone_id,
         content_partner_id, developer_id, module_id,
         data_id, data_source, agent_type, internal_ip,
         flags, atf_value, ad_group_id, total_inventory,
         total_debug)
        select /*+ parallel(nrd,4,1) */
         view_date, l_month_start, process_date_id,
         process_date_id, impressions_delivered, clicks,
         ad_id, country, impression_type, affiliate_id,
         site_id, zone_id, content_partner_id, developer_id,
         module_id, data_id, data_source, agent_type,
         internal_ip, flags, atf_value, ad_group_id,
         total_inventory, total_debug
        from   ben_network_revenue_daily_agg nrd
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_network_revenue_monthly partition(P_' ||
                        l_month_start ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_gpo_avg_ctr_v2
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_fact_gpo_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        execute immediate 'Alter table ben_fact_gpo_agg truncate partition P_' ||
                          p_process_date;
        for i in (select partition_name
                  from   user_tab_partitions
                  where  table_name = 'BEN_FACT_GPO_AGG'
                  and    to_number(replace(partition_name, 'P_', '')) <
                         to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 180, 'yyyymmdd'))) loop
          begin
            execute immediate 'Alter table ben_fact_gpo_agg drop partition ' ||
                              i.partition_name;
          exception
            when others then
              null;
          end;
        end loop;
      end if;
      insert /*+ append */
      into ben_fact_gpo_agg
        (date_id, ad_id, affiliate_id, country,
         impression_type, total_impressions, total_clicks)
        select /*+ parallel(a,4,1) */
         date_id, a.ad_id, affiliate_id, country,
         'P' as impression_type,
         sum(total_impressions) impressions_delivered,
         sum(total_clicks) clicks
        from   ben_fact_media_inventory a,
               ods_metadata.adm_ads b
        where  date_id = p_process_date
        and    decode(bitand(flags, 16), 16, 1, 0) = 0
        and    country in
               ('US', 'UK', 'CA', 'DE', 'AT', 'CH', 'JP', 'KR')
        and    upper(category) not in
               ('PUBLISHER DEFAULT ADS', 'INTERNAL')
        and    a.ad_id = b.ad_id
        group  by date_id, a.ad_id, affiliate_id, country;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_fact_gpo_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    if upper(p_process_name) in ('PROCESS') then
      l_process_name := p_process_name || '-' ||
                        'ben_gpo_avg_ctr_v2';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        execute immediate 'truncate table ben_gpo_avg_ctr_v2';
        insert /*+ append */
        into ben_gpo_avg_ctr_v2
          (affiliate_id, ad_industry, ad_industry_abv,
           ad_dimension, ad_dimension_id, country,
           impressions_delivered, clicks, ctr, process_date)
          with src_ad_industry as
           (select offer_industry as ad_industry,
                   offer_industry_abv as ad_industry_abv,
                   ad_dimension, ad_dimension_id, ad_id
            from   ods_metadata.network_ad_extended
            where  ad_category = 'Advertiser'
            and    offer_industry_abv is not null
            union
            select ad_industry, ad_industry_abv, ad_dimension,
                   ad_dimension_id, ad_id
            from   ods_metadata.network_ad_extended
            where  ad_category = 'Advertiser'
            and    offer_industry_abv is null),
          nrm as
           (select /*+ parallel(a,6,1) */
             affiliate_id, ad_id, country,
             sum(total_impressions) total_impressions,
             sum(total_clicks) total_clicks
            from   ben_fact_gpo_agg a
            where  a.date_id >=
                   to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 180, 'yyyymmdd'))
            group  by affiliate_id, ad_id, country),
          gpo_avg as
           (select affiliate_id, ad_industry, ad_industry_abv,
                   ad_dimension, ad_dimension_id, country,
                   sum(total_impressions) as impressions_delivered,
                   sum(total_clicks) as clicks,
                   round(decode(sum(total_impressions), 0, 0, sum(total_clicks) /
                                  sum(total_impressions) * 100), 4) as ctr
            from   nrm a, src_ad_industry nae
            where  a.ad_id = nae.ad_id
            group  by affiliate_id, ad_industry,
                      ad_industry_abv, ad_dimension,
                      ad_dimension_id, country)
          select affiliate_id, ad_industry, ad_industry_abv,
                 ad_dimension, ad_dimension_id, country,
                 impressions_delivered, clicks, ctr,
                 p_process_date
          from   gpo_avg
          where  impressions_delivered >= 100
          and    ctr <= 5;
        l_cnt := sql%rowcount;
        commit;
        execute immediate 'analyze table ben_gpo_avg_ctr_v2 estimate statistics';
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- gpo_avg_ctr_v2@nap
      l_process_name := p_process_name || '-' ||
                        'gpo_avg_ctr_v2@etlnap';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        --ap_data.pkg_generic_ddl.truncate_table@nap('ap_data', 'gpo_avg_ctr_v2');
        delete from ap_data.gpo_avg_ctr_v2@etlnap;
        insert into ap_data.gpo_avg_ctr_v2@etlnap
          (affiliate_id, ad_industry, ad_industry_abv,
           ad_dimension, ad_dimension_id, country,
           impressions_delivered, clicks, ctr)
          select affiliate_id, ad_industry, ad_industry_abv,
                 ad_dimension, ad_dimension_id, country,
                 impressions_delivered, clicks, ctr
          from   ben_gpo_avg_ctr_v2;
        l_cnt := sql%rowcount;
        commit;
        ap_data.pkg_generic_ddl.analyze_table@etlnap('ap_data', 'gpo_avg_ctr_v2');
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- ben_gpo_ad_industries
      l_process_name := p_process_name || '-' ||
                        'gpo_ad_industries@etlnap';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        --ap_data.pkg_generic_ddl.truncate_table@nap('ap_data', 'gpo_ad_industries');
        delete from ap_data.gpo_ad_industries@etlnap;
        insert into ap_data.gpo_ad_industries@etlnap
          (ad_industry, ad_industry_abv)
          select distinct ad_industry, ad_industry_abv
          from   ben_gpo_avg_ctr_v2;
        l_cnt := sql%rowcount;
        commit;
        ap_data.pkg_generic_ddl.analyze_table@etlnap('ap_data', 'gpo_ad_industries');
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
    end if;
  end;

  procedure replicate_to_nap
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_sql          varchar2(30000);
    l_month_start  number;
  begin
    -- network_revenue_daily
    l_process_name := p_process_name || '-' ||
                      'network_revenue_daily@etlnap';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ap_data.network_revenue_daily where date_id = ' ||
                 p_process_date;
        ap_data.pkg_generic_ddl.sql_from_ods@etlnap(l_sql);
      end if;
      insert into ap_data.network_revenue_daily@etlnap
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
      ap_data.pkg_generic_ddl.analyze_partition@etlnap('ap_data', 'network_revenue_daily', 'P_' ||
                                                        p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- network_revenue_monthly
    l_process_name := p_process_name || '-' ||
                      'network_revenue_monthly@etlnap';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      l_month_start := to_char(to_date(p_process_date, 'yyyymmdd'), 'yyyymm') || '01';
      l_sql         := 'delete from ap_data.network_revenue_monthly where date_id =' ||
                       l_month_start;
      ap_data.pkg_generic_ddl.sql_from_ods@etlnap(l_sql);
      insert into ap_data.network_revenue_monthly@etlnap
        (view_date, date_id, process_date_id, max_date_id,
         impressions_delivered, clicks, ad_id, country,
         impression_type, affiliate_id, site_id, zone_id,
         content_partner_id, developer_id, module_id,
         data_id, data_source, agent_type, internal_ip,
         flags, atf_value, ad_group_id, total_inventory,
         total_debug)
        select /*+ PARALLEL (a,8,1) */
         view_date, date_id, process_date_id, max_date_id,
         impressions_delivered, clicks, ad_id, country,
         impression_type, affiliate_id, site_id, zone_id,
         content_partner_id, developer_id, module_id,
         data_id, data_source, agent_type, internal_ip,
         flags, atf_value, ad_group_id, total_inventory,
         total_debug
        from   ben_network_revenue_monthly a
        where  date_id = l_month_start;
      l_cnt := sql%rowcount;
      commit;
      ap_data.pkg_generic_ddl.analyze_partition@etlnap('ap_data', 'network_revenue_monthly', 'P_' ||
                                                        l_month_start);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- fact_atac_atp_day
    l_process_name := p_process_name || '-' ||
                      'fact_atac_atp_day@etlnap';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from ap_data.fact_atac_atp_day where view_date = to_date(' ||
                 p_process_date || ', ''yyyymmdd'')';
        ap_data.pkg_generic_ddl.sql_from_ods@etlnap(l_sql);
      end if;
      insert into ap_data.fact_atac_atp_day@etlnap
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
      ap_data.pkg_generic_ddl.analyze_partition@etlnap('ap_data', 'fact_atac_atp_day', 'P_' ||
                                                        p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

end;
/
