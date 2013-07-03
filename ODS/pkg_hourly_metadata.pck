create or replace package pkg_hourly_metadata as

  l_admads_column_list varchar2(4000);

  l_admads_value_list varchar2(4000);

  procedure run_hourly_metadata
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_adm_ads
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure copy_to_all_schema
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2,
    p_schema_name  varchar2,
    p_dblink_name  varchar2
  );

end;
/
create or replace package body pkg_hourly_metadata as

  procedure run_hourly_metadata
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
    l_message       varchar2(1000) := 'Duplicate AD_ID found in the view. Please check the view and then rerun ';
    l_cnt           number := 0;
  begin
    l_process_name  := initcap(p_process_name);
    l_start_process := l_process_name ||
                       '-run_hourly_metadata';
    l_mail_msg      := 'Adm Ads Data Refresh';
    --l_to            := 'odsimportreport@glam.com';
    l_to := 'nileshm@glam.com,sahilt@glam.com';
    if p_process_date is null then
      l_process_date := to_number(to_char(sysdate - 1, 'yyyymmddhh24'));
    elsif p_process_date is not null then
      l_process_date := p_process_date;
    end if;
    select max(run_id)
    into   l_run_id
    from   daily_process_log
    where  substr(process_date, 1, 8) =
           substr(l_process_date, 1, 8)
    and    upper(process_name) = upper(l_start_process);
    if l_run_id is null then
      l_run_id := seq_daily_campaign_run.nextval;
      -- log starting of the process
      pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'I');
      -- send intimation mail for starting of process
      pa_send_email1.compose_email(l_run_id, 'START', l_process_date, l_mail_msg, l_start_process, l_to);
    end if;
    -- set the main process status as Running sothat there are no multiple entries for the main process
    update daily_process_log
    set    process_date = l_process_date,
           start_time = sysdate, status = 'RUNNING',
           error_reason = '', end_time = ''
    where  run_id = l_run_id
    and    upper(process_name) = upper(l_start_process);
    commit;
    select count(1)
    into   l_cnt
    from   (select ad_id
             from   vw_adm_ads a
             group  by ad_id
             having count(1) > 1);
    if l_cnt = 0 then
      load_adm_ads(l_run_id, l_process_date, l_process_name);
      -- mark the process as complete in log table
      pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'UC');
      if to_number(to_char(to_date(l_process_date, 'yyyymmddhh24'), 'hh24')) = 0 then
        -- send completion mail
        pa_send_email1.compose_email(l_run_id, 'COMPLETE', l_process_date, l_mail_msg, l_start_process, l_to);
      end if;
    else
      pa_send_email1.compose_email(l_run_id, 'FAIL', l_process_date, l_message, l_start_process, l_to);
    end if;
  exception
    when others then
      -- rollback the transaction from where exception is generated.
      rollback;
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      -- failed steps are handled in compose mail message procedure
      pa_send_email1.compose_email(l_run_id, 'FAIL', l_process_date, l_mail_msg, l_start_process, l_to);
  end;

  procedure load_adm_ads
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'adm_targeting';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from adm_targeting;
      insert into adm_targeting
        (targeting_type, targeting_id, targeting_key_value,
         source_id)
        select targeting_type, targeting_id,
               targeting_key_value, source_id
        from   adm_data.adm_targeting@etlgad
        where  trim(upper(targeting_type)) in
               ('ORDER', 'ADVERTISER');
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    l_admads_column_list := '(active, adexclusions, advertiser_active, advertiser_gad_country,
           advertiser_id, advertiser_info_value, advertiser_name,
           advertiser_updated_on, ad_id, ad_size_id, ad_type, agency,
           alternate_id, area_code, bandwidth, billing_source, booked_impressions,
           campaign_manager, category, cities, contains_survey, contain_floating_ad,
           countries, creative_count, dart_ad_id, dart_advertiser_id, dart_order_id,
           delivery_scheduling, delivery_type, dma, end_date, frequency, frequency_duration,
           frequency_duration_type, info_value, isp_network_code, is_approved, is_inc_area_code,
           is_inc_bandwidth, is_inc_country, is_inc_dma, is_inc_isp_network_code,
           is_inc_operating_system, is_inc_site, is_inc_site_zone, is_inc_state_region,
           is_inc_web_browser, is_inc_zip, keyword, launch_timezone, media_type, name,
           oms_advertiser_id, oms_order_id, operating_system, order_active, order_booked_impressions,
           order_end_date, order_id, order_name, order_pricing_type, order_rate, order_start_date,
           order_value, over_deliver, parent_advertiser, pricing_source, pricing_type, priority,
           process_updated_on, proposal_value, rate, rate_type, rep_firm, sales_person,
           sales_planner, same_adv_exception, second_campaign_manager, site, site_zone,
           source_id, start_date, state_region, sub_category, table_source_type, targeting_key_value,
           tile_category, time_delivery_type, updated_on, value, web_browser, weight, zip,
           delivery_progress, override_area_code, override_bandwidth, override_country,
           override_day_parting, override_dma, override_network_code, override_operating_system,
           override_site, override_site_zone, override_state_region, override_targeting_key_value,
           override_web_browser, override_zip, preset_key_value_exp, ad_industry,
           is_inc_day_parting, day_parting, region_group_id, override_region_group,
           override_exclusion, city, is_inc_city, override_city, currency, override_currency,
           subsidary, override_subsidary, meta_country, override_meta_country, offer_industry,
           offer_industry_abv, ctr_optimization_enabled, use_ad_frequency_cap, use_imp_del_max,
           imp_del_max, imp_del_max_dur, imp_del_max_dur_type, booked_impressions_unlimited,
           group_id, creative_preset_id, ad_group_name)';
    l_admads_value_list  := 'active, adexclusions, advertiser_active, advertiser_gad_country, advertiser_id,
               advertiser_info_value, advertiser_name, advertiser_updated_on, ad_id, ad_size_id,
               ad_type, agency, alternate_id, area_code, bandwidth, billing_source,
               booked_impressions, campaign_manager, category, cities, contains_survey,
               contain_floating_ad, countries, creative_count, dart_ad_id, dart_advertiser_id,
               dart_order_id, delivery_scheduling, delivery_type, dma, end_date, frequency,
               frequency_duration, frequency_duration_type, info_value, isp_network_code,
               is_approved, is_inc_area_code, is_inc_bandwidth, is_inc_country, is_inc_dma,
               is_inc_isp_network_code, is_inc_operating_system, is_inc_site, is_inc_site_zone,
               is_inc_state_region, is_inc_web_browser, is_inc_zip, keyword, launch_timezone,
               media_type, name, oms_advertiser_id, oms_order_id, operating_system, order_active,
               order_booked_impressions, order_end_date, order_id, order_name, order_pricing_type,
               order_rate, order_start_date, order_value, over_deliver, parent_advertiser,
               pricing_source, pricing_type, priority, process_updated_on, proposal_value, rate,
               rate_type, rep_firm, sales_person, sales_planner, same_adv_exception,
               second_campaign_manager, site, site_zone, source_id, start_date, state_region,
               sub_category, table_source_type, targeting_key_value, tile_category,
               time_delivery_type, updated_on, value, web_browser, weight, zip,
               delivery_progress, override_area_code, override_bandwidth, override_country,
               override_day_parting, override_dma, override_network_code, override_operating_system,
               override_site, override_site_zone, override_state_region, override_targeting_key_value,
               override_web_browser, override_zip, preset_key_value_exp, ad_industry,
               is_inc_day_parting, day_parting, region_group_id, override_region_group,
               override_exclusion, city, is_inc_city, override_city, currency, override_currency,
               subsidary, override_subsidary, meta_country, override_meta_country, offer_industry,
               offer_industry_abv, ctr_optimization_enabled, use_ad_frequency_cap, use_imp_del_max,
               imp_del_max, imp_del_max_dur, imp_del_max_dur_type, booked_impressions_unlimited,
               group_id, creative_preset_id, ad_group_name';
    ---
    l_process_name := p_process_name || '-' ||
                      'adm_ads@etlods';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from ods_metadata.adm_ads;
      execute immediate ' insert into ods_metadata.adm_ads ' ||
                        l_admads_column_list || ' select ' ||
                        l_admads_value_list ||
                        ' from vw_adm_ads';
      l_cnt := sql%rowcount;
      commit;
      update ods_metadata.adm_ads ad
      set    advertiser_keyvalue =
              (select targeting_key_value
               from   adm_targeting tg
               where  trim(upper(targeting_type)) =
                      'ADVERTISER'
               and    targeting_key_value is not null
               and    tg.targeting_id = ad.advertiser_id
               and    tg.source_id = ad.source_id)
      where  (advertiser_id, source_id) in
             (select targeting_id, source_id
              from   adm_targeting tg
              where  trim(upper(targeting_type)) =
                     'ADVERTISER'
              and    targeting_key_value is not null);
      update ods_metadata.adm_ads ad
      set    order_keyvalue =
              (select targeting_key_value
               from   adm_targeting tg
               where  trim(upper(targeting_type)) = 'ORDER'
               and    targeting_key_value is not null
               and    tg.targeting_id = ad.order_id
               and    tg.source_id = ad.source_id)
      where  (order_id, source_id) in
             (select targeting_id, source_id
              from   adm_targeting tg
              where  trim(upper(targeting_type)) = 'ORDER'
              and    targeting_key_value is not null);
      commit;
      execute immediate 'analyze table ods_metadata.adm_ads estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    for i in (select level,
                     case
                       when level = 1 then
                        'bt_data'
                       when level = 2 then
                        'ap_data'
                       when level = 3 then
                        'campaign'
                     end as schema_name,
                     case
                       when level = 1 then
                        'etlbta'
                       when level = 2 then
                        'etlnap'
                       when level = 3 then
                        'etladq'
                     end as dblink_name
              from   dual
              connect by level <= 3) loop
      copy_to_all_schema(p_run_id, p_process_date, p_process_name, i.schema_name, i.dblink_name);
    end loop;
  end;

  procedure copy_to_all_schema
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2,
    p_schema_name  varchar2,
    p_dblink_name  varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      p_schema_name || '.adm_ads@' ||
                      p_dblink_name;
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate ' delete from ' || p_schema_name ||
                        '.adm_ads@' || p_dblink_name;
      execute immediate ' insert into ' || p_schema_name ||
                        '.adm_ads@' || p_dblink_name ||
                        l_admads_column_list || ' select ' ||
                        l_admads_value_list ||
                        ' from ods_metadata.adm_ads';
      l_cnt := sql%rowcount;
      commit;
      if p_schema_name = 'ap_data' then
        ap_data.pkg_generic_ddl.analyze_table@etlnap(p_schema_name, 'adm_ads');
      elsif p_schema_name = 'bt_data' then
        bt_data.pkg_generic_ddl.analyze_table@etlbta(p_schema_name, 'adm_ads');
      elsif p_schema_name = 'campaign' then
        campaign.pkg_generic_ddl.analyze_table@etladq(p_schema_name, 'adm_ads');
      end if;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

end;
/
