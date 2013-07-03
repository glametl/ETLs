create or replace package pkg_evolution as

  procedure run_evolution
  (
    p_process_name varchar2,
    p_process_date number default null
  );

  procedure load_fact_media_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_atako_agg_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_media_camp_agg_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_atako_agg_adsize
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_ad_summary
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_product_summary
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure dim_product_average_metrics
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure update_evo_ad_summary
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_product_index
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_product_ad_size
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_product_ext
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_ibt
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_chd
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_delivery_data
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_placement
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_geo_countries
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_geo_states
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_order_summary
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_advertiser_summary
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_atako_ad
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure replicate_to_evolution
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_product_other
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure evo_product_standard_media
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure manage_partitions
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_evolution as

  procedure run_evolution
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
    l_start_process := l_process_name || '-run_evolution';
    l_mail_msg      := 'Evolution Data Refresh';
    l_to            := 'nileshm@glam.com,rahulk@glam.com,monikad@glam.com,sahilt@glam.com';
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
    -- send intimation mail for starting of process
    pa_send_email1.compose_email(l_run_id, 'START', l_process_date, l_mail_msg, l_start_process, l_to);
    -- log starting of the process
    delete from daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) = upper(l_start_process)
    and    run_id = l_run_id
    and    status in ('RUNNING', 'FAIL');
    commit;
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'I');
    -- do the partition management
    if upper(l_process_name) = 'REPROCESS' and
       substr(l_process_date, 7, 8) = '01' then
      manage_partitions(l_run_id, l_process_date, l_process_name);
    end if;
    --load_fact_media_day(l_run_id, l_process_date, l_process_name);
    evo_ad_summary(l_run_id, l_process_date, l_process_name);
    if upper(l_process_name) = 'MANUAL' or
       (upper(l_process_name) = 'REPROCESS' and
        l_process_date <>
        to_number(to_char(sysdate - 1, 'yyyymmdd'))) then
      for i in (select date_id
                from   dim_date
                where  date_id >= l_process_date
                and    date_id <=
                       to_number(to_char(sysdate - 1, 'yyyymmdd'))
                order  by date_id) loop
        evo_atako_agg_day(l_run_id, i.date_id, l_process_name);
        evo_media_camp_agg_day(l_run_id, i.date_id, l_process_name);
        evo_atako_agg_adsize(l_run_id, i.date_id, l_process_name);
      end loop;
    else
      evo_media_camp_agg_day(l_run_id, l_process_date, l_process_name);
      evo_atako_agg_day(l_run_id, l_process_date, l_process_name);
      evo_atako_agg_adsize(l_run_id, l_process_date, l_process_name);
    end if;
    if upper(l_process_name) not in ('MANUAL') then
      evo_product_summary(l_run_id, l_process_date, l_process_name);
      dim_product_average_metrics(l_run_id, l_process_date, l_process_name);
      update_evo_ad_summary(l_run_id, l_process_date, l_process_name);
      evo_product_index(l_run_id, l_process_date, l_process_name);
      evo_product_ad_size(l_run_id, l_process_date, l_process_name);
      evo_product_ext(l_run_id, l_process_date, l_process_name);
      evo_order_summary(l_run_id, l_process_date, l_process_name);
      evo_advertiser_summary(l_run_id, l_process_date, l_process_name);
      evo_product_other(l_run_id, l_process_date, l_process_name);
      evo_product_standard_media(l_run_id, l_process_date, l_process_name);
    end if;
    evo_atako_ad(l_run_id, l_process_date, l_process_name);
    evo_ibt(l_run_id, l_process_date, l_process_name);
    evo_chd(l_run_id, l_process_date, l_process_name);
    evo_placement(l_run_id, l_process_date, l_process_name);
    evo_geo_countries(l_run_id, l_process_date, l_process_name);
    evo_geo_states(l_run_id, l_process_date, l_process_name);
    evo_delivery_data(l_run_id, l_process_date, l_process_name);
    replicate_to_evolution(l_run_id, l_process_date, l_process_name);
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

  procedure load_fact_media_day
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
    /*l_process_name := p_process_name || '-' ||
                      'fact_media_day';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('PROCESS') then
        for i in (select partition_name
                  from   user_tab_partitions
                  where  table_name = 'FACT_MEDIA_DAY'
                  and    to_number(replace(partition_name, 'P_', '')) <
                         to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 45, 'yyyymmdd'))
                  order  by partition_position) loop
          begin
            execute immediate 'Alter table fact_media_day drop partition ' ||
                              i.partition_name;
          exception
            when others then
              null;
          end;
        end loop;
      end if;
      execute immediate 'Alter table fact_media_day truncate partition P_' ||
                        p_process_date;
      execute immediate '
        insert \*+ append *\
        into fact_media_day
            (date_id, advertiser_id, order_id, ad_id, site_id,
             affiliate_id, dma_id, country_id, state_province,
             city_id, zip_code, browser_id, connection_type_id,
             page_id, os_id, is_bt, is_atf, is_homepage, cid, did,
             mid, flags, total_impressions, total_clicks, source_id,
             creative_id, tile, agent_type, internal_ip, ga_city,
             ad_group_id, ad_size_request, developer_id,
             country_code, ga_os_id, ga_browser_id, zone_id,
             is_marketplace, tag_type, request_seq, nad_dimension,
             filtered_clicks, ad_category, hours)
          select \*+ parallel (a,8,1) *\
             date_id, advertiser_id, order_id, ad_id, site_id,
             affiliate_id, dma_id, country_id, state_province,
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
             end as is_marketplace, tag_type, request_seq,
             nad_dimension, sum(filtered_clicks) as filtered_clicks,
             category, extract(hour from cast(time_stamp as timestamp))
          from   adaptive_data.' ||
                        l_dedup_table || ' a 
          group  by date_id, advertiser_id, order_id, ad_id,
                    site_id, affiliate_id, dma_id, country_id,
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
                    extract(hour from cast(time_stamp as timestamp))';
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_media_day partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;*/
    ------ga_imp_clk
    /*if upper(p_process_name) = 'PROCESS' then
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
                       to_char(to_date(p_process_date, 'yyyymmdd') - 10, 'yyyymmdd')) loop
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
        insert \*+ append *\
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
                 lower(:p_process_name) as process_name
          --from   adaptive_data.ga_imp_clk_process_20130326@etlsga a,
          from   (select * from adaptive_data.' ||
                        l_dedup_table ||
                        '@etlsga where rownum < 12)a,      
                 ods_metadata.adm_ads ord, dim_countries coun,
                 channel_aggregate_primary ch
          where  a.ad_id = ord.ad_id(+)
          and    upper(a.country_id) = coun.adaptive_code(+)
          and    a.affiliate_id = ch.affiliate_id(+)'
        using p_process_name;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ga_imp_clk subpartition(P_' ||
                        p_process_date || '_' ||
                        upper(p_process_name) ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;*/
  end;

  procedure evo_atako_agg_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_previous_dt  varchar2(100);
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_atako_agg_day';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      l_previous_dt := to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd');
      execute immediate 'Alter table evo_atako_agg_day truncate partition P_' ||
                        p_process_date;
      -- evo atako agg table for the process_date
      insert /*+APPEND */
      into evo_atako_agg_day
        select p_process_date as date_id, atako.ad_id,
               sum(coalesce(impressions_count, 0)) as impressions_count,
               sum(coalesce(displaytime_total, 0)) as displaytime_total,
               sum(coalesce(allinteractions_count, 0)) as allinteractions_count,
               sum(coalesce(views_with_interaction_count, 0)) as views_with_interaction_count,
               sum(coalesce(interaction_totaltime, 0)) as interaction_totaltime,
               sum(coalesce(clicks, 0)) as clicks,
               sum(coalesce(expansion_count, 0)) as expansion_count,
               sum(coalesce(expansion_totaltime, 0)) as expansion_totaltime,
               sum(coalesce(video_totaltime, 0)) as video_totaltime,
               sum(coalesce(video_starts, 0)) as video_starts,
               sum(coalesce(video_completes, 0)) as video_completes,
               sum(coalesce(video_1, 0)) as video_1,
               sum(coalesce(video_2, 0)) as video_2,
               sum(coalesce(video_3, 0)) as video_3,
               sum(coalesce(video_4, 0)) as video_4,
               sum(coalesce(tab_1, 0)) as tab_1,
               sum(coalesce(tab_2, 0)) as tab_2,
               sum(coalesce(tab_3, 0)) as tab_3,
               sum(coalesce(tab_4, 0)) as tab_4,
               sum(coalesce(posts, 0)) as posts,
               sum(coalesce(refreshes, 0)) as refreshes,
               sum(coalesce(updates, 0)) as updates,
               sum(coalesce(raw_scrolls, 0)) as raw_scrolls,
               sum(coalesce(scrolls, 0)) as scrolls,
               sum(coalesce(tab_5, 0)) as tab_5,
               sum(coalesce(tab_6, 0)) as tab_6,
               sum(coalesce(tab_7, 0)) as tab_7,
               sum(coalesce(tab_8, 0)) as tab_8,
               sum(coalesce(tab_9, 0)) as tab_9,
               sum(coalesce(tab_10, 0)) as tab_10,
               sum(coalesce(shares, 0)) as shares,
               sum(coalesce(exit_1, 0)) as exit_1,
               sum(coalesce(exit_2, 0)) as exit_2,
               sum(coalesce(exit_3, 0)) as exit_3,
               sum(coalesce(exit_4, 0)) as exit_4,
               sum(coalesce(exit_5, 0)) as exit_5,
               sum(coalesce(exit_6, 0)) as exit_6,
               sum(coalesce(exit_7, 0)) as exit_7,
               sum(coalesce(exit_8, 0)) as exit_8,
               sum(coalesce(exit_9, 0)) as exit_9,
               sum(coalesce(exit_10, 0)) as exit_10,
               sum(coalesce(refresh_changes, 0)) as refresh_changes,
               sum(coalesce(logins, 0)) as logins,
               sum(coalesce(qualified_interaction_count, 0)) as qualified_interaction_count,
               sum(coalesce(all_tabs, 0)) as all_tabs,
               sum(coalesce(qualified_interaction_duration, 0)) as qualified_interaction_duration,
               sum(coalesce(developer_id, 0)) as developer_id,
               sum(coalesce(autoexpansion_count, 0)) as autoexpansion_count,
               sum(coalesce(autoexpansion_totaltime, 0)) as autoexpansion_totaltime,
               sum(coalesce(fullscreen_count, 0)) as fullscreen_count,
               sum(coalesce(fullscreen_totaltime, 0)) as fullscreen_totaltime,
               sum(coalesce(video25_count, 0)) as video25_count,
               sum(coalesce(video50_count, 0)) as video50_count,
               sum(coalesce(video75_count, 0)) as video75_count,
               sum(coalesce(pause_count, 0)) as pause_count,
               sum(coalesce(mute_count, 0)) as mute_count,
               sum(coalesce(error_num, 0)) as error_num,
               sum(coalesce(impressions_count, 0)) as impressions_888x11,
               sum(coalesce(displaytime_total, 0)) as displaytime_888x11
        from   (select /*+ parallel(a,5,1) */
                  ad_id, impressions_count, displaytime_total,
                  allinteractions_count,
                  views_with_interaction_count,
                  interaction_totaltime, clicks,
                  expansion_count, expansion_totaltime,
                  video_totaltime, video_starts,
                  video_completes, video_1, video_2, video_3,
                  video_4, tab_1, tab_2, tab_3, tab_4, posts,
                  refreshes, updates, raw_scrolls, scrolls,
                  tab_5, tab_6, tab_7, tab_8, tab_9, tab_10,
                  shares, exit_1, exit_2, exit_3, exit_4,
                  exit_5, exit_6, exit_7, exit_8, exit_9,
                  exit_10, refresh_changes, logins,
                  qualified_interaction_count, all_tabs,
                  qualified_interaction_duration, developer_id,
                  autoexpansion_count, autoexpansion_totaltime,
                  fullscreen_count, fullscreen_totaltime,
                  video25_count, video50_count, video75_count,
                  pause_count, mute_count, error_num,
                  impressions_888x11, displaytime_888x11
                 from   evo_atako_agg_day a
                 where  date_id = l_previous_dt
                 union all
                 select /*+ parallel(a,5,1) */
                  a.ad_id, impressions_count,
                  displaytime_total, allinteractions_count,
                  views_with_interaction_count,
                  interaction_totaltime, clicks,
                  expansion_count, expansion_totaltime,
                  video_totaltime, video_starts,
                  video_completes, video_1, video_2, video_3,
                  video_4, tab_1, tab_2, tab_3, tab_4, posts,
                  refreshes, updates, raw_scrolls, scrolls,
                  tab_5, tab_6, tab_7, tab_8, tab_9, tab_10,
                  shares, exit_1, exit_2, exit_3, exit_4,
                  exit_5, exit_6, exit_7, exit_8, exit_9,
                  exit_10, refresh_changes, logins,
                  qualified_interaction_count, all_tabs,
                  qualified_interaction_duration, developer_id,
                  autoexpansion_count, autoexpansion_totaltime,
                  fullscreen_count, fullscreen_totaltime,
                  video25_count, video50_count, video75_count,
                  pause_count, mute_count, error_num,
                  case
                    when ad_size_id = '888x11' then
                     coalesce(impressions_count, 0)
                    else
                     0
                  end as impressions_888x11,
                  case
                    when ad_size_id = '888x11' then
                     coalesce(displaytime_total, 0)
                    else
                     0
                  end as displaytime_888x11
                 from   fact_atako_adapt_agg_day a,
                        dim_ad_new dan
                 where  date_id = p_process_date
                 and    a.ad_id = dan.ad_id(+)) atako
        group  by atako.ad_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_atako_agg_day partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_media_camp_agg_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_previous_dt  varchar2(100);
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_media_camp_agg_day';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      l_previous_dt := to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd');
      execute immediate 'Alter table evo_media_camp_agg_day truncate partition P_' ||
                        p_process_date;
      -- evo media camp agg day table for the process_date
      insert /*+APPEND */
      into evo_media_camp_agg_day
        select p_process_date as date_id, a.ad_id,
               sum(coalesce(total_impressions, 0)) as total_impressions,
               sum(coalesce(total_clicks, 0)) as total_clicks
        from   (select /*+ parallel(a,5,1) */
                  ad_id, total_impressions, total_clicks
                 from   evo_media_camp_agg_day a
                 where  date_id = l_previous_dt
                 union all
                 select /*+ parallel(a,5,1) */
                  ad_id, total_impressions, total_clicks
                 from   fact_media_camp_agg a
                 where  date_id = p_process_date) a
        group  by a.ad_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_media_camp_agg_day partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_atako_agg_adsize
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_previous_dt  varchar2(100);
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_atako_agg_adsize';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      l_previous_dt := to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd');
      execute immediate 'Alter table evo_atako_agg_adsize truncate partition P_' ||
                        p_process_date;
      -- evo_atako_agg_adsize table for the process_date
      insert /*+APPEND */
      into evo_atako_agg_adsize
        (date_id, order_id, ad_id, atako_impressions,
         atako_clicks, evo_ad_sum_impressions,
         evo_ad_sum_clicks, impressions_888x11,
         displaytime_888x11, displaytime_total,
         video_totaltime, video_starts, video_completes,
         product_id)
        select p_process_date as date_id, order_id, ad_id,
               sum(atako_impressions) atako_impressions,
               sum(atako_clicks) atako_clicks,
               sum(evo_ad_sum_impressions) as evo_ad_sum_impressions,
               sum(evo_ad_sum_clicks) as evo_ad_sum_clicks,
               sum(impressions_888x11) as impressions_888x11,
               sum(displaytime_888x11) displaytime_888x11,
               sum(displaytime_total) as displaytime_total,
               sum(video_totaltime) as video_totaltime,
               sum(video_starts) as video_starts,
               sum(video_completes) as video_completes,
               product_id
        from   (select /*+ parallel(a,5,1) */
                  order_id, ad_id, atako_impressions,
                  atako_clicks, evo_ad_sum_impressions,
                  evo_ad_sum_clicks, impressions_888x11,
                  displaytime_888x11, displaytime_total,
                  video_totaltime, video_starts,
                  video_completes, product_id
                 from   evo_atako_agg_adsize a
                 where  date_id = l_previous_dt
                 union all
                 select /*+ parallel(eas,5,1) */
                  eas.order_id, eas.ad_id,
                  atako.impressions_count as atako_impressions,
                  atako.clicks as atako_clicks,
                  eas.total_impressions as evo_ad_sum_impressions,
                  eas.total_clicks as evo_ad_sum_clicks,
                  case
                    when ad_size_id = '888x11' then
                     total_impressions
                    else
                     0
                  end as impressions_888x11,
                  case
                    when ad_size_id = '888x11' then
                     displaytime_total
                    else
                     0
                  end as displaytime_888x11, displaytime_total,
                  coalesce(video_totaltime, 0) as video_totaltime,
                  coalesce(video_starts, 0) as video_starts,
                  coalesce(video_completes, 0) as video_completes,
                  eas.product_id
                 from   campaign.fact_atako_adapt_agg_day atako,
                        evo_ad_summary eas,
                        campaign.dim_ad_new dan
                 where  eas.ad_id = dan.ad_id
                 and    eas.ad_id = atako.ad_id(+)
                 and    date_id(+) = p_process_date)
        group  by order_id, ad_id, product_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_atako_agg_adsize partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_ad_summary
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_ad_summary';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table evo_ad_summary';
      insert /*+ append */
      into evo_ad_summary
        (ad_id, ad_name, order_id, rate, pricingtype,
         quantity, total_impressions, total_clicks,
         startdate, enddate, atako, product_id)
        select a.ad_id, a.name, a.order_id, a.rate,
               a.pricingtype, a.quantity,
               a.total_impressions, a.total_clicks,
               a.startdate, a.enddate,
               decode(d.id_type, 'd', 1, 0),
               decode(oms.product_id, '', 0, oms.product_id) as product_id
        from   (select /*+ parallel(b,4,1) */
                  a.ad_id, a.name, c.order_id, a.rate,
                  a.pricingtype, a.quantity,
                  sum(b.total_impressions) total_impressions,
                  sum(b.total_clicks) total_clicks,
                  a.startdate, a.enddate
                 from   campaign.dim_ad_new a,
                        evo_media_camp_agg_day b,
                        campaign.dim_campaign c
                 where  a.ad_id = c.ad_id
                 and    a.startdate >=
                        to_date('10/01/2008', 'mm/dd/yyyy')
                 and    a.ad_id = b.ad_id(+)
                 and    b.date_id(+) = p_process_date
                 group  by a.ad_id, a.name, c.order_id, a.rate,
                           a.pricingtype, a.quantity,
                           a.startdate, a.enddate) a,
               (select /*+ parallel(a,4,1) */
                 distinct ad_id, id_type
                 from   fact_atako_adapt_agg_day a
                 where  date_id = p_process_date) d,
               (select distinct ad_ad_server_id,
                                 eps.product_id
                 from   campaign.dim_oms_evolution_product@etladq oms,
                        evolution_data.evo_product_summary@etladq eps
                 where  upper(eps.product_name) =
                        upper(oms.evolution_product_name)) oms
        where  a.ad_id = d.ad_id(+)
        and    a.ad_id = oms.ad_ad_server_id(+)
        order  by name;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_ad_summary estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_product_summary
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_product_summary';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table evo_product_summary';
      insert into evo_product_summary
        select *
        from   evolution_data.evo_product_summary@etladq;
      l_cnt := sql%rowcount;
      for i in (select eas.product_id, count(1) as ad_count
                from   evo_ad_summary eas,
                       evo_product_summary eps,
                       evo_atako_agg_day c
                where  eas.product_id = eps.product_id
                and    eas.ad_id = c.ad_id
                and    c.date_id = p_process_date
                and    eps.product_name <> 'Other'
                and    upper(eps.product_name) in
                       ('DCM', 'BOOST', 'PUSHDOWN', 'ECLIPSE', 'SPLASH')
                group  by eas.product_id
                union all
                select eas.product_id, count(1) as ad_count
                from   evo_ad_summary eas,
                       evo_product_summary eps
                where  eas.product_id = eps.product_id
                and    eps.product_name <> 'Other'
                and    upper(eps.product_name) in
                       ('RESKIN', 'EXPERT', 'BRAND ADVOCATE', 'STANDARD MEDIA')
                group  by eas.product_id
                union all
                select max(eps.product_id) as product_id,
                       sum(cnt) as ad_count
                from   (select count(1) cnt
                         from   evo_ad_summary eas,
                                evo_product_summary eps
                         where  eas.product_id =
                                eps.product_id
                         and    eps.product_name = 'Other'
                         union all
                         select count(1) cnt
                         from   evo_ad_summary eas,
                                evo_product_summary eps
                         where  eas.product_id =
                                eps.product_id
                         and    upper(eps.product_name) not in
                               --('OTHER', 'STANDARD MEDIA')
                                ('RESKIN', 'EXPERT', 'BRAND ADVOCATE', 'STANDARD MEDIA', 'OTHER')
                         and    ad_id not in
                                (select ad_id
                                  from   evo_atako_agg_day
                                  where  date_id =
                                         p_process_date
                                  and    ad_id is not null)),
                       evo_product_summary eps
                where  eps.product_name = 'Other') loop
        update evo_product_summary
        set    ad_count = i.ad_count
        where  product_id = i.product_id;
      end loop;
      commit;
      execute immediate 'analyze table evo_product_summary estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure dim_product_average_metrics
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'dim_product_average_metrics';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_product_average_metrics';
      insert into dim_product_average_metrics
        select *
        from   campaign.dim_product_average_metrics@etladq;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table dim_product_average_metrics estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure update_evo_ad_summary
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'update-dim_product_average_metrics';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      /* for i in (select product_name, sum(avg_ctr) as avg_ctr,
             sum(avg_qir) as avg_qir,
             sum(avg_exp_rate) as avg_exp_rate,
             sum(avg_cpp) as avg_cpp
      from   (select upper(eps.product_name) as product_name,
                      avg(clicks /
                           nullif(impressions_count, 0)) as avg_ctr,
                      avg(qualified_interaction_count /
                           nullif(impressions_count, 0)) as avg_qir,
                      avg(expansion_count /
                           nullif(impressions_count, 0)) as avg_exp_rate,
                      0 as avg_cpp
               from   fact_atako_adapt_agg_day atako,
                      evo_ad_summary eas,
                      evo_product_summary eps
               where  date_id >
                      to_number(to_char(sysdate - 91, 'YYYYMMDD'))
               and    eas.product_id =
                      eps.product_id
               and    eas.ad_id = atako.ad_id
               group  by upper(eps.product_name)
               union all
               select product_name as product_name,
                      0 as avg_ctr, 0 as avg_qir,
                      0 as avg_exp_rate,
                      avg(avg_cpp) as avg_cpp
               from   (select upper(eps.product_name) as product_name,
                               sum(clicks) /
                               sum(nullif(case
                                            when ad_size_id =
                                                 '888x11' then
                                             impressions_count
                                            else
                                             0
                                          end, 0)) as avg_cpp
                        from   evo_ad_summary eas,
                               campaign.dim_ad_new dan,
                               campaign.fact_atako_adapt_agg_day atako,
                               evo_product_summary eps
                        where  atako.date_id >
                               to_number(to_char(sysdate - 91, 'YYYYMMDD'))
                        and    eas.product_id =
                               eps.product_id
                        and    eas.ad_id = dan.ad_id
                        and    atako.ad_id = eas.ad_id
                        group  by upper(eps.product_name))
               group  by product_name)
      group  by product_name) loop*/
      for i in (select /*+ parallel(atako,5,1) */
                 upper(eps.product_name) as product_name,
                 avg(clicks / nullif(impressions_count, 0)) as avg_ctr,
                 avg(qualified_interaction_count /
                      nullif(impressions_count, 0)) as avg_qir,
                 avg(expansion_count /
                      nullif(impressions_count, 0)) as avg_exp_rate,
                 avg(clicks / nullif(case
                                       when ad_size_id = '888x11' then
                                        impressions_count
                                       else
                                        0
                                     end, 0)) as avg_cpp
                from   evo_ad_summary eas,
                       evo_product_summary eps,
                       campaign.fact_atako_adapt_agg_day atako,
                       campaign.dim_ad_new dan
                where  eas.product_id = eps.product_id
                and    eas.ad_id = atako.ad_id
                and    atako.date_id >
                       to_number(to_char(sysdate - 91, 'YYYYMMDD'))
                      --to_number(to_char(p_process_date - 91, 'YYYYMMDD'))
                and    atako.ad_id = dan.ad_id
                group  by upper(eps.product_name)) loop
        update campaign.dim_product_average_metrics
        set    product_value = case
                                 when product_key =
                                      'AVG_CTR_PRODUCT_NAME' then
                                  i.avg_ctr
                                 when product_key =
                                      'AVG_QIR_PRODUCT_NAME' then
                                  i.avg_qir
                                 when product_key =
                                      'AVG_EXPANSION_RATE_PRODUCT_NAME' then
                                  i.avg_exp_rate
                                 when product_key =
                                      'AVG_CPP_PRODUCT_NAME' then
                                  i.avg_cpp
                               end
        where  upper(product_name) = i.product_name;
        commit;
      end loop;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_product_index
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_product_index';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table evo_product_index';
      insert /*+ APPEND */
      into evo_product_index
        (ad_id, impressions, total_clicks, ctr,
         glam_ctr_index, industry_ctr_index,
         total_qualified_interactions, qir, glam_qir_index,
         industry_qir_index, total_expansions,
         expansion_rate, glam_exp_rate_index,
         total_expansion_time, avg_expansion_time,
         avg_int_per_exp, total_video_play_time,
         avg_play_time, video_completion_rate, product_name)
        select /*+ parallel(a,5,1) */
         a.ad_id, a.impressions_count, a.clicks,
         a.clicks / nullif(a.impressions_count, 0) * 100 ctr,
         (a.clicks / nullif(a.impressions_count, 0)) /
          (select nullif(product_value, 0)
           from   campaign.dim_product_average_metrics c
           where  c.product_name = eps.product_name
           and    product_key = 'AVG_CTR_PRODUCT_NAME') * 100 glam_ctr_index,
         (a.clicks / nullif(a.impressions_count, 0)) /
          (select nullif(product_value, 0)
           from   campaign.dim_product_average_metrics c
           where  c.product_name = eps.product_name
           and    product_key = 'EXT_CTR_PRODUCT_NAME') * 100 industry_ctr_index,
         a.qualified_interaction_count,
         a.qualified_interaction_count /
          nullif(a.impressions_count, 0) * 100 qir,
         (a.qualified_interaction_count /
          nullif(a.impressions_count, 0)) /
          (select nullif(product_value, 0)
           from   campaign.dim_product_average_metrics c
           where  c.product_name = eps.product_name
           and    product_key = 'AVG_QIR_PRODUCT_NAME') * 100 glam_qir_index,
         (a.qualified_interaction_count /
          nullif(a.impressions_count, 0)) /
          (select nullif(product_value, 0)
           from   campaign.dim_product_average_metrics c
           where  c.product_name = eps.product_name
           and    product_key = 'EXT_QIR_PRODUCT_NAME') * 100 industry_qir_index,
         a.expansion_count,
         a.expansion_count / nullif(a.impressions_count, 0) * 100 expansion_rate,
         (a.expansion_count /
          nullif(a.impressions_count, 0)) /
          (select nullif(product_value, 0)
           from   campaign.dim_product_average_metrics c
           where  c.product_name = eps.product_name
           and    product_key =
                  'AVG_EXPANSION_RATE_PRODUCT_NAME') * 100 glam_exp_rate_index,
         a.expansion_totaltime,
         (a.expansion_totaltime * 60 * 60) /
          nullif(a.expansion_count, 0) avg_expansion_time,
         a.allinteractions_count /
          nullif((a.expansion_count + a.autoexpansion_count), 0) avg_int_per_exp,
         a.video_totaltime,
         (a.video_totaltime * 60 * 60) /
          nullif(a.video_starts, 0) avg_play_time,
         a.video_completes / nullif(a.video_starts, 0) * 100 video_completion_rate,
         product_name
        from   evo_atako_agg_day a, evo_ad_summary eas,
               evo_product_summary eps
        where  a.date_id = p_process_date
        and    eas.product_id = eps.product_id
        and    upper(product_name) in
               ('DCM', 'BOOST', 'PUSHDOWN', 'ECLIPSE', 'SPLASH')
        and    eas.ad_id = a.ad_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_product_index estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_product_ad_size
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_product_ad_size';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table evo_product_ad_size';
      insert /*+ APPEND */
      into evo_product_ad_size
        (ad_id, impressions, total_pages,
         total_dwell_display_time, avg_dwell_display_time,
         total_clicks, click_per_page_rate, glam_cpp_index,
         total_video_play_time, avg_play_time,
         video_completion_rate, product_name)
        select /*+ parallel(b,5,1) */
         a.ad_id, max_impressions,
         nvl(impressions_888x11, 0) total_pages,
         nvl(displaytime_888x11, 0) total_dwell,
         (displaytime_888x11 /
          nvl(nullif(displaytime_total, 0), 1) * 3600) /
          nvl(nullif(impressions_888x11, 0), 1) avg_dwell,
         max_clicks,
         evo_ad_sum_clicks /
          nvl(nullif(impressions_888x11, 0), 1) * 100 cpp,
         evo_ad_sum_clicks /
          nvl(nullif(impressions_888x11, 0), 1) * 100 /
          nvl(nullif((select product_value
                     from   campaign.dim_product_average_metrics@etladq
                     where  product_name = eps.product_name
                     and    product_key =
                            'AVG_CPP_PRODUCT_NAME'), 0), 1) cpp_index,
         video_totaltime as total_video_play_time,
         video_totaltime * 3600 /
          nvl(nullif(video_starts, 0), 1) avg_play_time,
         video_completes / nvl(nullif(video_starts, 0), 1) * 100 video_completion_rate,
         eps.product_name
        from   (select eas.order_id, eas.ad_id, product_id,
                        max(eas.total_impressions) as max_impressions,
                        max(eas.total_clicks) as max_clicks
                 from   evo_ad_summary eas
                 group  by eas.order_id, eas.ad_id, product_id) a,
               evo_atako_agg_adsize b,
               evo_product_summary eps
        where  a.product_id = eps.product_id
        and    b.date_id = p_process_date
        and    a.order_id = b.order_id
        and    a.ad_id = b.ad_id
        and    a.product_id = b.product_id
        and    eps.product_name in
               ('Reskin', 'Expert', 'Brand Advocate');
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_product_ad_size estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_product_ext
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_product_ext';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table evo_product_ext';
      insert /*+ APPEND */
      into evo_product_ext
        (ad_id, impressions_count, displaytime_total,
         allinteractions_count, views_with_interaction_count,
         interaction_totaltime, clicks, expansion_count,
         expansion_totaltime, video_totaltime, video_starts,
         video_completes, video_1, video_2, video_3, video_4,
         tab_1, tab_2, tab_3, tab_4, posts, refreshes,
         updates, raw_scrolls, scrolls, tab_5, tab_6, tab_7,
         tab_8, tab_9, tab_10, shares, exit_1, exit_2,
         exit_3, exit_4, exit_5, exit_6, exit_7, exit_8,
         exit_9, exit_10, refresh_changes, logins,
         qualified_interaction_count, all_tabs,
         qualified_interaction_duration, developer_id,
         autoexpansion_count, autoexpansion_totaltime,
         fullscreen_count, fullscreen_totaltime,
         video25_count, video50_count, video75_count,
         pause_count, mute_count, error_num, product_name)
        select /*+ parallel(a,5,1) */
         a.ad_id, impressions_count, displaytime_total,
         allinteractions_count, views_with_interaction_count,
         interaction_totaltime, clicks, expansion_count,
         expansion_totaltime, video_totaltime, video_starts,
         video_completes, video_1, video_2, video_3, video_4,
         tab_1, tab_2, tab_3, tab_4, posts, refreshes,
         updates, raw_scrolls, scrolls, tab_5, tab_6, tab_7,
         tab_8, tab_9, tab_10, shares, exit_1, exit_2,
         exit_3, exit_4, exit_5, exit_6, exit_7, exit_8,
         exit_9, exit_10, refresh_changes, logins,
         qualified_interaction_count, all_tabs,
         qualified_interaction_duration, developer_id,
         autoexpansion_count, autoexpansion_totaltime,
         fullscreen_count, fullscreen_totaltime,
         video25_count, video50_count, video75_count,
         pause_count, mute_count, error_num, product_name
        from   evo_atako_agg_day a, evo_ad_summary eas,
               evo_product_summary eps
        where  a.date_id = p_process_date
        and    eas.product_id = eps.product_id
        and    eas.ad_id = a.ad_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_product_ext estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_ibt
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    /*l_process_name := p_process_name || '-' ||
                      'evo_ibt_by_order_ad';
     if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table evo_ibt_by_order_ad truncate partition P_' ||
                        p_process_date;
      insert \*+ append *\
      into evo_ibt_by_order_ad
        (date_id, hours, order_id, ad_id, total_impressions,
         total_clicks)
        select \*+ parallel(a,5,1) *\
         date_id, hours, order_id, ad_id,
         sum(total_impressions), sum(a.total_clicks)
        from   campaign.fact_media_day a
        where  date_id = p_process_date
        group  by date_id, hours, order_id, ad_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_ibt_by_order_ad partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;*/
    l_process_name := p_process_name || '-' ||
                      'evo_ibt_by_order_ad';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table evo_ibt_by_order_ad truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into evo_ibt_by_order_ad
        (date_id, hours, order_id, ad_id, total_impressions,
         total_clicks)
        select /*+ parallel(a,5,1) */
         date_id,
         to_number(to_char(time_stamp, 'hh24')) as hours,
         order_id, ad_id, sum(impressions), sum(clicks)
        from   ga_imp_clk a
        where  date_id = p_process_date
        and    process_name = lower(p_process_name)
        group  by date_id,
                  to_number(to_char(time_stamp, 'hh24')),
                  order_id, ad_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_ibt_by_order_ad partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_chd
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_chd_by_order_ad';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table evo_chd_by_order_ad truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into evo_chd_by_order_ad
        (date_id, order_id, ad_id, channel_name,
         total_impressions, total_clicks)
        select /*+ parallel(a,5,1) */
         a.date_id, a.order_id, a.ad_id,
         coalesce(b.category_name_full, 'Unknown'),
         sum(a.total_impressions), sum(a.total_clicks)
        from   fact_adq_ad_aff_agg a,
               ods_metadata.channel_aggregate b
        where  a.date_id = p_process_date
        and    a.affiliate_id = b.affiliate_id(+)
        and    category_type_full(+) = 'Primary'
        group  by a.date_id, a.order_id, a.ad_id,
                  b.category_name_full;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_chd_by_order_ad partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_delivery_data
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_delivery_data_by_order_ad';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table evo_delivery_data_by_order_ad truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into evo_delivery_data_by_order_ad
        (date_id, order_id, ad_id, total_impressions,
         total_clicks, total_visitors)
        select /*+ parallel(a,5,1) */
         a.date_id, a.order_id, a.ad_id,
         sum(a.total_impressions) imps,
         sum(a.total_clicks) clk,
         max(coalesce(unique_visits, 0)) as visitors
        from   campaign.fact_adq_ad_agg a,
               (select /*+ parallel(b,5,1) */
                  date_id, order_id, ad_id,
                  count(distinct user_id) unique_visits
                 from   fact_bt_monthly_raw b
                 where  b.date_id = p_process_date
                 group  by date_id, order_id, ad_id) c
        where  a.date_id = p_process_date
        and    a.date_id = c.date_id(+)
        and    a.order_id = c.order_id(+)
        and    a.ad_id = c.ad_id(+)
        group  by a.date_id, a.order_id, a.ad_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_delivery_data_by_order_ad partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_placement
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_placement_by_order_ad';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table evo_placement_by_order_ad truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into evo_placement_by_order_ad
        (date_id, order_id, ad_id, is_atf,
         total_impressions, total_clicks)
        select /*+ parallel(a,5,1) */
         date_id, order_id, ad_id, is_atf,
         sum(total_impressions), sum(total_clicks)
        from   campaign.fact_adq_flag_agg a
        where  date_id = p_process_date
        group  by date_id, order_id, ad_id, is_atf;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_placement_by_order_ad partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_geo_countries
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    --
    l_process_name := p_process_name || '-' ||
                      'evo_geo_countries_by_order_ad';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table evo_geo_countries_by_order_ad truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into evo_geo_countries_by_order_ad
        (order_id, ad_id, total_impressions, total_clicks,
         date_id, country_id, country)
        select /*+ parallel(a,5,1) */
         order_id, ad_id, sum(total_impressions),
         sum(total_clicks), date_id, a.country_id, b.country
        from   campaign.fact_adq_ad_country_agg a,
               campaign.dim_countries b
        where  date_id = p_process_date
        and    a.country_id = b.country_id(+)
        group  by order_id, ad_id, date_id, a.country_id,
                  b.country;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_geo_countries_by_order_ad partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_geo_states
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    --
    l_process_name := p_process_name || '-' ||
                      'evo_geo_states_by_order_ad';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table evo_geo_states_by_order_ad truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into evo_geo_states_by_order_ad
        (order_id, ad_id, state_province_name,
         total_impressions, total_clicks, date_id,
         country_id)
        select /*+ parallel(a,5,1) */
         order_id, ad_id, b.state_province_name,
         sum(total_impressions), sum(total_clicks), date_id,
         country_id
        from   campaign.fact_adq_ad_state_agg a,
               campaign.dim_states b
        where  date_id = p_process_date
        and    a.state_province = b.state_province(+)
        group  by order_id, ad_id, b.state_province_name,
                  date_id, country_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_geo_states_by_order_ad partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_order_summary
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_order_summary';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table evo_order_summary';
      insert /*+ append */
      into evo_order_summary
        (order_id, order_name, advertiser_id,
         order_start_date, order_end_date, ad_count,
         net_value)
        select a.order_id, a.order_name, b.advertiser_id,
               a.order_start_date, a.order_end_date,
               count(distinct b.ad_id),
               sum((c.quantity / 1000) * c.rate)
        from   campaign.dim_order a, campaign.dim_campaign b,
               campaign.dim_ad_new c
        where  b.ad_id = c.ad_id
        and    a.order_id = b.order_id
        and    coalesce(a.order_start_date, to_date('10/01/2008', 'mm/dd/yyyy')) >=
               to_date('10/01/2008', 'mm/dd/yyyy')
        group  by a.order_id, a.order_name, b.advertiser_id,
                  a.order_start_date, a.order_end_date;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_order_summary estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_advertiser_summary
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_advertiser_summary';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table evo_advertiser_summary';
      insert /*+ append */
      into evo_advertiser_summary
        (advertiser_id, advertiser_name, order_count)
        select a.advertiser_id, a.advertiser_name,
               count(distinct a.order_id)
        from   campaign.dim_campaign a, campaign.dim_order b
        where  a.order_id = b.order_id
        and    coalesce(b.order_start_date, to_date('10/01/2008', 'mm/dd/yyyy')) >=
               to_date('10/01/2008', 'mm/dd/yyyy')
        group  by a.advertiser_id, a.advertiser_name;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_advertiser_summary estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_atako_ad
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_atako_ad';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table evo_atako_ad';
      insert /*+ append */
      into evo_atako_ad
        (date_id, ad_id, impressions_count,
         displaytime_total, allinteractions_count,
         views_with_interaction_count, interaction_totaltime,
         clicks, expansion_count, expansion_totaltime,
         video_totaltime, video_starts, video_completes,
         video_1, video_2, video_3, video_4, tab_1, tab_2,
         tab_3, tab_4, posts, refreshes, updates,
         raw_scrolls, scrolls, created_by, created_date)
        select /*+ parallel(a,5,1) */
         date_id, ad_id, impressions_count,
         displaytime_total, allinteractions_count,
         views_with_interaction_count, interaction_totaltime,
         clicks, expansion_count, expansion_totaltime,
         video_totaltime, video_starts, video_completes,
         video_1, video_2, video_3, video_4, tab_1, tab_2,
         tab_3, tab_4, posts, refreshes, updates,
         raw_scrolls, scrolls, created_by, created_date
        from   fact_atako_adapt_agg_day a
        where  id_type = 'd'
        and    date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_atako_ad estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_product_other
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_product_other';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table evo_product_other';
      insert /*+ append */
      into evo_product_other
        (ad_id, impressions, total_clicks, ctr)
        select eas.ad_id, eas.total_impressions impressions,
               eas.total_clicks,
               eas.total_clicks /
                nullif(eas.total_impressions, 0) * 100 ctr
        from   evo_ad_summary eas,
               (select ad_id
                 from   evo_ad_summary
                 where  product_id =
                        (select product_id
                         from   evo_product_summary
                         where  product_name = 'Other')
                 union all
                 select ad_id
                 from   evo_ad_summary
                 where  ad_id not in
                        (select ad_id
                         from   evo_atako_agg_day
                         where  ad_id is not null)
                 and    product_id not in
                        (select product_id
                          from   evo_product_summary
                          where  product_name in
                                 ('Other', 'Standard Media'))) b
        where  eas.ad_id = b.ad_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_product_other estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure evo_product_standard_media
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'evo_product_standard_media';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table evo_product_standard_media';
      insert /*+ append */
      into evo_product_standard_media
        (ad_id, rate, unit, impressions, total_clicks, ctr)
        select /*+ parallel(a,5,1) */
         ad_id, rate, quantity unit,
         total_impressions impressions, total_clicks,
         total_clicks / nullif(total_impressions, 0) * 100 ctr
        from   evo_ad_summary a
        where  product_id =
               (select product_id
                from   evo_product_summary
                where  product_name = 'Standard Media');
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table evo_product_standard_media estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure replicate_to_evolution
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    --l_sql          varchar2(4000);
  begin
    -- evolution_data.evo_ibt_by_order@adq
    l_process_name := p_process_name || '-' ||
                      'evo_ibt_by_order@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_ibt_by_order@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_ibt_by_order@etladq
        (date_id, hours, order_id, total_impressions,
         total_clicks)
        select /*+ parallel(a,5,1) */
         date_id, hours, order_id, sum(total_impressions),
         sum(total_clicks)
        from   evo_ibt_by_order_ad a
        where  date_id = p_process_date
        group  by date_id, hours, order_id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- evolution_data.evo_ibt_by_ad@adq
    l_process_name := p_process_name || '-' ||
                      'evo_ibt_by_ad@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_ibt_by_ad@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_ibt_by_ad@etladq
        (date_id, hours, ad_id, total_impressions,
         total_clicks)
        select /*+ parallel(a,5,1) */
         date_id, hours, ad_id, sum(total_impressions),
         sum(total_clicks)
        from   evo_ibt_by_order_ad a
        where  date_id = p_process_date
        group  by date_id, hours, ad_id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- evolution_data.evo_chd_by_order@adq
    l_process_name := p_process_name || '-' ||
                      'evo_chd_by_order@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_chd_by_order@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_chd_by_order@etladq
        (date_id, order_id, channel_name, total_impressions,
         total_clicks)
        select /*+ parallel(a,5,1) */
         date_id, order_id, channel_name,
         sum(total_impressions), sum(total_clicks)
        from   evo_chd_by_order_ad a
        where  date_id = p_process_date
        group  by date_id, order_id, channel_name;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- evolution_data.evo_chd_by_ad@adq
    l_process_name := p_process_name || '-' ||
                      'evo_chd_by_ad@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_chd_by_ad@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_chd_by_ad@etladq
        (date_id, ad_id, channel_name, total_impressions,
         total_clicks)
        select /*+ parallel(a,5,1) */
         date_id, ad_id, channel_name,
         sum(total_impressions), sum(total_clicks)
        from   evo_chd_by_order_ad a
        where  date_id = p_process_date
        group  by date_id, ad_id, channel_name;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --
    -- evolution_data.evo_delivery_data_by_order@adq
    l_process_name := p_process_name || '-' ||
                      'evo_delivery_data_by_order@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_delivery_data_by_order@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_delivery_data_by_order@etladq
        (date_id, order_id, total_impressions, total_clicks,
         total_visitors)
        select /*+ parallel(a,5,1) */
         date_id, order_id, sum(total_impressions),
         sum(total_clicks), max(total_visitors)
        from   evo_delivery_data_by_order_ad a
        where  date_id = p_process_date
        group  by date_id, order_id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- evolution_data.evo_delivery_data_by_ad@adq
    l_process_name := p_process_name || '-' ||
                      'evo_delivery_data_by_ad@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_delivery_data_by_ad@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_delivery_data_by_ad@etladq
        (date_id, ad_id, total_impressions, total_clicks,
         total_visitors)
        select /*+ parallel(a,5,1) */
         date_id, ad_id, sum(total_impressions),
         sum(total_clicks), max(total_visitors)
        from   evo_delivery_data_by_order_ad a
        where  date_id = p_process_date
        group  by date_id, ad_id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --
    -- evolution_data.evo_placement_by_order@adq
    l_process_name := p_process_name || '-' ||
                      'evo_placement_by_order@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_placement_by_order@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_placement_by_order@etladq
        (date_id, order_id, is_atf, total_impressions,
         total_clicks)
        select /*+ parallel(a,5,1) */
         date_id, order_id, is_atf, sum(total_impressions),
         sum(total_clicks)
        from   evo_placement_by_order_ad a
        where  date_id = p_process_date
        group  by date_id, order_id, is_atf;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- evolution_data.evo_placement_by_ad@adq
    l_process_name := p_process_name || '-' ||
                      'evo_placement_by_ad@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_placement_by_ad@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_placement_by_ad@etladq
        (date_id, ad_id, is_atf, total_impressions,
         total_clicks)
        select /*+ parallel(a,5,1) */
         date_id, ad_id, is_atf, sum(total_impressions),
         sum(total_clicks)
        from   evo_placement_by_order_ad a
        where  date_id = p_process_date
        group  by date_id, ad_id, is_atf;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --
    -- evolution_data.evo_geo_countries_by_order@adq
    l_process_name := p_process_name || '-' ||
                      'evo_geo_countries_by_order@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_geo_countries_by_order@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_geo_countries_by_order@etladq
        (order_id, total_impressions, total_clicks, date_id,
         country_id, country)
        select /*+ parallel(a,5,1) */
         order_id, sum(total_impressions), sum(total_clicks),
         date_id, country_id, country
        from   evo_geo_countries_by_order_ad a
        where  date_id = p_process_date
        group  by order_id, date_id, country_id, country;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- evolution_data.evo_geo_countries_by_ad@adq
    l_process_name := p_process_name || '-' ||
                      'evo_geo_countries_by_ad@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_geo_countries_by_ad@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_geo_countries_by_ad@etladq
        (ad_id, total_impressions, total_clicks, date_id,
         country_id, country)
        select /*+ parallel(a,5,1) */
         ad_id, sum(total_impressions), sum(total_clicks),
         date_id, country_id, country
        from   evo_geo_countries_by_order_ad a
        where  date_id = p_process_date
        group  by ad_id, date_id, country_id, country;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --
    -- evolution_data.evo_geo_states_by_order@adq
    l_process_name := p_process_name || '-' ||
                      'evo_geo_states_by_order@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_geo_states_by_order@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_geo_states_by_order@etladq
        (date_id, order_id, state_province_name,
         total_impressions, total_clicks, country_id)
        select /*+ parallel(a,5,1) */
         date_id, order_id, state_province_name,
         sum(total_impressions), sum(total_clicks),
         country_id
        from   evo_geo_states_by_order_ad a
        where  date_id = p_process_date
        group  by date_id, order_id, state_province_name,
                  country_id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- evolution_data.evo_geo_states_by_ad@adq
    l_process_name := p_process_name || '-' ||
                      'evo_geo_states_by_ad@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from evolution_data.evo_geo_states_by_ad@etladq
        where  date_id = p_process_date;
      end if;
      insert into evolution_data.evo_geo_states_by_ad@etladq
        (date_id, ad_id, state_province_name,
         total_impressions, total_clicks, country_id)
        select /*+ parallel(a,5,1) */
         date_id, ad_id, state_province_name,
         sum(total_impressions), sum(total_clicks),
         country_id
        from   evo_geo_states_by_order_ad a
        where  date_id = p_process_date
        group  by date_id, ad_id, state_province_name,
                  country_id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    if upper(p_process_name) <> 'MANUAL' then
      -- evolution_data.evo_ad_summary@adq
      l_process_name := p_process_name || '-' ||
                        'evo_ad_summary@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_ad_summary@etladq;
        insert into evolution_data.evo_ad_summary@etladq
          (ad_id, ad_name, order_id, rate, pricingtype,
           quantity, total_impressions, total_clicks,
           startdate, enddate, atako, product_id)
          select ad_id, ad_name, order_id, rate, pricingtype,
                 quantity, total_impressions, total_clicks,
                 startdate, enddate, atako, product_id
          from   evo_ad_summary a;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      --
      -- evolution_data.evo_order_summary@adq
      l_process_name := p_process_name || '-' ||
                        'evo_order_summary@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_order_summary@etladq;
        insert into evolution_data.evo_order_summary@etladq
          (order_id, order_name, advertiser_id,
           order_start_date, order_end_date, ad_count,
           net_value)
          select order_id, order_name, advertiser_id,
                 order_start_date, order_end_date, ad_count,
                 net_value
          from   evo_order_summary a;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      --
      -- evolution_data.evo_advertiser_summary@adq
      l_process_name := p_process_name || '-' ||
                        'evo_advertiser_summary@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_advertiser_summary@etladq;
        insert into evolution_data.evo_advertiser_summary@etladq
          (advertiser_id, advertiser_name, order_count)
          select advertiser_id, advertiser_name, order_count
          from   evo_advertiser_summary a;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      --
      -- evolution_data.evo_atako_ad@adq
      l_process_name := p_process_name || '-' ||
                        'evo_atako_ad@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_atako_ad@etladq;
        insert into evolution_data.evo_atako_ad@etladq
          (date_id, ad_id, impressions_count,
           displaytime_total, allinteractions_count,
           views_with_interaction_count,
           interaction_totaltime, clicks, expansion_count,
           expansion_totaltime, video_totaltime,
           video_starts, video_completes, video_1, video_2,
           video_3, video_4, tab_1, tab_2, tab_3, tab_4,
           posts, refreshes, updates, raw_scrolls, scrolls,
           created_by, created_date)
          select date_id, ad_id, impressions_count,
                 displaytime_total, allinteractions_count,
                 views_with_interaction_count,
                 interaction_totaltime, clicks,
                 expansion_count, expansion_totaltime,
                 video_totaltime, video_starts,
                 video_completes, video_1, video_2, video_3,
                 video_4, tab_1, tab_2, tab_3, tab_4, posts,
                 refreshes, updates, raw_scrolls, scrolls,
                 created_by, created_date
          from   evo_atako_ad a;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      --
      -- evo_product_boost@adq
      l_process_name := p_process_name || '-' ||
                        'evo_product_boost@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_product_boost@etladq;
        insert into evolution_data.evo_product_boost@etladq
          (ad_id, impressions, total_clicks, ctr,
           glam_ctr_index, industry_ctr_index,
           total_qualified_interactions, qir, glam_qir_index,
           industry_qir_index, total_expansions,
           expansion_rate, glam_exp_rate_index,
           total_expansion_time, avg_expansion_time,
           avg_int_per_exp, total_video_play_time,
           avg_play_time, video_completion_rate,
           allinteractions_count, video_completes,
           video_starts)
          select ad_id, impressions, total_clicks, ctr,
                 glam_ctr_index, industry_ctr_index,
                 total_qualified_interactions, qir,
                 glam_qir_index, industry_qir_index,
                 total_expansions, expansion_rate,
                 glam_exp_rate_index, total_expansion_time,
                 avg_expansion_time, avg_int_per_exp,
                 total_video_play_time, avg_play_time,
                 video_completion_rate,
                 allinteractions_count, video_completes,
                 video_starts
          from   evo_product_index a
          where  upper(product_name) = 'BOOST';
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- evo_product_dcm@adq
      l_process_name := p_process_name || '-' ||
                        'evo_product_dcm@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_product_dcm@etladq;
        insert into evolution_data.evo_product_dcm@etladq
          (ad_id, impressions, expansions, expansion_rate,
           total_clicks, ctr, glam_ctr_index,
           industry_ctr_index, total_qualified_interactions,
           qir, glam_qir_index, industry_qir_index,
           total_expansions, glam_exp_rate_index,
           total_expansion_time, avg_expansion_time)
          select ad_id, impressions, expansions,
                 expansion_rate, total_clicks, ctr,
                 glam_ctr_index, industry_ctr_index,
                 total_qualified_interactions, qir,
                 glam_qir_index, industry_qir_index,
                 total_expansions, glam_exp_rate_index,
                 total_expansion_time, avg_expansion_time
          from   evo_product_index a
          where  upper(product_name) = 'DCM';
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- evo_product_pushdown@adq
      l_process_name := p_process_name || '-' ||
                        'evo_product_pushdown@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_product_pushdown@etladq;
        insert into evolution_data.evo_product_pushdown@etladq
          (ad_id, impressions, total_clicks, ctr,
           glam_ctr_index, industry_ctr_index,
           total_qualified_interactions, qir, glam_qir_index,
           industry_qir_index, total_expansions,
           expansion_rate, glam_exp_rate_index,
           total_expansion_time, avg_expansion_time,
           avg_int_per_exp, total_video_play_time,
           avg_play_time, video_completion_rate,
           autoexpansion_count, allinteractions_count,
           expansion_totaltime, autoexpansion_totaltime,
           video_starts, video_completes)
          select ad_id, impressions, total_clicks, ctr,
                 glam_ctr_index, industry_ctr_index,
                 total_qualified_interactions, qir,
                 glam_qir_index, industry_qir_index,
                 total_expansions, expansion_rate,
                 glam_exp_rate_index, total_expansion_time,
                 avg_expansion_time, avg_int_per_exp,
                 total_video_play_time, avg_play_time,
                 video_completion_rate, autoexpansion_count,
                 allinteractions_count, expansion_totaltime,
                 autoexpansion_totaltime, video_starts,
                 video_completes
          from   evo_product_index a
          where  upper(product_name) = 'PUSHDOWN';
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- evo_product_eclipse@adq
      l_process_name := p_process_name || '-' ||
                        'evo_product_eclipse@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_product_eclipse@etladq;
        insert into evolution_data.evo_product_eclipse@etladq
          (ad_id, impressions, total_clicks, ctr,
           glam_ctr_index, industry_ctr_index,
           total_qualified_interactions, qir, glam_qir_index,
           industry_qir_index, total_expansions,
           expansion_rate, glam_exp_rate_index,
           total_expansion_time, avg_expansion_time,
           avg_int_per_exp, total_video_play_time,
           avg_play_time, video_completion_rate,
           allinteractions_count, video_completes,
           video_starts)
          select ad_id, impressions, total_clicks, ctr,
                 glam_ctr_index, industry_ctr_index,
                 total_qualified_interactions, qir,
                 glam_qir_index, industry_qir_index,
                 total_expansions, expansion_rate,
                 glam_exp_rate_index, total_expansion_time,
                 avg_expansion_time, avg_int_per_exp,
                 total_video_play_time, avg_play_time,
                 video_completion_rate,
                 allinteractions_count, video_completes,
                 video_starts
          from   evo_product_index a
          where  upper(product_name) = 'ECLIPSE';
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- evo_product_eclipse@adq
      l_process_name := p_process_name || '-' ||
                        'evo_product_splash@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_product_splash@etladq;
        insert into evolution_data.evo_product_splash@etladq
          (ad_id, impressions, total_clicks, ctr,
           glam_ctr_index, industry_ctr_index,
           total_qualified_interactions, qir, glam_qir_index,
           industry_qir_index, total_expansions,
           expansion_rate, glam_exp_rate_index,
           total_expansion_time, avg_expansion_time,
           avg_int_per_exp, total_video_play_time,
           avg_play_time, video_completion_rate,
           allinteractions_count, video_completes,
           video_starts)
          select ad_id, impressions, total_clicks, ctr,
                 glam_ctr_index, industry_ctr_index,
                 total_qualified_interactions, qir,
                 glam_qir_index, industry_qir_index,
                 total_expansions, expansion_rate,
                 glam_exp_rate_index, total_expansion_time,
                 avg_expansion_time, avg_int_per_exp,
                 total_video_play_time, avg_play_time,
                 video_completion_rate,
                 allinteractions_count, video_completes,
                 video_starts
          from   evo_product_index a
          where  upper(product_name) = 'SPLASH';
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- evo_product_reskin@adq
      l_process_name := p_process_name || '-' ||
                        'evo_product_reskin@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_product_reskin@etladq;
        insert into evolution_data.evo_product_reskin@etladq
          (ad_id, impressions, total_pages,
           total_dwell_display_time, avg_dwell_display_time,
           total_clicks, click_per_page_rate, glam_cpp_index,
           total_video_play_time, avg_play_time,
           video_completion_rate)
          select ad_id, impressions, total_pages,
                 total_dwell_display_time,
                 avg_dwell_display_time, total_clicks,
                 click_per_page_rate, glam_cpp_index,
                 total_video_play_time, avg_play_time,
                 video_completion_rate
          from   evo_product_ad_size a
          where  upper(product_name) = 'RESKIN';
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- evo_product_expert@adq
      l_process_name := p_process_name || '-' ||
                        'evo_product_expert@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_product_expert@etladq;
        insert into evolution_data.evo_product_expert@etladq
          (ad_id, impressions, total_pages,
           total_dwell_display_time, avg_dwell_display_time,
           total_clicks, click_per_page_rate, glam_cpp_index,
           total_video_play_time, avg_play_time,
           video_completion_rate)
          select ad_id, impressions, total_pages,
                 total_dwell_display_time,
                 avg_dwell_display_time, total_clicks,
                 click_per_page_rate, glam_cpp_index,
                 total_video_play_time, avg_play_time,
                 video_completion_rate
          from   evo_product_ad_size a
          where  upper(product_name) = 'EXPERT';
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- evo_product_brand_advocate@adq
      l_process_name := p_process_name || '-' ||
                        'evo_product_brand_advocate@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_product_brand_advocate@etladq;
        insert into evolution_data.evo_product_brand_advocate@etladq
          (ad_id, impressions, total_pages,
           total_dwell_display_time, avg_dwell_display_time,
           total_clicks, click_per_page_rate, glam_cpp_index,
           total_video_play_time, avg_play_time,
           video_completion_rate)
          select ad_id, impressions, total_pages,
                 total_dwell_display_time,
                 avg_dwell_display_time, total_clicks,
                 click_per_page_rate, glam_cpp_index,
                 total_video_play_time, avg_play_time,
                 video_completion_rate
          from   evo_product_ad_size a
          where  upper(product_name) = 'BRAND ADVOCATE';
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- evo_product_other@adq
      l_process_name := p_process_name || '-' ||
                        'evo_product_other@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_product_other@etladq;
        insert into evolution_data.evo_product_other@etladq
          (ad_id, impressions, total_clicks, ctr)
          select ad_id, impressions, total_clicks, ctr
          from   evo_product_other a;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- evo_product_standard_media@adq
      l_process_name := p_process_name || '-' ||
                        'evo_product_standard_media@etladq';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        delete from evolution_data.evo_product_standard_media@etladq;
        insert into evolution_data.evo_product_standard_media@etladq
          (ad_id, rate, unit, impressions, total_clicks,
           ctr)
          select ad_id, rate, unit, impressions,
                 total_clicks, ctr
          from   evo_product_standard_media a;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      --
      begin
        for i in (select ext_product_tab_name,
                         ext_product_name
                  from   evo_tally
                  where  ext_product_name is not null) loop
          l_process_name := p_process_name || '-' ||
                            i.ext_product_tab_name ||
                            '@etladq';
          if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
            pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
            execute immediate '
              delete from evolution_data.' ||
                              i.ext_product_tab_name ||
                              '@etladq';
            execute immediate '
              insert into evolution_data.' ||
                              i.ext_product_tab_name ||
                              '@etladq
                (ad_id, impressions_count, displaytime_total,
                 allinteractions_count,
                 views_with_interaction_count,
                 interaction_totaltime, clicks, expansion_count,
                 expansion_totaltime, video_totaltime,
                 video_starts, video_completes, video_1, video_2,
                 video_3, video_4, tab_1, tab_2, tab_3, tab_4,
                 posts, refreshes, updates, raw_scrolls, scrolls,
                 tab_5, tab_6, tab_7, tab_8, tab_9, tab_10, shares,
                 exit_1, exit_2, exit_3, exit_4, exit_5, exit_6,
                 exit_7, exit_8, exit_9, exit_10, refresh_changes,
                 logins, qualified_interaction_count, all_tabs,
                 qualified_interaction_duration,
                 autoexpansion_count, autoexpansion_totaltime,
                 fullscreen_count, fullscreen_totaltime,
                 video25_count, video50_count, video75_count,
                 pause_count, mute_count, error_num)
                select /*+ parallel(a,4) */
                 ad_id, impressions_count, displaytime_total,
                 allinteractions_count,
                 views_with_interaction_count,
                 interaction_totaltime, clicks, expansion_count,
                 expansion_totaltime, video_totaltime,
                 video_starts, video_completes, video_1, video_2,
                 video_3, video_4, tab_1, tab_2, tab_3, tab_4,
                 posts, refreshes, updates, raw_scrolls, scrolls,
                 tab_5, tab_6, tab_7, tab_8, tab_9, tab_10, shares,
                 exit_1, exit_2, exit_3, exit_4, exit_5, exit_6,
                 exit_7, exit_8, exit_9, exit_10, refresh_changes,
                 logins, qualified_interaction_count, all_tabs,
                 qualified_interaction_duration,
                 autoexpansion_count, autoexpansion_totaltime,
                 fullscreen_count, fullscreen_totaltime,
                 video25_count, video50_count, video75_count,
                 pause_count, mute_count, error_num
                from   evo_product_ext a
                where  upper(product_name) = ''' ||
                              i.ext_product_name || '''';
            l_cnt := sql%rowcount;
            commit;
            pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
          end if;
        end loop;
      end;
    end if;
    l_process_name := p_process_name || '-' ||
                      'evo_sanity_check@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from evolution_data.evo_sanity_check@etladq
      where  data_date_id = p_process_date;
      for i in (select * from evo_tally order by group_id) loop
        execute immediate '
          insert into evolution_data.evo_sanity_check@etladq
            (group_id, data_date_id, total_impressions,
             total_clicks)
            select :process_group_id, :process_date,
                   coalesce(sum(' ||
                          i.imp_col_name ||
                          '), 0), coalesce(sum(' ||
                          i.clk_col_name ||
                          '), 0)
            from   ' ||
                          i.table_name || ' 
            where  date_id = :process_date'
          using i.group_id, p_process_date, p_process_date;
        commit;
      end loop;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', '');
    end if;
  end;

  procedure manage_partitions
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_date         number;
  begin
    l_date         := to_number(to_char(trunc(add_months(to_date(p_process_date, 'yyyymmdd'), -3), 'mm'), 'yyyymmdd'));
    l_process_name := p_process_name || '-' ||
                      'manage_partitions_' || l_date;
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      for i in (select *
                from   user_tab_partitions
                where  table_name like 'EVO_%'
                and    table_name not in
                       ('EVO_ATAKO_AGG_ADSIZE', 'EVO_ATAKO_AGG_DAY', 'EVO_MEDIA_CAMP_AGG_DAY')
                and    to_number(replace(partition_name, 'P_', '')) <
                       l_date
                order  by table_name, partition_name) loop
        begin
          execute immediate ' alter table ' || i.table_name ||
                            ' drop partition ' ||
                            i.partition_name;
        exception
          when others then
            null;
        end;
      end loop;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', '');
    end if;
  end;

end pkg_evolution;
/
