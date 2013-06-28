create or replace package pkg_sequence_aggregate as

  procedure run_inventory
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_ghost_staging
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ghost_imps_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_adq_request_seq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ghost_user_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_adq_request_seq_agg
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

  procedure manage_partitions
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_sequence_aggregate as

  procedure run_inventory
  (
    p_process_name varchar2,
    p_process_date number
  ) is
    l_run_id        number;
    l_process_date  number;
    l_process_name  varchar2(10);
    l_error_msg     varchar2(4000);
    l_status        varchar2(100);
    l_start_process varchar2(100);
    l_mail_msg      varchar2(100);
    l_to            varchar2(1000);
  begin
    l_process_name  := initcap(p_process_name);
    l_start_process := l_process_name || '-run_inventory';
    l_mail_msg      := 'Sequence Aggregate Data';
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
      -- send intimation mail for starting of process
      pa_send_email1.compose_email(l_run_id, 'START', l_process_date, l_mail_msg, l_start_process, l_to);
    end if;
    -- make sure we delete the record for the main process name(this has dependency in shell script).
    delete from daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) = upper(l_start_process)
    and    run_id = l_run_id
    and    status in ('RUNNING', 'FAIL');
    commit;
    -- log starting of the process
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'I');
    -- populate the base table first
    load_ghost_staging(l_run_id, l_process_date, l_process_name);
    load_ghost_imps_day(l_run_id, l_process_date, l_process_name);
    load_adq_request_seq(l_run_id, l_process_date, l_process_name);
    load_ghost_user_day(l_run_id, l_process_date, l_process_name);
    select max(status)
    into   l_status
    from   daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) =
           case
             when upper(p_process_name) = 'PROCESS' then
              'PROCESS'
             when upper(p_process_name) = 'REPROCESS' then
              'REPROCESS'
             when upper(p_process_name) = 'MANUAL' then
              'REPROCESS'
           end || '-FACT_MEDIA_DAY_AGG'
    and    status = 'COMPLETE';
    if l_status = 'COMPLETE' then
      load_adq_request_seq_agg(l_run_id, l_process_date, l_process_name);
      -- replicates to ADQ
      replicate_to_adq(l_run_id, l_process_date, l_process_name);
      -- mark the process as complete in log table
      pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'UC');
      manage_partitions(l_run_id, l_process_date, l_process_name);
      -- send completion mail
      pa_send_email1.compose_email(l_run_id, 'COMPLETE', l_process_date, l_mail_msg, l_start_process, l_to);
    else
      l_error_msg := 'Campaign-Fact_Media_Day not completed yet.';
      insert into daily_process_log
        (run_id, process_date, process_name, start_time,
         status, error_reason)
      values
        (l_run_id, l_process_date,
         l_process_name || '-sequence_fact_media_day',
         sysdate, 'FAIL', l_error_msg);
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      commit;
    end if;
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      -- failed steps are handled in compose mail message procedure
      pa_send_email1.compose_email(l_run_id, 'FAIL', l_process_date, l_mail_msg, l_start_process, l_to);
  end;

  procedure load_ghost_staging
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_next_date    number;
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_next_date    := to_number(to_char(to_date(p_process_date, 'yyyymmdd') + 1, 'yyyymmdd'));
    l_process_name := p_process_name || '-' ||
                      'truncate_fact_ghost_stg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'analyze table fact_ghost_stg estimate statistics';
      execute immediate 'truncate table fact_ghost_stg';
      execute immediate 'analyze table fact_ghost_stg estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- LOOP THROUGH VARIOUS TABLES/PARTITIONS TO BE INSERTED
    for i in (select level,
                     case
                       when level in (1, 2) then
                        'GA_RAW_GHOST_IMPS_NEW'
                       when level in (3, 4) then
                        'GA_RAW_GHOST_TLOGS'
                     end as table_name,
                     case
                       when level in (1, 3) then
                        'P_' || p_process_date
                       when level in (2, 4) then
                        'P_' || l_next_date
                     end as partition_name,
                     case
                       when level in (1, 3) then
                        ' date_hour = 1 '
                       when level in (2, 4) then
                        ' date_hour = 0 '
                     end as where_cond
              from   dual
              connect by level <= 4) loop
      l_process_name := p_process_name || '-' ||
                        i.table_name || '_' ||
                        i.partition_name;
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        -- do the raw table partition analyze
        execute immediate 'analyze table adaptive_data.' ||
                          i.table_name || ' partition(' ||
                          i.partition_name ||
                          ') estimate statistics';
        execute immediate '
        insert /*+ append */
        into fact_ghost_stg
          (request_seq, request_status, user_id,
           affiliate_id, site_id, ad_size_request, ad_id,
           creative_id, content_partner_id, data_id,
           developer_id, module_id, tile, tag_type, atf_value,
           flags, country_id, dma_id, state_province, zip_code,
           city_id, internal_ip, ad_group_id, ga_os_id,
           ga_browser_id, impressions, date_id, advertiser_id, 
           order_id, agent_type, country_code)
          select /*+ parallel(a,4,1) */
           request_seq, request_status, user_id,
           affiliate_id, site_id, ad_size_request, a.ad_id,
           creative_id, content_partner_id, data_id,
           developer_id, module_id, tile, tag_type, atf_value,
           flags, coalesce(coun.country_id, 0), dma_id, state_province,
           zip_code, city_id, internal_ip, ad_group_id, ga_os_id,
           ga_browser_id, impressions, ' ||
                          p_process_date ||
                          ', coalesce(ord.advertiser_id, 0),
           coalesce(ord.order_id, 0), agent_type,
           decode(upper(a.country_id), ''GB'', ''UK'', upper(a.country_id)) as country_code
          from   adaptive_data.' ||
                          i.table_name || ' partition(' ||
                          i.partition_name ||
                          ') a,
                 ods_metadata.adm_ads ord, dim_countries coun
          where  ' || i.where_cond || '
          and    a.ad_id = ord.ad_id(+)
          and    upper(a.country_id) = coun.adaptive_code(+)';
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
    end loop;
    execute immediate 'analyze table fact_ghost_stg estimate statistics';
  end;

  procedure load_ghost_imps_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_ghost_imps_day';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_ghost_imps_day truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into fact_ghost_imps_day
        (date_id, advertiser_id, order_id, request_seq,
         request_status, affiliate_id, site_id,
         ad_size_request, ad_id, creative_id, page_id,
         content_partner_id, data_id, developer_id,
         module_id, tile, tag_type, atf_value, flags,
         country_id, dma_id, state_province, zip_code,
         city_id, internal_ip, ad_group_id, ga_os_id,
         ga_browser_id, impressions, zone_id, agent_type,
         country_code)
        select /*+ parallel(a,8,1) */
         date_id, advertiser_id, order_id, request_seq,
         request_status, affiliate_id, site_id,
         ad_size_request, ad_id, creative_id,
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
                  end, -20) as page_id, content_partner_id,
         data_id, developer_id, module_id, tile, tag_type,
         atf_value, flags, country_id, dma_id,
         state_province,
         to_char(regexp_substr(zip_code, '^[0-9]{5}')),
         -1 as city_id, internal_ip, ad_group_id, ga_os_id,
         ga_browser_id, sum(impressions),
         decode(bitand(coalesce(greatest(flags, 0), 0), 16), 16, -21, -20) as zone_id,
         agent_type, country_code
        from   fact_ghost_stg a
        group  by date_id, advertiser_id, order_id,
                  request_seq, request_status, affiliate_id,
                  site_id, ad_size_request, ad_id,
                  creative_id,
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
                           end, -20), content_partner_id,
                  data_id, developer_id, module_id, tile,
                  tag_type, atf_value, flags, country_id,
                  dma_id, state_province,
                  to_char(regexp_substr(zip_code, '^[0-9]{5}')),
                  internal_ip, ad_group_id, ga_os_id,
                  ga_browser_id,
                  decode(bitand(coalesce(greatest(flags, 0), 0), 16), 16, -21, -20),
                  agent_type, country_code;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_ghost_imps_day partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_adq_request_seq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_adq_request_seq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_request_seq truncate partition P_' ||
                        p_process_date;
      insert /*+ APPEND */
      into fact_adq_request_seq
        (date_id, request_seq, affiliate_id, ad_id,
         count_number, country_id, state_province, ga_city,
         dma_id)
        select /*+ parallel(a,5,1) */
         date_id, request_seq, affiliate_id, ad_id,
         sum(impressions), country_id, state_province,
         city_id, dma_id
        from   fact_ghost_imps_day a
        where  date_id = p_process_date
        group  by date_id, request_seq, affiliate_id, ad_id,
                  country_id, state_province, city_id,
                  dma_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_request_seq partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ghost_user_day
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_ghost_user_day';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_ghost_user_day truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into fact_ghost_user_day
        (date_id, advertiser_id, order_id, request_seq,
         request_status, user_id, affiliate_id, site_id,
         ad_size_request, ad_id, creative_id,
         content_partner_id, data_id, developer_id,
         module_id, tile, tag_type, atf_value, flags,
         country_id, dma_id, state_province, zip_code,
         city_id, internal_ip, ad_group_id, ga_os_id,
         ga_browser_id, impressions)
        select /*+ parallel(a,5,1) */
         date_id, advertiser_id, order_id, request_seq,
         request_status, user_id, affiliate_id, site_id,
         ad_size_request, a.ad_id, creative_id,
         content_partner_id, data_id, developer_id,
         module_id, tile, tag_type, atf_value, flags,
         country_id, dma_id, state_province, zip_code,
         city_id, internal_ip, ad_group_id, ga_os_id,
         ga_browser_id, sum(impressions)
        from   fact_ghost_imps_day a
        where  date_id = p_process_date
        and    request_seq = 0
        and    ad_size_request = '555x2'
        group  by date_id, advertiser_id, order_id,
                  request_seq, request_status, user_id,
                  affiliate_id, site_id, ad_size_request,
                  a.ad_id, creative_id, content_partner_id,
                  data_id, developer_id, module_id, tile,
                  tag_type, atf_value, flags, country_id,
                  dma_id, state_province, zip_code, city_id,
                  internal_ip, ad_group_id, ga_os_id,
                  ga_browser_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_ghost_user_day partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_adq_request_seq_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_adq_request_seq_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_adq_request_seq_agg truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into fact_adq_request_seq_agg
        (date_id, ad_id, affiliate_id, country_id,
         state_province, dma_id, ga_city, seq_0_ad_call,
         seq_123_ad_call, seq_4_ad_call, seq_5_ad_call,
         seq_6_ad_call, seq_7_ad_call, total_impressions,
         total_clicks)
        select /*+ parallel (c,5,1) */
         date_id, ad_id, affiliate_id, country_id,
         state_province, dma_id, ga_city,
         sum(decode(request_seq, 0, count_number, 0)) as seqid_0_ad_call,
         sum(case
               when request_seq in (1, 2, 3) then
                count_number
               else
                null
             end) as seqid_123_ad_call,
         sum(decode(request_seq, 4, count_number, 0)) as seqid_4_ad_call,
         sum(decode(request_seq, 5, count_number, 0)) as seqid_5_ad_call,
         sum(decode(request_seq, 6, count_number, 0)) as seqid_6_ad_call,
         sum(decode(request_seq, 7, count_number, 0)) as seqid_7_ad_call,
         sum(total_impressions) as total_impressions,
         sum(total_clicks) as total_clicks
        from   (select /*+ parallel (a,5,1) */
                  date_id, ad_id, affiliate_id, country_id,
                  upper(state_province) as state_province,
                  dma_id, upper(ga_city) as ga_city,
                  request_seq, count_number,
                  0 as total_impressions, 0 as total_clicks
                 from   fact_adq_request_seq a
                 where  date_id = p_process_date
                 union all
                 select /*+ parallel (b,5,1) */
                  date_id, ad_id, affiliate_id, country_id,
                  upper(state_province), dma_id,
                  upper(ga_city), request_seq,
                  total_impressions as count_number,
                  total_impressions, total_clicks
                 --from   fact_adq_flag_agg a
                 from   fact_media_day_agg b
                 where  date_id = p_process_date) c
        group  by date_id, ad_id, affiliate_id, country_id,
                  state_province, dma_id, ga_city;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table fact_adq_request_seq_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
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
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_adq_request_seq_agg@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        campaign.pkg_generic_ddl.truncate_partition@etladq('campaign', 'fact_adq_request_seq_agg', 'P_' ||
                                                            p_process_date);
      end if;
      insert /*+ append */
      into fact_adq_request_seq_agg@etladq
        (date_id, ad_id, affiliate_id, country_id,
         state_province, dma_id, ga_city, seq_0_ad_call,
         seq_123_ad_call, seq_4_ad_call, seq_5_ad_call,
         seq_6_ad_call, seq_7_ad_call, total_impressions,
         total_clicks)
        select /*+ parallel (a,5,1) */
         date_id, ad_id, affiliate_id, country_id,
         state_province, dma_id, ga_city, seq_0_ad_call,
         seq_123_ad_call, seq_4_ad_call, seq_5_ad_call,
         seq_6_ad_call, seq_7_ad_call, total_impressions,
         total_clicks
        from   fact_adq_request_seq_agg a
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_generic_ddl.analyze_partition@etladq('campaign', 'fact_adq_request_seq_agg', 'P_' ||
                                                         p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure manage_partitions
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_date         varchar2(8);
  begin
    l_date         := to_char(to_date(p_process_date, 'yyyymmdd') - 45, 'yyyymmdd');
    l_process_name := p_process_name || '-' ||
                      'drop_partitions';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      for i in (select *
                from   all_tab_partitions
                where  table_name in
                       ('GA_RAW_GHOST_IMPS_NEW', 'GA_RAW_GHOST_TLOGS')
                and    to_char(replace(partition_name, 'P_', '')) <
                       l_date
                order  by table_name, partition_name) loop
        begin
          execute immediate ' alter table ' ||
                            i.table_owner || '.' ||
                            i.table_name ||
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

end;
/
