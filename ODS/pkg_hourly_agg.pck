create or replace package pkg_hourly_agg as

  procedure run_agg;

  procedure drop_partitions(l_process_date number);

  procedure add_partitions;

  procedure load_imp_clk
  (
    p_run_id            number,
    p_current_date_hour number,
    p_process_name      varchar2
  );

  procedure load_imp_clk_agg
  (
    p_run_id             number,
    p_current_date_hour  number,
    p_process_name       varchar2,
    p_previous_date_hour number
  );

  procedure load_hourly_agg
  (
    p_run_id            number,
    p_current_date_hour number,
    p_process_name      varchar2
  );

  procedure load_lam_ads_gpo
  (
    p_run_id            number,
    p_current_date_hour number,
    p_process_name      varchar2
  );

end;
/
create or replace package body pkg_hourly_agg as

  procedure run_agg as
    l_hour         varchar2(100);
    l_run_id       number;
    l_cnt          number;
    l_process_date number;
    l_process_name varchar2(100) := 'Process';
  begin
    --l_run_id := seq_daily_campaign_run.nextval;
    l_run_id := 2245;
    -- extract the UTC date and hour for processing
    l_process_date := to_number(to_char(sysdate + 3 / 24, 'yyyymmdd'));
    l_hour         := to_char(sysdate + 3 / 24, 'hh24');
    if l_hour = '00' then
      -- do the partition maintanance
      drop_partitions(l_process_date);
      add_partitions;
      select count(1)
      into   l_cnt
      from   hourly_process_log
      where  run_id = 2245
      and    process_date = l_process_date;
      if l_cnt = 0 then
        -- delete 2 days old daily_process_log records from the table
        delete from daily_process_log
        where  run_id = 2245
        and    process_date <=
               to_number(to_char(to_date(l_process_date, 'yyyymmdd') - 2, 'yyyymmdd') || '23');
        -- delete 2 days old hourly_process_log records from the table
        delete from hourly_process_log
        where  run_id = 2245
        and    process_date <=
               to_number(to_char(to_date(l_process_date, 'yyyymmdd') - 2, 'yyyymmdd'));
        insert into hourly_process_log -- 2245  2561  run_ids
          (run_id, process_date, process_name,
           current_date_hour, previous_date_hour, status)
          select l_run_id, l_process_date,
                 c_hour as process_name,
                 l_process_date || c_hour as current_date_hour,
                 l_process_date || p_hour as previous_date_hour,
                 'SCHEDULED' as status
          from   (select decode(length(c_hour), 1, '0' ||
                                   c_hour, c_hour) as c_hour,
                          replace(decode(length(p_hour), 1, '0' ||
                                           p_hour, p_hour), -1, '00') as p_hour
                   from   (select level - 1 as c_hour,
                                   level - 2 as p_hour
                            from   dual
                            connect by level <= 24)
                   order  by c_hour);
        commit;
      end if;
    end if;
    for i in (select run_id, process_date, process_name,
                     current_date_hour, previous_date_hour,
                     status
              from   hourly_process_log
              where  run_id = 2245
              and    status <> 'COMPLETE'
              and    process_date >=
                     to_number(to_char(to_date(l_process_date, 'yyyymmdd') - 1, 'yyyymmdd'))
              and    process_date <= l_process_date
              order  by process_date, to_number(process_name)) loop
      begin
        -- we need to do minus -1 hour processing, so if l_hour = i.process_hour then we have to do exit
        if i.process_name = l_hour and
           i.process_date = l_process_date then
          exit;
        end if;
        -- mark the start time 
        update hourly_process_log
        set    start_time = sysdate
        --where  run_id = i.run_id
        where  process_date = i.process_date
        and    process_name = i.process_name;
        if to_char(to_date(i.current_date_hour, 'yyyymmddhh24'), 'hh24') = '00' then
          l_cnt := 0;
          select count(1)
          into   l_cnt
          from   campaign.tz_agg_hist@etladq
          where  trunc(agg_date, 'dd') =
                 trunc(to_date(i.current_date_hour, 'yyyymmddhh24'), 'dd');
          if l_cnt = 0 then
            insert into campaign.tz_agg_hist@etladq
              (id, agg_date, time_zone, is_day_agg,
               is_dedup_agg, start_hour, end_hour)
            values
              (2245,
               to_date(i.current_date_hour, 'yyyymmddhh24'),
               'EST', 0, 0,
               to_date(i.current_date_hour, 'yyyymmddhh24') - 6,
               '');
            insert into ade_data.adq_tz_agg_hist_est@etlgad
              (id, agg_date, time_zone, is_day_agg,
               is_dedup_agg, start_hour, end_hour)
            values
              (2245,
               to_date(i.current_date_hour, 'yyyymmddhh24'),
               'EST', 0, 0,
               to_date(i.current_date_hour, 'yyyymmddhh24') - 6,
               '');
          end if;
        end if;
        load_imp_clk(l_run_id, i.current_date_hour, l_process_name);
        load_imp_clk_agg(l_run_id, i.current_date_hour, l_process_name, i.previous_date_hour);
        --load_lam_ads_gpo(l_run_id, i.current_date_hour, l_process_name);
        --load_hourly_agg(l_run_id, i.current_date_hour, l_process_name);
        -- mark the process as complete
        update hourly_process_log
        set    status = 'COMPLETE', end_time = sysdate
        --where  run_id = i.run_id
        where  process_date = i.process_date
        and    process_name = i.process_name;
        update campaign.tz_agg_hist@etladq
        set    end_hour = to_date(i.current_date_hour, 'yyyymmddhh24')
        where  trunc(agg_date, 'dd') =
               trunc(to_date(i.current_date_hour, 'yyyymmddhh24'), 'dd');
        update ade_data.adq_tz_agg_hist_est@etlgad
        set    end_hour = to_date(i.current_date_hour, 'yyyymmddhh24')
        where  trunc(agg_date, 'dd') =
               trunc(to_date(i.current_date_hour, 'yyyymmddhh24'), 'dd');
        commit;
      exception
        when others then
          -- mark the process as fail
          update hourly_process_log
          set    status = 'FAIL', end_time = sysdate,
                 error_reason = dbms_utility.format_error_backtrace ||
                                 dbms_utility.format_error_stack
          --where  run_id = i.run_id
          where  process_date = i.process_date
          and    process_name = i.process_name;
          commit;
          pa_send_email.mail('odsimportreport@glam.com', 'nileshm@glam.com', 'pkg_hourly_agg', 'pkg_hourly_agg has been failed.' ||
                              chr(10) ||
                              sqlerrm);
          -- we need to exit on failuare becoz we are aggregating the data using previous data and current data 
          exit;
      end;
    end loop;
  end;

  procedure drop_partitions(l_process_date number) as
    l_part_name varchar2(100);
  begin
    l_part_name := to_char(to_date(l_process_date, 'yyyymmdd') - 7, 'yyyymmdd') || '00';
    for x in (select table_owner, table_name, partition_name
              from   dba_tab_partitions
              where  table_name in
                     ('RT_IMP_CLK', 'RT_IMP_CLK_AGG')
              and    replace(partition_name, 'P_', '') <
                     l_part_name
              order  by table_name, partition_position) loop
      begin
        execute immediate '
          alter table ' ||
                          x.table_name || '
          drop partition ' ||
                          x.partition_name;
      exception
        when others then
          null;
      end;
    end loop;
  end;

  procedure add_partitions as
    l_sql varchar2(4000);
  begin
    for x in (select table_name, tablespace_name,
                     max(to_number(to_char(to_date(replace(partition_name, 'P_', ''), 'yyyymmddhh24'), 'yyyymmdd'))) as last_partition
              from   dba_tab_partitions
              where  table_name in
                     ('RT_IMP_CLK', 'RT_IMP_CLK_AGG', 'LAM_ADS_GPO_STG')
              group  by table_name, tablespace_name) loop
      for i in (select *
                from   dim_date
                where  date_id > x.last_partition
                and    date_id <=
                       to_number(to_char(to_date(x.last_partition, 'yyyymmdd') + 1, 'yyyymmdd'))) loop
        for j in (select i.date_id ||
                          decode(length(c_hour), 1, '0' ||
                                  c_hour, c_hour) as part_name,
                         case
                           when n_hour <> 24 then
                            i.date_id ||
                            decode(length(n_hour), 1, '0' ||
                                    n_hour, n_hour)
                           else
                            i.next_date_id ||
                            replace(n_hour, '24', '00')
                         end as part_val
                  from   (select level - 1 as c_hour,
                                  level as n_hour
                           from   dual
                           connect by level <= 24)
                  order  by c_hour) loop
          l_sql := 'alter table ' || x.table_name || ' 
                  add partition p_' ||
                   j.part_name || ' values less than (' ||
                   j.part_val || ')';
          execute immediate l_sql;
        end loop;
      end loop;
    end loop;
  end;

  procedure load_imp_clk
  (
    p_run_id            number,
    p_current_date_hour number,
    p_process_name      varchar2
  ) as
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' || 'rt_imp_clk';
    if pkg_log_process.is_not_complete(p_run_id, p_current_date_hour, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'I');
      --analyze raw table partition before processing
      execute immediate 'analyze table intra_data.fact_media_stage_new partition (P_' ||
                        p_current_date_hour ||
                        ') estimate statistics';
      execute immediate 'Alter table rt_imp_clk truncate partition P_' ||
                        p_current_date_hour;
      insert /*+ append */
      into rt_imp_clk
        (date_id, advertiser_id, order_id, ad_id,
         creative_id, affiliate_id, dma_id, country_id,
         /*state_province, ga_city,*/ total_impressions,
         total_clicks, ad_group_id, source_id, network_name,
         vertical, primary_channel, country_code, /*flags,*/
         ad_dimension, ga_os_id, ga_browser_id, tile,
         tag_type, atf_value, zip_code, region_date_coc,
         region_code, coc, time_stamp)
      -- intra_data.fact_media_stage_new
        select /*+ parallel(a,4,1) */
         p_current_date_hour as date_id,
         coalesce(ord.advertiser_id, 0),
         coalesce(ord.order_id, 0), a.ad_id, creative_id,
         a.affiliate_id, dma_id, coalesce(b.country_id, 0),
         /*state_province, ga_city,*/
         sum(a.total_impressions) total_impressions,
         sum(a.total_clicks) total_clicks, a.ad_group_id,
         a.source_id, c.network network_name,
         c.site_uber_category vertical,
         c.category_name_full primary_channel,
         decode(upper(a.country_id), 'GB', 'UK', upper(a.country_id)),
         /*flags,*/ ad_size_id, ga_os_id, ga_browser_id,
         tile, tag_type, coalesce(is_atf, 'u'),
         to_number(regexp_substr(zip_code, '^[0-9]{5}')),
         case
           when c.rowid is null or
                a.source_id in (1001, 1002) then
           -- US offset is -4, hence deduct it from time_stamp which is in UTC
            to_number(to_char(trunc(a.time_stamp - 4 / 24, 'dd'), 'yyyymmdd'))
           else
            to_number(to_char(trunc(a.time_stamp +
                                    coalesce(coc_offset, 0) / 24, 'dd'), 'yyyymmdd'))
         end region_date_coc,
         case
           when a.source_id = '1001' then
            '1001'
           when a.source_id = '1002' then
            '1002'
           when c.rowid is null then
            'OTHER'
           else
            coalesce(c.region_code, 'OTHER')
         end region_code,
         case
           when a.source_id = '1001' then
            '1001'
           when a.source_id = '1002' then
            '1002'
           when c.rowid is null then
            'Non_coc_aff'
           else
            c.country
         end coc
         -- US offset is -4, hence deduct it from time_stamp which is in UTC
        , time_stamp - 4 / 24
        from   intra_data.fact_media_stage_new a,
               new_channel_agg c, dim_countries b,
               ods_metadata.adm_ads ord
        where  a.date_hour = p_current_date_hour
        and    a.ad_id = ord.ad_id(+)
        and    a.affiliate_id = c.affiliate_id(+)
        and    upper(a.country_id) = b.adaptive_code(+)
        group  by coalesce(ord.advertiser_id, 0),
                  coalesce(ord.order_id, 0), a.ad_id,
                  creative_id, a.affiliate_id, dma_id,
                  coalesce(b.country_id, 0),
                  /*state_province, ga_city,*/ a.ad_group_id,
                  a.source_id, c.network,
                  c.site_uber_category, c.category_name_full,
                  decode(upper(a.country_id), 'GB', 'UK', upper(a.country_id)),
                  /*flags,*/ ad_size_id, ga_os_id,
                  ga_browser_id, tile, tag_type,
                  coalesce(is_atf, 'u'),
                  to_number(regexp_substr(zip_code, '^[0-9]{5}')),
                  case
                    when c.rowid is null or
                         a.source_id in (1001, 1002) then
                    -- US offset is -4, hence deduct it from time_stamp which is in UTC
                     to_number(to_char(trunc(a.time_stamp -
                                             4 / 24, 'dd'), 'yyyymmdd'))
                    else
                     to_number(to_char(trunc(a.time_stamp +
                                             coalesce(coc_offset, 0) / 24, 'dd'), 'yyyymmdd'))
                  end,
                  case
                    when a.source_id = '1001' then
                     '1001'
                    when a.source_id = '1002' then
                     '1002'
                    when c.rowid is null then
                     'OTHER'
                    else
                     coalesce(c.region_code, 'OTHER')
                  end,
                  case
                    when a.source_id = '1001' then
                     '1001'
                    when a.source_id = '1002' then
                     '1002'
                    when c.rowid is null then
                     'Non_coc_aff'
                    else
                     c.country
                  end
                  -- US offset is -4, hence deduct it from time_stamp which is in UTC
                 , time_stamp - 4 / 24;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table rt_imp_clk partition (P_' ||
                        p_current_date_hour ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_imp_clk_agg
  (
    p_run_id             number,
    p_current_date_hour  number,
    p_process_name       varchar2,
    p_previous_date_hour number
  ) as
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'rt_imp_clk_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_current_date_hour, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'I');
      execute immediate 'Alter table rt_imp_clk_agg truncate partition P_' ||
                        p_current_date_hour;
      insert /*+ append */
      into rt_imp_clk_agg
        (date_id, advertiser_id, order_id, ad_id,
         creative_id, affiliate_id, dma_id, country_id,
         state_province, ga_city, total_impressions,
         total_clicks, ad_group_id, source_id, network_name,
         vertical, primary_channel, country_code, flags,
         ad_dimension, ga_os_id, ga_browser_id, tile,
         tag_type, atf_value, zip_code, region_date_coc,
         region_code, coc, time_stamp)
        select p_current_date_hour as date_id, advertiser_id,
               order_id, ad_id, creative_id, affiliate_id,
               dma_id, country_id, state_province, ga_city,
               sum(total_impressions), sum(total_clicks),
               ad_group_id, source_id, network_name,
               vertical, primary_channel, country_code,
               flags, ad_dimension, ga_os_id, ga_browser_id,
               tile, tag_type, atf_value, zip_code,
               region_date_coc, region_code, coc, time_stamp
        from   (select /*+ parallel(a,4,1) */
                  advertiser_id, order_id, ad_id, creative_id,
                  affiliate_id, dma_id, country_id,
                  state_province, ga_city, total_impressions,
                  total_clicks, ad_group_id, source_id,
                  network_name, vertical, primary_channel,
                  country_code, flags, ad_dimension, ga_os_id,
                  ga_browser_id, tile, tag_type, atf_value,
                  zip_code, region_date_coc, region_code, coc,
                  time_stamp
                 from   rt_imp_clk_agg a
                 where  date_id = p_previous_date_hour
                 union all
                 select /*+ parallel(b,4,1) */
                  advertiser_id, order_id, ad_id, creative_id,
                  affiliate_id, dma_id, country_id,
                  state_province, ga_city, total_impressions,
                  total_clicks, ad_group_id, source_id,
                  network_name, vertical, primary_channel,
                  country_code, flags, ad_dimension, ga_os_id,
                  ga_browser_id, tile, tag_type, atf_value,
                  zip_code, region_date_coc, region_code, coc,
                  time_stamp
                 from   rt_imp_clk b
                 where  date_id = p_current_date_hour)
        group  by advertiser_id, order_id, ad_id,
                  creative_id, affiliate_id, dma_id,
                  country_id, state_province, ga_city,
                  ad_group_id, source_id, network_name,
                  vertical, primary_channel, country_code,
                  flags, ad_dimension, ga_os_id,
                  ga_browser_id, tile, tag_type, atf_value,
                  zip_code, region_date_coc, region_code,
                  coc, time_stamp;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table rt_imp_clk_agg partition (P_' ||
                        p_current_date_hour ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_hourly_agg
  (
    p_run_id            number,
    p_current_date_hour number,
    p_process_name      varchar2
  ) as
    l_date         number;
    l_hour         number;
    l_part_name    number;
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_date := to_number(to_char(to_date(p_current_date_hour, 'yyyymmddhh24'), 'yyyymmdd'));
    l_hour := to_number(to_char(to_date(p_current_date_hour, 'yyyymmddhh24'), 'hh24'));
    if l_hour = 0 then
      l_part_name := to_number(to_char(to_date(p_current_date_hour, 'yyyymmddhh24') - 7, 'yyyymmdd'));
      campaign.pkg_generic_ods.drop_hourly_partition@etladq(l_part_name);
    end if;
    l_process_name := p_process_name || '-' ||
                      'fact_adq_all_hourly@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_current_date_hour, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'I');
      -- no delete & truncate because we need to append the hourly data into this table
      insert /*+ append */
      into fact_adq_all_hourly@etladq
        (date_id, advertiser_id, order_id, ad_id,
         creative_id, affiliate_id, dma_id, country_id,
         state_province, ga_city, total_impressions,
         total_clicks, ad_group_id, source_id, network_name,
         vertical, primary_channel, country_code, flags,
         ad_dimension, ga_os_id, ga_browser_id, tile,
         tag_type, atf_value, zip_code, region_date_coc,
         region_code, coc, date_hour, time_stamp)
        select /*+ parallel(a,4,1) */
         l_date, advertiser_id, order_id, ad_id, creative_id,
         affiliate_id, dma_id, country_id, state_province,
         ga_city, total_impressions, total_clicks,
         ad_group_id, source_id, network_name, vertical,
         primary_channel, country_code, flags, ad_dimension,
         ga_os_id, ga_browser_id, tile, tag_type, atf_value,
         zip_code, region_date_coc, region_code, coc, l_hour,
         time_stamp
        from   rt_imp_clk a
        where  date_id = p_current_date_hour;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'UC', l_cnt);
    end if;
    -- copy the cumulative agg data in the hourly agg table
    l_process_name := p_process_name || '-' ||
                      'fact_adq_all_hourly_agg@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_current_date_hour, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'I');
      campaign.pkg_generic_ods.truncate_partition@etladq(l_date, 'campaign', 'fact_adq_all_hourly_agg');
      insert /*+ append */
      into fact_adq_all_hourly_agg@etladq
        (date_id, advertiser_id, order_id, ad_id,
         creative_id, affiliate_id, dma_id, country_id,
         state_province, ga_city, total_impressions,
         total_clicks, ad_group_id, source_id, network_name,
         vertical, primary_channel, country_code, flags,
         ad_dimension, ga_os_id, ga_browser_id, tile,
         tag_type, atf_value, zip_code, region_date_coc,
         region_code, coc, time_stamp)
        select /*+ parallel(a,4,1) */
         l_date, advertiser_id, order_id, ad_id, creative_id,
         affiliate_id, dma_id, country_id, state_province,
         ga_city, total_impressions, total_clicks,
         ad_group_id, source_id, network_name, vertical,
         primary_channel, country_code, flags, ad_dimension,
         ga_os_id, ga_browser_id, tile, tag_type, atf_value,
         zip_code, region_date_coc, region_code, coc,
         time_stamp
        from   rt_imp_clk_agg a
        where  date_id = p_current_date_hour;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_lam_ads_gpo
  (
    p_run_id            number,
    p_current_date_hour number,
    p_process_name      varchar2
  ) as
    l_part_name    number;
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'lam_ads_gpo';
    if pkg_log_process.is_not_complete(p_run_id, p_current_date_hour, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'I');
      l_part_name := to_char(to_date(p_current_date_hour, 'yyyymmddhh24') -
                             12 / 24, 'yyyymmddhh24');
      for x in (select table_owner, table_name,
                       partition_name
                from   dba_tab_partitions
                where  table_name in ('LAM_ADS_GPO_STG')
                and    replace(partition_name, 'P_', '') <
                       l_part_name
                order  by table_name, partition_position) loop
        begin
          execute immediate '
            alter table ' ||
                            x.table_name || '
            drop partition ' ||
                            x.partition_name;
        exception
          when others then
            null;
        end;
      end loop;
      execute immediate 'alter table lam_ads_gpo_stg truncate partition p_' ||
                        p_current_date_hour;
      insert /*+ append */
      into lam_ads_gpo_stg
        (date_id, affiliate_id, key, ctr, ad_id,
         impressions, clicks, key_id)
        select /*+ parallel(a,5,1) index(b IDX_ADM_ADS_ADID)*/
         p_current_date_hour, affiliate_id,
         a.ad_id || '-12h' as key,
         case
           when sum(total_impressions) = 0 then
            0
           else
            round(sum(total_clicks) * 100 /
                  sum(total_impressions), 5)
         end as ctr, a.ad_id, sum(total_impressions),
         sum(total_clicks), 'HOURWISE' as key_id
        from   rt_imp_clk a, ods_metadata.adm_ads b
        where  date_id = p_current_date_hour
        and    a.ad_id = b.ad_id
        and    coalesce(affiliate_id, 0) <> 0
        and    ctr_optimization_enabled = 1
        group  by a.ad_id, affiliate_id
        having sum(total_impressions) > 100;
      commit;
      if p_current_date_hour >=
         to_number(to_char(sysdate, 'yyyymmddhh24')) then
        execute immediate 'alter table lam_ads_gpo truncate partition P_HOURLY';
        insert /*+ append */
        into lam_ads_gpo
          (affiliate_id, key, ctr, ad_id, impressions,
           clicks, key_id)
          select /*+ parallel(a,5,1) */
           affiliate_id, key,
           case
             when sum(impressions) = 0 then
              0
             else
              round(sum(clicks) * 100 / sum(impressions), 5)
           end as ctr, ad_id, sum(impressions), sum(clicks),
           key_id
          from   lam_ads_gpo_stg a
          group  by affiliate_id, key, ad_id, key_id
          having sum(impressions) > 100;
        commit;
        delete from adm_data.lam_ads_gpo@etlgad
        where  key_id = 'HOURWISE';
        insert into adm_data.lam_ads_gpo@etlgad
          (affiliate_id, key, ctr, ad_id, impressions,
           clicks, key_id)
          select /*+ parallel(a,4,1) */
           affiliate_id, key, ctr, ad_id, impressions,
           clicks, key_id
          from   lam_ads_gpo a
          where  key_id = 'HOURWISE';
      end if;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'UC', l_cnt);
    end if;
  end;

end;
/
