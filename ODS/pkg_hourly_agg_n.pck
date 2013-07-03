create or replace package pkg_hourly_agg_n as

  procedure run_agg;

  procedure drop_partitions(l_process_date number);

  procedure add_partitions;

  procedure load_imp_clk
  (
    p_run_id            number,
    p_utc_date          number,
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
create or replace package body pkg_hourly_agg_n as

  procedure run_agg as
    l_run_id       number;
    l_cnt          number;
    l_sys_est_date number;
    l_process_name varchar2(100) := 'Process';
    l_select_part  varchar2(30);
    l_error_msg    varchar2(4000);
  begin
    --l_run_id := seq_daily_campaign_run.nextval;
    -- extract the EST date hour for processing from system date
    -- sysdate(pst)+3/24(est time diff) -1/24(lagging time for processing) = sysdate +1/24
    l_sys_est_date := to_number(to_char(sysdate + 2 / 24, 'yyyymmddhh24'));
    --l_run_id       := 187;
    if to_char(sysdate, 'hh24') = '00' then
      -- do the partition maintanance
      drop_partitions(to_number(to_char(sysdate - 7, 'yyyymmddhh24')));
      -- delete 7 days old hourly_process_log records from the table
      delete from hourly_process_log_n
      where  run_id = 2244
      and    utc_date <=
             to_number(to_char(sysdate - 7, 'yyyymmdd'));
      --select the maximum partition above which needs to be addded in hourly process log table
      select to_char(to_date(max(utc_date), 'yyyymmdd'), 'yyyymmdd')
      into   l_select_part
      from   hourly_process_log_n;
      --please add the table name condition while selecting the count ( give all table_names) 
      if l_select_part is null then
        select count(1)
        into   l_cnt
        from   dba_tab_partitions
        where  table_name = 'GA_RAW_IMPS'
        and    table_owner = 'INTRA_DATA';
        --this is to ensure that we must have value in l_select_part when it is null
        --and it is taken as -1 sothat we can add the minimum entry in the hourly_process_log table
        select min(replace(partition_name, 'P_', '')) - 1
        into   l_select_part
        from   dba_tab_partitions
        where  table_name = 'GA_RAW_IMPS'
        and    table_owner = 'INTRA_DATA';
      else
        select count(1)
        into   l_cnt
        from   dba_tab_partitions
        where  table_name = 'GA_RAW_IMPS'
        and    table_owner = 'INTRA_DATA'
        and    replace(partition_name, 'P_', '') >
               to_char(l_select_part);
      end if;
      if l_cnt > 0 then
        insert into hourly_process_log_n
          (run_id, utc_date, est_date, current_date_hour,
           previous_date_hour, status)
          with src as
           (select replace(partition_name, 'P_', '') as utc_date,
                   to_number(substr(subpartition_name, instr(subpartition_name, '_', 1, 2) + 1)) as est_date_hour
            from   dba_tab_subpartitions
            where  table_name = 'GA_RAW_IMPS'
            and    replace(partition_name, 'P_', '') >
                   to_char(l_select_part)
            and    subpartition_name not like '%DEFAULT'
            and    table_owner = 'INTRA_DATA'
            order  by 1, 2)
          select 2244, utc_date,
                 to_number(to_char(to_date(est_date_hour, 'yyyymmddhh24'), 'yyyymmdd')) as est_date,
                 est_date_hour as current_date_hour,
                 case
                   when to_number(to_char(to_date(est_date_hour, 'yyyymmddhh24'), 'hh24')) = 0 then
                    est_date_hour
                   else
                    to_number(to_char(to_date(est_date_hour, 'yyyymmddhh24') -
                                      1 / 24, 'yyyymmddhh24'))
                 end as previous_date_hour,
                 'SCHEDULED' as status
          from   src;
        commit;
        add_partitions;
      end if;
    end if;
    for i in (select utc_date, est_date, current_date_hour,
                     previous_date_hour, status
              from   hourly_process_log_n
              where  run_id = 2244
                    --and    current_date_hour > 2013011519
                    --and    current_date_hour <= 2013011601
              and    status <> 'COMPLETE'
              order  by current_date_hour) loop
      begin
        -- we need to do minus -1 hour processing, so if current_date_hour = l_sys_est_date then we have to do exit
        if i.current_date_hour = l_sys_est_date then
          exit;
        end if;
        -- mark the start time 
        update hourly_process_log_n
        set    start_time = sysdate
        where  current_date_hour = i.current_date_hour;
        select max(run_id)
        into   l_run_id
        from   daily_process_log
        where  to_char(to_date(process_date, 'yyyymmddhh24'), 'yyyymmdd') =
               to_char(to_date(i.current_date_hour, 'yyyymmddhh24'), 'yyyymmdd')
        and    process_name like '%rt_imp_clk'
        and    run_id <> 2245;
        if l_run_id is null then
          l_run_id := seq_daily_campaign_run.nextval;
          pa_send_email1.mail('odsimportreport@glam.com', 'nileshm@glam.com,sahilt@glam.com', 'pkg_hourly_agg_new', 'pkg_hourly_agg_new has started for EST date: ' ||
                               i.current_date_hour ||
                               ' With run_id:' ||
                               l_run_id);
        end if;
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
        load_imp_clk(l_run_id, i.utc_date, i.current_date_hour, l_process_name);
        load_imp_clk_agg(l_run_id, i.current_date_hour, l_process_name, i.previous_date_hour);
        load_hourly_agg(l_run_id, i.current_date_hour, l_process_name);
        load_lam_ads_gpo(l_run_id, i.current_date_hour, l_process_name);
        -- mark the process as complete
        update hourly_process_log_n
        set    status = 'COMPLETE', end_time = sysdate
        where  current_date_hour = i.current_date_hour;
        commit;
      exception
        when others then
          l_error_msg := dbms_utility.format_error_backtrace ||
                         dbms_utility.format_error_stack;
          -- mark the process as fail
          update hourly_process_log_n
          set    status = 'FAIL', end_time = sysdate,
                 error_reason = l_error_msg
          where  current_date_hour = i.current_date_hour;
          commit;
          pkg_log_process.log_process(l_run_id, i.current_date_hour, '', 'UF', 0, l_error_msg);
          pa_send_email1.mail('odsimportreport@glam.com', 'nileshm@glam.com,sahilt@glam.com', 'pkg_hourly_agg_new', 'pkg_hourly_agg_new has been failed.' ||
                               chr(10) ||
                               sqlerrm);
          -- we need to exit on failuare becoz we are aggregating the data using previous data and current data 
          exit;
      end;
    end loop;
  end;

  procedure drop_partitions(l_process_date number) as
  begin
    for x in (select table_owner, table_name, partition_name
              from   dba_tab_partitions
              where  table_name in
                     ('RT_IMP_CLK_N', 'RT_IMP_CLK_AGG_N')
              and    replace(partition_name, 'P_', '') <
                     l_process_date
              order  by table_name, partition_position) loop
      begin
        execute immediate 'alter table ' || x.table_name ||
                          ' drop partition ' ||
                          x.partition_name;
      exception
        when others then
          null;
      end;
    end loop;
  end;

  procedure add_partitions as
  begin
    for x in (select table_name, tablespace_name,
                     max(to_number(replace(partition_name, 'P_', ''))) as last_partition
              from   dba_tab_partitions
              where  table_name in
                     ('RT_IMP_CLK_N', 'RT_IMP_CLK_AGG_N', 'LAM_ADS_GPO_STG_N')
              group  by table_name, tablespace_name) loop
      for i in (select current_date_hour
                from   hourly_process_log_n
                where  current_date_hour > x.last_partition
                order  by current_date_hour) loop
        execute immediate 'alter table ' || x.table_name ||
                          ' add partition p_' ||
                          i.current_date_hour ||
                          ' values (' ||
                          i.current_date_hour || ')';
      end loop;
    end loop;
    --- 
    for x in (select table_name, tablespace_name,
                     max(to_number(replace(partition_name, 'P_', ''))) || '23' as last_partition
              from   dba_tab_partitions
              where  table_name in ('RT_ALL_HOURLY_AGG')
              group  by table_name, tablespace_name) loop
      for i in (select distinct to_char(to_date(current_date_hour, 'yyyymmddhh24'), 'yyyymmdd') as current_date_hour,
                                to_char(to_date(current_date_hour, 'yyyymmddhh24') + 1, 'yyyymmdd') as next_date_hour
                from   hourly_process_log_n
                where  current_date_hour > x.last_partition
                order  by 1) loop
        execute immediate 'alter table ' || x.table_name ||
                          ' add partition p_' ||
                          i.current_date_hour ||
                          ' values less than (' ||
                          i.next_date_hour || ')';
      end loop;
    end loop;
  end;

  procedure load_imp_clk
  (
    p_run_id            number,
    p_utc_date          number,
    p_current_date_hour number,
    p_process_name      varchar2
  ) as
    l_process_name  varchar2(100);
    l_cnt           number := 0;
    l_def_us_offset number;
    l_hour          number;
  begin
    l_def_us_offset := ((trunc(sys_extract_utc(systimestamp), 'hh24') -
                       trunc(sysdate + 3 / 24, 'hh24')) * 24);
    l_process_name  := p_process_name || '-' ||
                       'rt_imp_clk';
    if pkg_log_process.is_not_complete(p_run_id, p_current_date_hour, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'I');
      --analyze raw table partition before processing
      execute immediate 'analyze table intra_data.ga_raw_imps subpartition (P_' ||
                        p_utc_date || '_' ||
                        p_current_date_hour ||
                        ') estimate statistics';
      execute immediate 'Alter table rt_imp_clk_n truncate partition P_' ||
                        p_current_date_hour;
      insert /*+ append */
      into rt_imp_clk_n
        (date_hour, date_id, advertiser_id, order_id, ad_id,
         total_clicks, date_id_coc, coc, country_code,
         affiliate_id, total_impressions)
      -- intra_data.fact_media_stage_new
        select /*+ parallel(a,4,1) */
         p_current_date_hour as date_hour,
         to_number(to_char(to_date(est_date_hour, 'yyyymmddhh24'), 'yyyymmdd')) as date_id,
         coalesce(ord.advertiser_id, 0),
         coalesce(ord.order_id, 0), a.ad_id,
         sum(clicks) as total_clicks,
         case
           when c.affiliate_id is null then
            to_number(to_char(trunc(a.time_stamp -
                                    l_def_us_offset / 24, 'dd'), 'yyyymmdd'))
           else
            to_number(to_char(trunc(a.time_stamp +
                                    coalesce(coc_offset, 0) / 24, 'dd'), 'yyyymmdd'))
         end date_id_coc,
         case
           when c.affiliate_id is null then
            'NO_COC'
           else
            c.country
         end coc, country_code, a.affiliate_id,
         sum(impressions) as total_impressions
        from   intra_data.ga_raw_imps a,
               channel_aggregate_primary c,
               ods_metadata.adm_ads ord
        where  a.est_date_hour = p_current_date_hour
        and    a.ad_id = ord.ad_id(+)
        and    a.affiliate_id = c.affiliate_id(+)
        group  by to_number(to_char(to_date(est_date_hour, 'yyyymmddhh24'), 'yyyymmdd')),
                  coalesce(ord.advertiser_id, 0),
                  coalesce(ord.order_id, 0), a.ad_id,
                  case
                    when c.affiliate_id is null then
                     to_number(to_char(trunc(a.time_stamp -
                                             l_def_us_offset / 24, 'dd'), 'yyyymmdd'))
                    else
                     to_number(to_char(trunc(a.time_stamp +
                                             coalesce(coc_offset, 0) / 24, 'dd'), 'yyyymmdd'))
                  end,
                  case
                    when c.affiliate_id is null then
                     'NO_COC'
                    else
                     c.country
                  end, country_code, a.affiliate_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table rt_imp_clk_n partition (P_' ||
                        p_current_date_hour ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'UC', l_cnt);
    end if;
    l_hour := case
                when l_def_us_offset = 4 then
                 19
                when l_def_us_offset = 5 then
                 18
              end;
    if l_hour = substr(p_current_date_hour, 9, 10) or
       substr(p_current_date_hour, 9, 10) = 23 then
      l_process_name := p_process_name || '-' ||
                        'rt_imp_clk' || '-default_subpart';
      if pkg_log_process.is_not_complete(p_run_id, p_utc_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_utc_date, l_process_name, 'I');
        --analyze raw table partition before processing
        execute immediate 'analyze table intra_data.ga_raw_imps subpartition (P_' ||
                          p_utc_date || '_DEFAULT' ||
                          ') estimate statistics';
        --no truncate partition because we are inserting the records from the default partition
        execute immediate '
        insert /*+ append */
        into rt_imp_clk_n
          (date_hour, date_id, advertiser_id, order_id,
           ad_id, total_clicks, date_id_coc, coc,
           country_code, affiliate_id, total_impressions)
        -- intra_data.fact_media_stage_new
          select /*+ parallel(a,4,1) */
           :p_current_date_hour as date_hour,
           to_number(to_char(to_date(actual_est_date, ''yyyymmddhh24''), ''yyyymmdd'')) as date_id,
           coalesce(ord.advertiser_id, 0),
           coalesce(ord.order_id, 0), a.ad_id,
           sum(a.clicks) as total_clicks,
           case
             when c.affiliate_id is null then
              to_number(to_char(trunc(a.time_stamp -' ||
                          l_def_us_offset ||
                          ' / 24, ''dd''), ''yyyymmdd''))
             else
              to_number(to_char(trunc(a.time_stamp +
                                      coalesce(coc_offset, 0) / 24, ''dd''), ''yyyymmdd''))
           end date_id_coc,
           case
             when c.affiliate_id is null then
              ''NO_COC''
             else
              c.country
           end coc, country_code, a.affiliate_id,
           sum(impressions) as total_impressions
          from   intra_data.ga_raw_imps subpartition(P_' ||
                          p_utc_date ||
                          '_DEFAULT) a,
                 channel_aggregate_primary c, ods_metadata.adm_ads ord
          where  to_char(time_stamp - ' ||
                          l_def_us_offset ||
                          ' / 25, ''yyyymmdd'') =
                 to_char(to_date(:p_current_date_hour, ''yyyymmddhh24''),''yyyymmdd'')
          and    a.ad_id = ord.ad_id(+)
          and    a.affiliate_id = c.affiliate_id(+)
          group  by to_number(to_char(to_date(actual_est_date, ''yyyymmddhh24''), ''yyyymmdd'')),
                    coalesce(ord.advertiser_id, 0),
                    coalesce(ord.order_id, 0), a.ad_id,
                    case
                      when c.affiliate_id is null then
                       to_number(to_char(trunc(a.time_stamp - ' ||
                          l_def_us_offset ||
                          ' / 24, ''dd''), ''yyyymmdd''))
                      else
                       to_number(to_char(trunc(a.time_stamp +
                                               coalesce(coc_offset, 0) / 24, ''dd''), ''yyyymmdd''))
                    end,
                    case
                      when c.affiliate_id is null then
                       ''NO_COC''
                      else
                       c.country
                    end, country_code, a.affiliate_id'
          using p_current_date_hour, p_current_date_hour;
        l_cnt := sql%rowcount;
        commit;
        execute immediate 'analyze table rt_imp_clk_n partition (P_' ||
                          p_current_date_hour ||
                          ') estimate statistics';
        pkg_log_process.log_process(p_run_id, p_utc_date, l_process_name, 'UC', l_cnt);
      end if;
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
      execute immediate 'Alter table rt_imp_clk_agg_n truncate partition P_' ||
                        p_current_date_hour;
      insert /*+ append */
      into rt_imp_clk_agg_n
        (date_hour, date_id, advertiser_id, order_id, ad_id,
         total_clicks, date_id_coc, coc, country_code,
         affiliate_id, total_impressions)
        select p_current_date_hour as date_hour, date_id,
               advertiser_id, order_id, ad_id,
               sum(total_clicks), date_id_coc, coc,
               country_code, affiliate_id,
               sum(total_impressions)
        from   (select /*+ parallel(a,4,1) */
                  date_id, advertiser_id, order_id, ad_id,
                  total_clicks, date_id_coc, coc, country_code,
                  affiliate_id, total_impressions
                 from   rt_imp_clk_agg_n a
                 where  date_hour = p_previous_date_hour
                 union all
                 select /*+ parallel(b,4,1) */
                  date_id, advertiser_id, order_id, ad_id,
                  total_clicks, date_id_coc, coc, country_code,
                  affiliate_id, total_impressions
                 from   rt_imp_clk_n b
                 where  date_hour = p_current_date_hour)
        group  by date_id, advertiser_id, order_id, ad_id,
                  date_id_coc, coc, country_code,
                  affiliate_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table rt_imp_clk_agg_n partition (P_' ||
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
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_date         number;
    l_hour         number;
    l_part_name    number;
  begin
    -- copy the cumulative agg data in the hourly agg table
    l_process_name := p_process_name || '-' ||
                      'rt_all_hourly_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_current_date_hour, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'I');
      execute immediate 'alter table rt_all_hourly_agg truncate partition P_' ||
                        to_char(to_date(p_current_date_hour, 'yyyymmddhh24'), 'yyyymmdd');
      insert /*+ append */
      into rt_all_hourly_agg
        (date_id, advertiser_id, order_id, ad_id,
         total_clicks, date_id_coc, coc, country_code,
         affiliate_id, total_impressions)
        select /*+ parallel(a,4,1) */
         date_id, advertiser_id, order_id, ad_id,
         total_clicks, date_id_coc, coc, country_code,
         affiliate_id, total_impressions
        from   rt_imp_clk_agg_n a
        where  date_hour = p_current_date_hour;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table rt_all_hourly_agg partition (P_' ||
                        to_char(to_date(p_current_date_hour, 'yyyymmddhh24'), 'yyyymmdd') ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_current_date_hour, l_process_name, 'UC', l_cnt);
    end if;
    ------hourly data on ADQ
    l_date := to_number(to_char(to_date(p_current_date_hour, 'yyyymmddhh24'), 'yyyymmdd'));
    l_hour := to_number(to_char(to_date(p_current_date_hour, 'yyyymmddhh24'), 'hh24'));
    if l_hour = 0 then
      l_part_name := to_number(to_char(to_date(p_current_date_hour, 'yyyymmddhh24') - 5, 'yyyymmdd'));
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
         country_id, total_impressions, total_clicks,
         ad_group_id, source_id, region_date_coc, coc,
         country_code, affiliate_id, date_hour)
        select /*+ parallel(a,4,1) */
         date_id, advertiser_id, order_id, ad_id, country_id,
         total_impressions, total_clicks, ad_group_id,
         source_id, date_id_coc, coc, country_code,
         affiliate_id, substr(date_hour, 9, 10)
        from   rt_imp_clk_n a
        where  date_hour = p_current_date_hour;
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
         country_id, total_impressions, total_clicks,
         ad_group_id, source_id, region_date_coc, coc,
         country_code, affiliate_id)
        select /*+ parallel(a,4,1) */
         date_id, advertiser_id, order_id, ad_id, country_id,
         total_impressions, total_clicks, ad_group_id,
         source_id, date_id_coc, coc, country_code,
         affiliate_id
        from   rt_imp_clk_agg_n a
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
                where  table_name in ('LAM_ADS_GPO_STG_N')
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
      execute immediate 'alter table lam_ads_gpo_stg_n truncate partition p_' ||
                        p_current_date_hour;
      insert /*+ append */
      into lam_ads_gpo_stg_n
        (date_id, affiliate_id, key, ctr, ad_id,
         impressions, clicks, key_id)
        select /*+ parallel(a,5,1) index(b IDX_ADM_ADS_ADID)*/
         date_hour, affiliate_id, a.ad_id || '-12h' as key,
         case
           when sum(total_impressions) = 0 then
            0
           else
            round(sum(total_clicks) * 100 /
                  sum(total_impressions), 5)
         end as ctr, a.ad_id, sum(total_impressions),
         sum(total_clicks), 'HOURWISE' as key_id
        from   rt_imp_clk_n a, ods_metadata.adm_ads b
        where  date_hour = p_current_date_hour
        and    a.ad_id = b.ad_id
        and    coalesce(affiliate_id, 0) <> 0
        and    ctr_optimization_enabled = 1
        group  by date_hour, a.ad_id, affiliate_id
        having sum(total_impressions) > 100;
      commit;
      execute immediate 'analyze table lam_ads_gpo_stg_n partition (P_' ||
                        p_current_date_hour ||
                        ') estimate statistics';
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
          from   lam_ads_gpo_stg_n a
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
