create or replace package pkg_monthly_seg_analysis as

  /******************************************************************************
     NAME:       Load_Segment
     PURPOSE:
  
     REVISIONS:
     Ver        Date        Author             Description
     ---------  ----------  ---------------   ------------------------------------
     1.0        01/14/2011   Nilesh Mahajan    Created this package.
  ******************************************************************************/
  procedure run_full_analysis
  (
    p_process_name  varchar2,
    p_start_date_id number,
    p_analysis_type varchar2
  );

  procedure populate_order_list
  (
    p_run_id        number,
    p_start_date_id number,
    p_end_date_id   number,
    p_analysis_type varchar2
  );

  procedure populate_score_table
  (
    p_run_id        number,
    p_start_date_id number,
    p_end_date_id   number
  );

  procedure populate_raw_data
  (
    p_start_date_id      number,
    p_start_no           number,
    p_end_no             number,
    p_parallel_thread_no number
  );

  procedure clear_tables_and_data
  (
    p_order_id           number,
    p_start_date_id      number,
    p_end_date_id        number,
    p_country            varchar2,
    p_parallel_thread_no number
  );

  procedure load_generic_data
  (
    p_ad_ids             varchar2,
    p_order_name         varchar2,
    p_analysis_type      varchar2,
    p_order_id           number,
    p_start_date_id      number,
    p_end_date_id        number,
    p_country            varchar2,
    p_month_start        number,
    p_parallel_thread_no number
  );

  procedure stg_monthly_userid
  (
    p_run_id        number,
    p_start_date_id number
  );

  procedure load_monthly_agg_data
  (
    p_run_id        number,
    p_start_date_id number
  );

  procedure copy_data_to_ods
  (
    p_run_id        number,
    p_start_date_id number
  );

  procedure compose_sendemailmessage
  (
    p_run_id       number,
    p_stage        varchar2,
    p_process_date number,
    p_message      varchar2,
    p_process_name varchar2
  );

end pkg_monthly_seg_analysis;
/
create or replace package body pkg_monthly_seg_analysis as

  /******************************************************************************
     name:       load_segment
     purpose:
  
     revisions:
     ver        date        author             description
     ---------  ----------  ---------------   ------------------------------------
     1.0        01/14/2011   nilesh mahajan            created this package.
  ******************************************************************************/
  procedure run_full_analysis
  (
    p_process_name  varchar2,
    p_start_date_id number,
    p_analysis_type varchar2
  ) is
    l_month_start number;
    l_month_end   number;
    l_run_id      number;
    l_error_msg   varchar2(4000);
  begin
    l_month_start := to_number(to_char(trunc(to_date(p_start_date_id, 'yyyymmdd'), 'mm'), 'yyyymmdd'));
    l_month_end   := to_number(to_char(last_day(to_date(p_start_date_id, 'yyyymmdd')), 'yyyymmdd'));
    if l_month_start = p_start_date_id then
      select max(run_id)
      into   l_run_id
      from   daily_process_log
      where  process_date = p_start_date_id
      and    upper(process_name) like
             upper(p_process_name) ||
             '-BT_MONTHLY_SEG_ANALYSIS%';
      if l_run_id is null then
        l_run_id := bt_data.seq_daily_bt_run.nextval;
      end if;
      -- send intimation mail for starting of process
      compose_sendemailmessage(l_run_id, 'START', p_start_date_id, 'Monthly BT Data Load', p_process_name);
      -- log starting of the process
      delete from daily_process_log
      where  process_date = p_start_date_id
      and    upper(process_name) like
             upper(p_process_name) ||
             '-BT_MONTHLY_SEG_ANALYSIS%'
      and    run_id = l_run_id
      and    status = 'FAIL';
      pkg_log_process.log_process(l_run_id, p_start_date_id, 'Process-bt_monthly_seg_analysis', 'I');
      --call procedure 
      populate_order_list(l_run_id, p_start_date_id, l_month_end, p_analysis_type);
      populate_score_table(l_run_id, p_start_date_id, l_month_end);
      pkg_log_process.log_process(l_run_id, p_start_date_id, 'Process-bt_monthly_seg_analysis', 'UC');
    else
      l_error_msg := ' start_date_id and end_date_id are not matched 
                       with real first day and last day of the month. 
                       Please check before processing. ';
      pkg_log_process.log_process(-1, p_start_date_id, l_error_msg, 'I');
      --send failuare mail
      compose_sendemailmessage(-1, 'FAIL', p_start_date_id, 'Monthly BT Data Load', 'PROCESS');
      l_error_msg := '';
    end if;
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, p_start_date_id, '', 'UF', 0, l_error_msg);
      --send failuare mail
      compose_sendemailmessage(l_run_id, 'FAIL', p_start_date_id, 'Monthly BT Data Load', 'PROCESS');
  end;

  procedure populate_order_list
  (
    p_run_id        number,
    p_start_date_id number,
    p_end_date_id   number,
    p_analysis_type varchar2
  ) is
    l_cnt             number := 0;
    l_campaign_imps   number;
    l_country_id      varchar2(1000);
    l_limit           number;
    l_parallel_thread number := 4;
    l_start_no        number := 0;
    l_end_no          number := 0;
    l_process_name    varchar2(100);
  begin
    l_process_name := 'Process-monthly_user_ids_truncate';
    if pkg_log_process.is_not_complete(p_run_id, p_start_date_id, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_start_date_id, l_process_name, 'I');
      -- truncate the monthly user id table before the start of the process
      execute immediate 'truncate table bt_data.monthly_user_ids ';
      pkg_log_process.log_process(p_run_id, p_start_date_id, l_process_name, 'UC');
    end if;
    l_process_name := 'Process-monthly_order_list';
    if pkg_log_process.is_not_complete(p_run_id, p_start_date_id, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_start_date_id, l_process_name, 'I');
      -- ensure that there are no records in monthly order list table for this date id
      delete from monthly_order_list
      where  month_start_date = p_start_date_id
      and    analysis_type = 'RON';
      commit;
      -- loop through each campaign during the timeframe
      for i in (select order_id orderid,
                       replace(order_name, '''', '') ordername,
                       to_number(to_char(min(start_date), 'yyyymmdd')) real_start_id,
                       to_number(to_char(max(end_date), 'yyyymmdd')) real_end_id,
                       case
                         when min(start_date) <
                              to_date(to_char(p_start_date_id), 'yyyymmdd') then
                          p_start_date_id
                         else
                          to_number(to_char(min(start_date), 'yyyymmdd'))
                       end start_date_id,
                       case
                         when max(end_date) >
                              to_date(to_char(p_end_date_id), 'yyyymmdd') then
                          p_end_date_id
                         else
                          to_number(to_char(max(end_date), 'yyyymmdd'))
                       end end_date_id,
                       wm_concat(a.ad_id) as ad_ids,
                       null as ad_names,
                       sum(b.impressions) total_ga_impressions,
                       sum(coalesce(b.impressions, 0)) total_impressions,
                       a.meta_country as country
                --from   bodata.network_ad_extended@benlink a,
                from   adm_ads a,
                       (select ad_id,
                                sum(impressions) impressions
                         from   adaptive_data.view_ga_ad_counts_detail@rosga
                         where  date_id >= p_start_date_id
                         and    date_id <= p_end_date_id
                         group  by ad_id) b
                where  a.start_date <=
                       to_date(to_char(p_end_date_id), 'yyyymmdd')
                and    a.end_date >=
                       to_date(to_char(p_start_date_id), 'yyyymmdd')
                and    ((upper(a.name) like '%RON%' and
                      upper(a.category) = 'ADVERTISER') or
                      meta_country = 'JP' and
                      (lower(targeting_key_value) like
                      '%jpsp=allsp%' or lower(targeting_key_value) like
                      '%jpglam=all%'))
                and    b.ad_id = a.ad_id(+)
                and    a.meta_country <> 'IH'
                group  by order_id,
                          replace(order_name, '''', ''),
                          a.meta_country
                having sum(coalesce(b.impressions, 0)) > 10000
                order  by a.meta_country) loop
        l_country_id := pkg_log_process.get_country_info(i.country);
        --calculate total impressions
        execute immediate '
            select coalesce(sum(total_impressions), 0)
            from   campaign.fact_adq_ad_country_agg@roadq
            where  date_id >= :p_start_date_id
            and    date_id <= :p_end_date_id
            and    country_id in (' ||
                          l_country_id || ')
            and    ad_id in (' ||
                          i.ad_ids || ')'
          into l_campaign_imps
          using i.start_date_id, i.end_date_id;
        if l_campaign_imps > 10000 then
          l_cnt := l_cnt + 1;
          insert into monthly_order_list
            (seq_no, orderid, ordername, real_start_id,
             real_end_id, start_date_id, end_date_id, ad_ids,
             ad_names, country, country_id, analysis_type,
             run_id, is_processed, fact_adq_imps,
             month_start_date)
          values
            (l_cnt, i.orderid, i.ordername, i.real_start_id,
             i.real_end_id, i.start_date_id, i.end_date_id,
             i.ad_ids, i.ad_names, i.country, l_country_id,
             p_analysis_type, p_run_id, 'N', l_campaign_imps,
             p_start_date_id);
        end if;
      end loop;
      commit;
      l_limit := round((l_cnt + 5) / l_parallel_thread);
      loop
        l_start_no := l_start_no + 1;
        l_end_no   := l_end_no + l_limit;
        update monthly_order_list
        set    parallel_thread_no = l_parallel_thread
        where  seq_no between l_start_no and l_end_no
        and    month_start_date = p_start_date_id
        and    run_id = p_run_id;
        l_parallel_thread := l_parallel_thread - 1;
        l_start_no        := l_end_no;
        exit when l_end_no >=(l_cnt + 5);
      end loop;
      commit;
      pkg_log_process.log_process(p_run_id, p_start_date_id, l_process_name, 'UC');
    end if;
  end;

  procedure populate_score_table
  (
    p_run_id        number,
    p_start_date_id number,
    p_end_date_id   number
  ) is
    l_drop_table   number;
    l_process_name varchar2(100);
  begin
    l_process_name := 'Process-monthly_score_table';
    if pkg_log_process.is_not_complete(p_run_id, p_start_date_id, l_process_name) then
      -- drop 2nd previous month's score table  
      begin
        l_drop_table := to_number(to_char(add_months(to_date(p_start_date_id, 'yyyymmdd'), -4), 'yyyymmdd'));
        execute immediate '
            drop table bt_data.monthly_score_' ||
                          l_drop_table || ' purge';
      exception
        when others then
          null;
      end;
      pkg_log_process.log_process(p_run_id, p_start_date_id, l_process_name, 'I');
      begin
        execute immediate '
            drop table bt_data.monthly_score_' ||
                          p_start_date_id || ' purge';
      exception
        when others then
          null;
      end;
      -- create the score table
      execute immediate '
          create table bt_data.monthly_score_' ||
                        p_start_date_id ||
                        ' compress for all operations tablespace tbl_monthly_process 
                            parallel(degree 5 instances 1) nologging as
            select /*+ parallel (a 5 1) */
                   user_id, bt_category_id, sum(weighted_page_visits) as bt_score
            from   bt_data.fact_bt_category_visits_new a
            where  date_id >= ' ||
                        p_start_date_id || '
            and    date_id <= ' ||
                        p_end_date_id || '
            group  by user_id, bt_category_id';
      -- ANALYZE SCORE TABLE
      execute immediate 'analyze table bt_data.monthly_score_' ||
                        p_start_date_id ||
                        ' estimate statistics';
      pkg_log_process.log_process(p_run_id, p_start_date_id, l_process_name, 'UC');
    end if;
  end;

  procedure populate_raw_data
  (
    p_start_date_id      number,
    p_start_no           number,
    p_end_no             number,
    p_parallel_thread_no number
  ) as
    l_error_msg varchar2(4000);
    l_end_no    number;
    l_cnt       number := 0;
    l_run_id    number;
  begin
    --calculate the end no for the processing order id's
    if p_start_no is not null then
      if p_end_no is null then
        select max(seq_no)
        into   l_end_no
        from   bt_data.monthly_order_list
        where  month_start_date = p_start_date_id
        and    analysis_type = 'RON';
      else
        l_end_no := p_end_no;
      end if;
    end if;
    for i in (select seq_no, orderid, ordername,
                     start_date_id, end_date_id,
                     trim(ad_ids) as ad_ids, country,
                     country_id, analysis_type, run_id,
                     month_start_date, parallel_thread_no
              from   bt_data.monthly_order_list
              where  month_start_date = p_start_date_id
              and    is_processed = 'N'
              and    analysis_type = 'RON'
              and    fact_adq_imps > 10000
              and    (parallel_thread_no =
                    p_parallel_thread_no or
                    seq_no between p_start_no and
                    l_end_no)) loop
      begin
        l_run_id := i.run_id;
        -- start actual process by making the log entry
        pkg_log_process.log_process(i.run_id, p_start_date_id, i.orderid, 'I');
        -- generate raw table table
        begin
          execute immediate ' drop table MONTHLY_RAW_TABLE_' ||
                            i.parallel_thread_no ||
                            ' purge';
        exception
          when others then
            null;
        end;
        execute immediate '
          create table bt_data.monthly_raw_table_' ||
                          i.parallel_thread_no ||
                          ' compress for all operations tablespace tbl_monthly_process 
                          parallel(degree 5 instances 1) nologging
          as select /*+ parallel( a,5,1) */ 
                    date_id, user_id, ad_id, impressions, clicks
             from   bt_data.monthly_campaign_raw_new a            
             where  date_id >= ' ||
                          i.start_date_id || '
             and    date_id <= ' ||
                          i.end_date_id || '
             and    ad_id in (' ||
                          i.ad_ids || ')
             and    country_code = ''' ||
                          i.country || '''';
        /*and    country_id in (' ||
        i.country_id || ')';*/
        /*from   bt_data.monthly_raw_data_' ||
        p_start_id || ' a*/
        /*and    country = ' ||
        i.country;*/
        -- generate user scores
        begin
          execute immediate ' drop table MONTHLY_TEMP_SCORE_' ||
                            i.parallel_thread_no ||
                            ' purge';
        exception
          when others then
            null;
        end;
        execute immediate '
          create table bt_data.monthly_temp_score_' ||
                          i.parallel_thread_no ||
                          ' compress for all operations tablespace tbl_monthly_process 
                          parallel(degree 5 instances 1) nologging 
          as select /*+ parallel (a,5,1) */
                    user_id, bt_category_id, bt_score
             from   bt_data.monthly_score_' ||
                          p_start_date_id || ' a
             where  user_id in
                     ( select /*+ parallel (b,3,1) */ distinct user_id
                       from   bt_data.monthly_raw_table_' ||
                          i.parallel_thread_no || ' b)';
        -- call procedure to populate the generic table data
        load_generic_data(i.ad_ids, i.ordername, i.analysis_type, i.orderid, i.start_date_id, i.end_date_id, i.country, i.month_start_date, i.parallel_thread_no);
        -- update monthly order list to set is_processed = 'Y'
        update monthly_order_list
        set    is_processed = 'Y'
        where  month_start_date = i.month_start_date
        and    orderid = i.orderid
        and    country = i.country;
        pkg_log_process.log_process(i.run_id, p_start_date_id, i.orderid, 'UC');
        commit;
      exception
        when others then
          -- update monthly order list to set is_processed = 'F'
          update monthly_order_list
          set    is_processed = 'F'
          where  month_start_date = i.month_start_date
          and    orderid = i.orderid
          and    country = i.country;
          l_error_msg := 'parallel thread=' ||
                         p_parallel_thread_no || '&' ||
                         dbms_utility.format_error_backtrace ||
                         dbms_utility.format_error_stack;
          pkg_log_process.log_process(i.run_id, p_start_date_id, i.orderid, 'UF', 0, l_error_msg);
      end;
    end loop;
    -- check for the failed order id's first
    select count(1)
    into   l_cnt
    from   monthly_order_list
    where  is_processed = 'F'
    and    month_start_date = p_start_date_id;
    --and    parallel_thread_no = i.parallel_thread_no;
    if l_cnt = 0 then
      --after the completion of the loop, call the procedure to aggregate the data
      select count(1)
      into   l_cnt
      from   monthly_order_list
      where  is_processed = 'N'
      and    month_start_date = p_start_date_id;
      if l_cnt = 0 then
        -- create staging table for monthly user ids table
        stg_monthly_userid(l_run_id, p_start_date_id);
        load_monthly_agg_data(l_run_id, p_start_date_id);
        -- call this procedure to copy the data to ODS database
        copy_data_to_ods(l_run_id, p_start_date_id);
        -- send completion mail
        compose_sendemailmessage(l_run_id, 'COMPLETE', p_start_date_id, 'Monthly BT Data Load', 'PROCESS');
      end if;
    else
      l_error_msg := l_cnt ||
                     ': order_id''s are failed while processing for the parallel thread no:' ||
                     p_parallel_thread_no;
      --send failuare mail
      /*compose_sendemailmessage(l_run_id, 'FAIL', p_start_date_id, 'Monthly BT Data Load' ||
                                l_error_msg, 'PROCESS');*/
    end if;
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, p_start_date_id, '', 'UF', 0, l_error_msg);
      --send failuare mail
      compose_sendemailmessage(l_run_id, 'FAIL', p_start_date_id, 'Monthly BT Data Load', 'PROCESS');
  end populate_raw_data;

  procedure clear_tables_and_data
  (
    p_order_id           number,
    p_start_date_id      number,
    p_end_date_id        number,
    p_country            varchar2,
    p_parallel_thread_no number
  ) is
    l_cnt number := 0;
  begin
    select count(1)
    into   l_cnt
    from   bt_data.monthly_icat_agg
    where  order_id = p_order_id
    and    start_date_id = p_start_date_id
    and    end_date_id = p_end_date_id
    and    country_code = p_country;
    if l_cnt > 0 then
      delete from bt_data.monthly_icat_agg
      where  order_id = p_order_id
      and    start_date_id = p_start_date_id
      and    end_date_id = p_end_date_id
      and    country_code = p_country;
      commit;
      l_cnt := 0;
    end if;
    select count(1)
    into   l_cnt
    from   bt_data.monthly_segment_agg
    where  order_id = p_order_id
    and    start_date_id = p_start_date_id
    and    end_date_id = p_end_date_id
    and    country_code = p_country;
    if l_cnt > 0 then
      delete from bt_data.monthly_segment_agg
      where  order_id = p_order_id
      and    start_date_id = p_start_date_id
      and    end_date_id = p_end_date_id
      and    country_code = p_country;
      commit;
      l_cnt := 0;
    end if;
    select count(1)
    into   l_cnt
    from   bt_data.monthly_summary
    where  order_id = p_order_id
    and    start_date_id = p_start_date_id
    and    end_date_id = p_end_date_id
    and    country_code = p_country;
    if l_cnt > 0 then
      delete from bt_data.monthly_summary
      where  order_id = p_order_id
      and    start_date_id = p_start_date_id
      and    end_date_id = p_end_date_id
      and    country_code = p_country;
      commit;
      l_cnt := 0;
    end if;
    select count(1)
    into   l_cnt
    from   bt_data.monthly_user_ids
    where  order_id = p_order_id
    and    country = p_country
    and    parallel_thread_no = p_parallel_thread_no;
    if l_cnt > 0 then
      delete from bt_data.monthly_user_ids
      where  order_id = p_order_id
      and    country = p_country
      and    parallel_thread_no = p_parallel_thread_no;
      commit;
    end if;
  end;

  procedure load_generic_data
  (
    p_ad_ids             varchar2,
    p_order_name         varchar2,
    p_analysis_type      varchar2,
    p_order_id           number,
    p_start_date_id      number,
    p_end_date_id        number,
    p_country            varchar2,
    p_month_start        number,
    p_parallel_thread_no number
  ) as
  begin
    -- delete the data from generic tables if already processed 
    clear_tables_and_data(p_order_id, p_start_date_id, p_end_date_id, p_country, p_parallel_thread_no);
    -- calculate icat aggregate data
    execute immediate 'insert into bt_data.monthly_icat_agg
        (start_date_id, end_date_id, order_id, ad_ids,
         order_name, analysis_type, bt_category_id,
         bt_category, uq_users, impressions, clicks, ctr,
         avg_score, time_stamp, country_code, month_start_date)
        select :p_start_date_id, :p_end_date_id, :p_order_id,
               :l_ad_ids, :p_order_name, :p_analysis_type,
               a.bt_category_id, b.bt_category, uq_users,
               impressions, clicks, ctr, avg_score,
               sysdate as time_stamp, :p_country, :l_month_start
        from   (select /*+ ordered parallel( buss 3 1) parallel( pusr 4 1 ) */
                  buss.bt_category_id,
                  count(distinct(pusr.user_id)) uq_users,
                  sum(pusr.impressions) impressions,
                  sum(pusr.clicks) clicks,
                  case when sum(impressions) = 0 then
                    0
                    else
                  ((sum(clicks) / sum(impressions)) * 100) 
                  end as ctr,
                  avg(buss.bt_score) avg_score
                 from   bt_data.monthly_raw_table_' ||
                      p_parallel_thread_no ||
                      ' pusr,
                        bt_data.monthly_temp_score_' ||
                      p_parallel_thread_no ||
                      ' buss
                 where  buss.user_id = pusr.user_id
                 group  by buss.bt_category_id
                 order  by buss.bt_category_id) a,
               bt_data.bt_taxonomy b
        where  a.bt_category_id = b.bt_category_id(+)
        order  by a.bt_category_id'
      using p_start_date_id, p_end_date_id, p_order_id, p_ad_ids, p_order_name, p_analysis_type, p_country, p_month_start;
    --calculate segmentwise aggregate data
    for segment_list in (select segmentation,
                                count(1) as i_cat_count
                         from   bt_data.bt_segmentation_taxonomy
                         where  upper(country) = p_country
                         and    display_type = 'DISPLAY'
                         group  by segmentation
                         order  by segmentation) loop
      execute immediate '                  
        insert into bt_data.monthly_segment_agg
          (order_id, ad_ids, order_name, analysis_type,
           start_date_id, end_date_id, segment_name, uq_users,
           impressions, clicks, ctr, avg_impressions, visits,
           avg_clicks, avg_visits, avg_score, time_stamp,
           country_code, month_start_date)
          with segment_score as
           (select /*+ parallel( fbst 4 1 ) */
             user_id, sum(bt_score) segment_sum
            from   bt_data.monthly_temp_score_' ||
                        p_parallel_thread_no ||
                        '  fbst
            where  fbst.bt_category_id in
                   (select bt_category_id
                     from   bt_data.bt_segmentation_taxonomy
                     where  segmentation = :segment
                     and    display_type = ''DISPLAY'')
            group  by user_id),
          final_data as
           (select /*+ ordered parallel( buss 4 1 ) parallel( pusr 4 1 ) */
             buss.user_id, sum(impressions) as impressions,
             sum(clicks) as clicks,
             count(distinct(date_id)) as visits,
             sum(segment_sum) bt_score
            from   segment_score buss,
                   bt_data.monthly_raw_table_' ||
                        p_parallel_thread_no ||
                        ' pusr
            where  buss.user_id = pusr.user_id
            group  by buss.user_id)
          select /*+ parallel( busc 2 3 ) */
           :p_order_id, :l_ad_ids, :p_order_name, :p_analysis_type,
           :p_start_date_id, :p_end_date_id, :segment,
           count(distinct(user_id)) uq_users,
           sum(impressions) impressions, sum(clicks) clicks,
           case when sum(impressions) = 0 then
                    0
           else
            ((sum(clicks) / sum(impressions)) * 100) 
           end as ctr,
           avg(impressions) avg_impressions,
           sum(visits) visits, avg(clicks) avg_clicks,
           avg(visits) avg_visits, avg(bt_score) avg_score,
           sysdate as time_stamp, :p_country, :l_month_start
          from   final_data busc'
        using segment_list.segmentation, p_order_id, p_ad_ids, p_order_name, p_analysis_type, p_start_date_id, p_end_date_id, segment_list.segmentation, p_country, p_month_start;
      commit;
    end loop;
    -- calculate summary totals
    execute immediate '
      insert into bt_data.monthly_summary
          (start_date_id, end_date_id, order_id, ad_ids,
           order_name, analysis_type, unique_users, impressions,
           clicks, time_stamp, country_code, month_start_date)
      select /*+ parallel (a 4 1) */
         :p_start_date_id, :p_end_date_id,
         :p_order_id, :l_ad_ids, :p_order_name, :p_analysis_type,
         count(distinct(user_id)) unique_users,
         sum(impressions) impressions, sum(clicks) clicks,
         sysdate as time_stamp, :p_country, :l_month_start
      from   bt_data.monthly_raw_table_' ||
                      p_parallel_thread_no || ' a'
      using p_start_date_id, p_end_date_id, p_order_id, p_ad_ids, p_order_name, p_analysis_type, p_country, p_month_start;
    commit;
    -- populate the monthly_user_ids
    execute immediate '
      insert into bt_data.monthly_user_ids
          (user_id, bt_category_id, country, order_id, parallel_thread_no)
      select /*+ parallel( a,4,1) */
           distinct user_id, bt_category_id, :p_country, :p_order_id, :parallel_thread_no
      from  bt_data.monthly_temp_score_' ||
                      p_parallel_thread_no || ' a'
      using p_country, p_order_id, p_parallel_thread_no;
    commit;
  end load_generic_data;

  procedure load_monthly_agg_data
  (
    p_run_id        number,
    p_start_date_id number
  ) is
  begin
    pkg_log_process.log_process(p_run_id, p_start_date_id, 'BTA_MONTHLY_AGG', 'I');
    -- delete the data if already processed
    delete from bt_data.monthly_glam_icat_agg
    where  date_id = p_start_date_id;
    delete from bt_data.monthly_glam_seg_agg
    where  date_id = p_start_date_id;
    delete from bt_data.monthly_glam_summary
    where  date_id = p_start_date_id;
    commit;
    for i in (select distinct country
              from   monthly_order_list
              where  month_start_date = p_start_date_id) loop
      -- category wise aggregate 
      execute immediate '
          insert into bt_data.monthly_glam_icat_agg
            (date_id, bt_category_id, bt_category, uq_users,
             impressions, clicks, ctr, avg_score, country)
            select month_start_date, a.bt_category_id,
                   bt_category, max(b.uq_users) as uq_users,
                   sum(impressions) as impressions,
                   sum(clicks) as clicks,
                   (sum(a.clicks) / sum(impressions)) * 100 as ctr,
                   avg(avg_score) as avg_score, country_code
            from   bt_data.monthly_icat_agg a, 
                   monthly_userid_category_temp b
            where  month_start_date = :p_start_date_id
            and    a.bt_category_id = b.bt_category_id
            and    a.country_code = b.country
            and    b.country = :country
            group  by month_start_date, a.bt_category_id,
                      bt_category, country_code'
        using p_start_date_id, i.country;
      commit;
      -- segment wise aggregate
      execute immediate '
          insert into bt_data.monthly_glam_seg_agg
            (date_id, segment, uq_users, impressions, clicks,
             ctr, avg_impressions, avg_visits, country)
            select month_start_date, segment_name,
                   max(b.uq_users) as uq_users,
                   sum(impressions) as impressions,
                   sum(clicks) as clicks,
                   (sum(clicks) / sum(impressions)) * 100 as ctr,
                   sum(impressions)/max(b.uq_users) avg_impressions,
                   sum(visits)/max(b.uq_users) as avg_visits, country_code
            from   bt_data.monthly_segment_agg a,
                   monthly_userid_segment_temp b
            where  month_start_date = :p_start_date_id
            and    a.segment_name = b.segment
            and    a.country_code = b.country
            and    b.country = :country
            group  by month_start_date, segment_name,
                      country_code'
        using p_start_date_id, i.country;
      commit;
    end loop;
    -- monthly summary
    execute immediate '
        insert into bt_data.monthly_glam_summary
          (date_id, uq_users, impressions, clicks, ctr,
           country)
          select month_start_date, max(b.uq_users) as uq_users,
                 sum(impressions) as impressions,
                 sum(clicks) as clicks,
                 (sum(clicks) / sum(impressions)) * 100 as ctr,
                 country_code
          from   bt_data.monthly_summary a,
                 monthly_userid_country_temp b
          where  month_start_date = :p_start_date_id
          and    a.country_code = b.country
          group  by month_start_date, country_code'
      using p_start_date_id;
    commit;
    pkg_log_process.log_process(p_run_id, p_start_date_id, 'BTA_MONTHLY_AGG', 'UC');
  end;

  procedure stg_monthly_userid
  (
    p_run_id        number,
    p_start_date_id number
  ) is
    l_last_part number;
    l_sql       varchar2(4000) := 'create table monthly_userid_stg tablespace tbl_monthly_process 
                                   parallel(degree 3 instances 2) nologging
                                   as ' ||
                                  chr(10);
  begin
    pkg_log_process.log_process(p_run_id, p_start_date_id, 'STG_MONTHLY_USERID', 'I');
    --drop the staging table needs to the drop the temp tables for monthly user ids
    begin
      execute immediate '
            drop table monthly_userid_stg purge';
    exception
      when others then
        null;
    end;
    select max(partition_position)
    into   l_last_part
    from   user_tab_partitions
    where  lower(table_name) = 'monthly_user_ids';
    --analyze the user id table
    execute immediate 'analyze table monthly_user_ids estimate statistics';
    for i in (select partition_position, partition_name
              from   user_tab_partitions
              where  lower(table_name) = 'monthly_user_ids'
              order  by partition_position) loop
      l_sql := l_sql ||
               'select /*+ parallel(b,4,1) */
                     bt_category_id, country,
                     user_id
              from   monthly_user_ids partition(' ||
               i.partition_name || ') b' || chr(10) || case
                 when i.partition_position = l_last_part then
                  ' '
                 else
                  ' union '
               end || chr(10);
    end loop;
    execute immediate l_sql;
    --analyze the staging user id table
    execute immediate 'analyze table monthly_userid_stg estimate statistics';
    -- category wise unique monthly user ids
    begin
      execute immediate '
            drop table monthly_userid_category_temp purge';
    exception
      when others then
        null;
    end;
    execute immediate '
      create table monthly_userid_category_temp
            compress
            tablespace tbl_monthly_process nologging
            parallel(degree 5 instances 1)
            partition by list(country)
            ( 
              partition p_ca values (''CA''),  
              partition p_de values (''DE''),  
              partition p_fr values (''FR''),  
              partition p_jp values (''JP''),  
              partition p_uk values (''UK''),  
              partition p_us values (''US''),  
              partition p_other values (default))  
        as
        select /*+ parallel(a,4,1) */
               bt_category_id, country, count(distinct user_id) as uq_users
        from   monthly_userid_stg a
        group by bt_category_id, country';
    --analyze the category user id table
    execute immediate 'analyze table monthly_userid_category_temp estimate statistics';
    -- segment wise unique monthly user ids  
    begin
      execute immediate '
            drop table monthly_userid_segment_temp purge';
    exception
      when others then
        null;
    end;
    execute immediate '   
      create table monthly_userid_segment_temp
            compress
            tablespace tbl_monthly_process nologging
            parallel(degree 5 instances 1)
            partition by list(country)
            ( 
              partition p_ca values (''CA''),  
              partition p_de values (''DE''),  
              partition p_fr values (''FR''),  
              partition p_jp values (''JP''),  
              partition p_uk values (''UK''),  
              partition p_us values (''US''),  
              partition p_other values (default))  
        as
        select /*+ parallel(a,4,1) */
               b.segmentation as segment,a.country,
               count(distinct user_id) as uq_users
        from   monthly_userid_stg a,
               bt_data.bt_segmentation_taxonomy b
        where  a.bt_category_id = b.bt_category_id
        and    a.country = b.country
        and    display_type = ''DISPLAY''
        group by b.segmentation, a.country';
    --analyze the segment user id table
    execute immediate 'analyze table monthly_userid_segment_temp estimate statistics';
    -- country wise unique monthly user ids
    begin
      execute immediate '
            drop table monthly_userid_country_temp purge';
    exception
      when others then
        null;
    end;
    execute immediate ' 
      create table monthly_userid_country_temp
            compress
            tablespace tbl_monthly_process nologging
            parallel(degree 5 instances 1) 
        as
        select /*+ parallel(a,4,1) */
               country, count(distinct user_id) as uq_users
        from monthly_userid_stg a
        group by country';
    --analyze the country user id table
    execute immediate 'analyze table monthly_userid_country_temp estimate statistics';
    pkg_log_process.log_process(p_run_id, p_start_date_id, 'STG_MONTHLY_USERID', 'UC');
  end;

  procedure copy_data_to_ods
  (
    p_run_id        number,
    p_start_date_id number
  ) is
  begin
    pkg_log_process.log_process(p_run_id, p_start_date_id, 'COPY_TO_ODS', 'I');
    -- monthly ICAT 
    delete from campaign_icat_aggregate@etlods
    where  to_number(to_char(trunc(to_date(start_date_id, 'yyyymmdd'), 'mm'), 'yyyymmdd')) =
           p_start_date_id;
    insert into campaign_icat_aggregate@etlods
      (start_date_id, end_date_id, order_id, ad_ids,
       order_name, analysis_type, bt_category_id,
       bt_category, uq_users, impressions, clicks, ctr,
       avg_score, time_stamp, country_code)
      select start_date_id, end_date_id, order_id, ad_ids,
             order_name, analysis_type, bt_category_id,
             bt_category, uq_users, impressions, clicks, ctr,
             avg_score, time_stamp, country_code
      from   monthly_icat_agg
      where  month_start_date = p_start_date_id
      and    country_code = 'US';
    -- monthly segment 
    delete from campaign_segment_aggregate@etlods
    where  to_number(to_char(trunc(to_date(start_date_id, 'yyyymmdd'), 'mm'), 'yyyymmdd')) =
           p_start_date_id;
    insert into campaign_segment_aggregate@etlods
      (order_id, ad_ids, order_name, analysis_type,
       start_date_id, end_date_id, segment_name, uq_users,
       impressions, clicks, ctr, avg_impressions, visits,
       avg_clicks, avg_visits, avg_score, time_stamp,
       country_code)
      select order_id, ad_ids, order_name, analysis_type,
             start_date_id, end_date_id, segment_name,
             uq_users, impressions, clicks, ctr,
             avg_impressions, visits, avg_clicks, avg_visits,
             avg_score, time_stamp, country_code
      from   monthly_segment_agg
      where  month_start_date = p_start_date_id
      and    country_code = 'US';
    -- monthly summary
    delete from campaign_summary@etlods
    where  to_number(to_char(trunc(to_date(start_date_id, 'yyyymmdd'), 'mm'), 'yyyymmdd')) =
           p_start_date_id;
    insert into campaign_summary@etlods
      (start_date_id, end_date_id, order_id, ad_ids,
       order_name, analysis_type, unique_users, impressions,
       clicks, time_stamp, country_code)
      select start_date_id, end_date_id, order_id, ad_ids,
             order_name, analysis_type, unique_users,
             impressions, clicks, time_stamp, country_code
      from   monthly_summary
      where  month_start_date = p_start_date_id
      and    country_code = 'US';
    -- segment wise aggregate
    delete from campaign.all_glam_seg_agg@etlods
    where  date_id = p_start_date_id;
    insert into campaign.all_glam_seg_agg@etlods
      (date_id, segment, uq_users, impressions, clicks, ctr,
       avg_impressions, avg_visits)
      select date_id, segment, uq_users, impressions, clicks,
             ctr, avg_impressions, avg_visits
      from   monthly_glam_seg_agg
      where  date_id = p_start_date_id
      and    country = 'US';
    -- summary aggregate
    delete from campaign.all_glam_summary@etlods
    where  date_id = p_start_date_id;
    insert into campaign.all_glam_summary@etlods
      (date_id, uq_users, total_impressions, total_clicks,
       total_ctr)
      select date_id, uq_users,
             impressions as total_impressions,
             clicks as total_clicks, ctr as total_ctr
      from   monthly_glam_summary
      where  date_id = p_start_date_id
      and    country = 'US';
    commit;
    pkg_log_process.log_process(p_run_id, p_start_date_id, 'COPY_TO_ODS', 'UC');
  end;

  procedure compose_sendemailmessage
  (
    p_run_id       number,
    p_stage        varchar2,
    p_process_date number,
    p_message      varchar2,
    p_process_name varchar2
  ) is
    l_sender     varchar2(100) := 'btimportreport@glam.com';
    l_to         varchar2(1000) := 'btimportreport@glam.com';
    l_subject    varchar2(1000) := '';
    l_message    varchar2(4000);
    l_today      varchar2(8);
    l_crlf       varchar2(2) := chr(13) || chr(10);
    l_hrs        number;
    l_minutes    number;
    l_sec        number;
    l_start_time date;
    l_end_time   date;
    l_complete   number;
  begin
    l_today := to_char(sysdate, 'yyyymmdd');
    select upper(sys_context('USERENV', 'DB_NAME')) || ': ' ||
            p_message || ' Date: ' || p_process_date || ' ' ||
            p_process_name || ' -> ETL RUN_ID : ' ||
            p_run_id
    into   l_subject
    from   dual;
    if (upper(p_stage) = 'START') then
      l_message := l_crlf || 'Hello ,' || l_crlf || l_crlf ||
                   p_message ||
                   ' load process has Started for ' ||
                   p_process_date || l_crlf ||
                   'Please restrain from running any reports and queries. ' ||
                   l_crlf ||
                   'You will be notified once the process has been completed.';
    elsif upper(p_stage) = 'COMPLETE' then
      select min(start_time), max(end_time)
      into   l_start_time, l_end_time
      from   daily_process_log
      where  run_id = p_run_id
      and    status = 'COMPLETE';
      select count(1)
      into   l_complete
      from   daily_process_log
      where  run_id = p_run_id
      and    status = 'COMPLETE';
      l_hrs     := trunc(((86400 *
                         (l_end_time - l_start_time)) / 60) / 60) -
                   24 *
                   (trunc((((86400 *
                          (l_end_time - l_start_time)) / 60) / 60) / 24));
      l_minutes := trunc((86400 *
                         (l_end_time - l_start_time)) / 60) -
                   60 *
                   (trunc(((86400 *
                          (l_end_time - l_start_time)) / 60) / 60));
      l_sec     := trunc(86400 *
                         (l_end_time - l_start_time)) -
                   60 *
                   (trunc((86400 *
                          (l_end_time - l_start_time)) / 60));
      l_message := l_crlf || 'Hello ,' || l_crlf || l_crlf ||
                   p_message || ' details are as follows: ' ||
                   l_crlf || l_crlf || 'Job Run ID: ' ||
                   p_run_id || l_crlf || 'Data Date : ' ||
                   p_process_date || l_crlf ||
                   'Job Started: ' ||
                   to_char(l_start_time, ' dd Mon YYYY hh24:mi:ss') ||
                   l_crlf || 'Job Ended  : ' ||
                   to_char(l_end_time, ' dd Mon YYYY hh24:mi:ss') ||
                   l_crlf ||
                   'Duration process time (In Hrs) : ' ||
                   l_hrs || l_crlf ||
                   'Duration process time (In Minutes) : ' ||
                   l_minutes || l_crlf ||
                   'Duration process time (In Seconds) : ' ||
                   l_sec || l_crlf ||
                   'Total Time (Hr:Min:Sec) :' || l_hrs || ':' ||
                   l_minutes || ':' || l_sec;
      l_message := l_message || l_crlf || l_crlf ||
                   'Steps Completed: ' || l_complete ||
                   l_crlf || 'All jobs ran successfully ' ||
                   l_crlf || l_crlf || '- Data Team';
      l_subject := l_subject || ' ,Successful on ' ||
                   l_today;
    elsif upper(p_stage) = 'FAIL' then
      l_message := l_crlf || 'Hello ,' || l_crlf || l_crlf ||
                   p_message || ' details are as follows: ' ||
                   l_crlf || l_crlf || 'Job Run ID : ' ||
                   p_run_id || l_crlf || 'Data Date : ' ||
                   p_process_date || l_crlf || l_crlf ||
                   ' Failed on : ' || l_today || l_crlf ||
                   l_crlf || '- Data Team';
      l_subject := l_subject || ' Failed on ' || l_today;
    end if;
    pa_send_email.mail(l_sender, l_to, l_subject, l_message);
  end;

end pkg_monthly_seg_analysis;
/
