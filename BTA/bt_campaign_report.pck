create or replace package bt_data.bt_campaign_report as

  /******************************************************************************
     NAME:       bt_campaign_report
     PURPOSE:    To automate the process to generate the Bt_Campaign_Report
  
     REVISIONS:
     Ver        Date        Author           Description
     ---------  ----------  ---------------  ------------------------------------
     1.0       10/01/2010   Nilesh Mahajan   1. Created this package.
  ******************************************************************************/
  procedure run_campaign(p_bt_report_id number);

  procedure populate_raw_data(p_bt_report_id number);

  procedure load_generic_data(p_bt_report_id number);

  function get_country_info(p_country varchar2)
    return varchar2;

  procedure report_2x2(p_bt_report_id number);

  procedure reschedule_report(p_bt_report_id number);

  procedure purge_report_data
  (
    p_bt_report_id number,
    p_is_debug     number
  );

  procedure cancel_report_request
  (
    p_bt_report_id number,
    p_machine_name varchar2
  );

end bt_campaign_report;
/
create or replace package body bt_data.bt_campaign_report as

  /******************************************************************************
     NAME:       bt_campaign_report
     PURPOSE:    To automate the process to generate the Bt_Campaign_Report
  
     REVISIONS:
     Ver        Date        Author           Description
     ---------  ----------  ---------------  ------------------------------------
     1.0       10/01/2010   Nilesh Mahajan   1. Created this package.
  ******************************************************************************/
  procedure run_campaign(p_bt_report_id number) is
    l_report_status varchar2(10);
    l_error_msg     varchar2(4000);
    l_run_count     number;
    l_report_type   number;
    l_queue_max     number;
    queue_full exception;
    l_is_debug number := 0;
  begin
    -- LOAD UP MAX QUEUE LENGTH (CONFIGURABLE)
    select max(queue_max)
    into   l_queue_max
    from   bt_report_queue_max;
    -- LOAD UP CURRENT SET OF RUNNING REPORTS
    select count(1)
    into   l_run_count
    from   bt_report_schedule
    where  report_status = 1;
    -- START NEW PROCESS IF THERE IS ROOM IN THE RUNNING QUEUE
    if l_run_count < l_queue_max then
      -- do everything
      select report_status, bt_report_type, is_debug
      into   l_report_status, l_report_type, l_is_debug
      from   bt_report_schedule
      where  bt_report_id = p_bt_report_id;
      if l_report_status = '0' then
        -- set the report status as Running
        update bt_report_schedule
        set    report_status = 1, start_time = sysdate
        where  bt_report_id = p_bt_report_id;
        commit;
        --populate raw and score table data
        populate_raw_data(p_bt_report_id);
        -- 1 = Ad/Pixel  2 = Ad And Pixel 3 = Reach And Frequency
        load_generic_data(p_bt_report_id);
        -- 4 = 2X2 report
        if l_report_type = 4 then
          report_2x2(p_bt_report_id);
        end if;
        -- set the report status as completed.
        update bt_report_schedule
        set    report_status = 2, end_time = sysdate
        where  bt_report_id = p_bt_report_id;
        commit;
      end if;
      purge_report_data(p_bt_report_id, 0);
    else
      raise queue_full;
    end if;
  exception
    when queue_full then
      raise_application_error(-20667, 'bt_report_schedule queue is full. Please schedule ' ||
                               p_bt_report_id ||
                               ' later.');
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      update bt_report_schedule
      set    report_status = 4, error_reason = l_error_msg,
             end_time = sysdate
      where  bt_report_id = p_bt_report_id;
      purge_report_data(p_bt_report_id, 1);
      commit;
  end run_campaign;

  procedure populate_raw_data(p_bt_report_id number) as
    l_ad_ids       varchar2(4000);
    l_start_date   number;
    l_end_date     number;
    l_country_code varchar2(10);
    l_country_id   varchar2(100);
    l_report_type  number;
    l_pixel_ids    varchar2(4000);
  begin
    -- get the required parameters
    select sch.ad_ids, sch.start_date, sch.end_date,
           get_country_info(sch.country_code),
           sch.country_code, sch.bt_report_type,
           sch.pixel_ids
    into   l_ad_ids, l_start_date, l_end_date, l_country_id,
           l_country_code, l_report_type, l_pixel_ids
    from   bt_report_schedule sch
    where  sch.bt_report_id = p_bt_report_id;
    -- Populate raw table data
    if l_report_type <> 3 then
      execute immediate '
      create table bt_data.bt_report_raw_table_' ||
                        p_bt_report_id ||
                        ' compress for all operations tablespace tbl_monthly_process 
                        parallel(degree 5 instances 1) nologging
          as       
          select /*+ PARALLEL (a 3,1) */
                 date_id, user_id, ad_id, impressions, clicks
          from   bt_data.monthly_campaign_raw_new a
          where  date_id >= ' ||
                        l_start_date || '
          and    date_id <= ' ||
                        l_end_date || '
          and    ad_id in (' ||
                        l_ad_ids || ')
          and    country_code in (''' ||
                        l_country_code || ''')';
    else
      execute immediate '
      create table bt_data.bt_report_raw_table_' ||
                        p_bt_report_id ||
                        ' compress for all operations tablespace tbl_monthly_process 
                        parallel(degree 3 instances 1) nologging
          as       
          select /*+ PARALLEL (a 3,1) */
                 date_id, user_id, impressions, clicks,
                 b.ad_size_id, b.order_id, b.ad_id
          from   bt_data.monthly_campaign_raw_new a,
                 adm_ads b
          where  date_id >= ' ||
                        l_start_date || '
          and    date_id <= ' ||
                        l_end_date || '
          and    b.ad_id in (' ||
                        l_ad_ids || ')
          and    country_code in (''' ||
                        l_country_code || ''')
          and    a.ad_id = b.ad_id';
      --- total impressions and total clicks from adq
      execute immediate '
        create table bt_data.bt_report_imps_' ||
                        p_bt_report_id ||
                        ' compress for all operations tablespace tbl_monthly_process 
                          parallel(degree 3 instances 1) nologging
            as       
            select b.ad_id, b.ad_size_id, b.order_id,
                   sum(total_impressions) as total_impressions,
                   sum(total_clicks) as total_clicks
            from   campaign.fact_adq_ad_country_agg@roadq a, 
                   adm_ads b
            where  date_id >= ' ||
                        l_start_date || '
            and    date_id <= ' ||
                        l_end_date || '
            and    a.ad_id in (' ||
                        l_ad_ids || ')
            and    country_id in (' ||
                        l_country_id || ')
            and    a.ad_id = b.ad_id
            group  by b.ad_id, b.ad_size_id, b.order_id';
    end if;
    -- GENERATE USER SCORES
    execute immediate '
      create table bt_data.bt_report_score_table_' ||
                      p_bt_report_id ||
                      ' compress for all operations tablespace tbl_monthly_process 
                        parallel(degree 3 instances 1) nologging
          as  
          select /*+ PARALLEL (a 3,1) */
               user_id, bt_category_id, sum(weighted_page_visits) as bt_score
          from   bt_data.fact_bt_category_visits_new a
          where  date_id >= ' ||
                      l_start_date || '
          and    date_id <= ' ||
                      l_end_date || '
          and    user_id in
                 (select /*+ parallel (b 3) */
                   distinct user_id
                   from bt_data.bt_report_raw_table_' ||
                      p_bt_report_id ||
                      ' b )
          group  by user_id, bt_category_id';
    -- populate pixel raw table only for Ad And Pixel report (2)
    if l_report_type in (2) then
      -- populate pixel raw table
      execute immediate '
        create table bt_data.bt_report_raw_pixel_' ||
                        p_bt_report_id ||
                        ' compress for all operations tablespace tbl_monthly_process 
                        parallel(degree 3 instances 1) nologging
          as
          select /*+ PARALLEL (a 3,1) */
                 date_id, user_id, ad_id, impressions, clicks
          from   bt_data.monthly_campaign_raw_new a
          where  date_id >= ' ||
                        l_start_date || '
          and    date_id <= ' ||
                        l_end_date || '
          and    ad_id in (' ||
                        l_pixel_ids || ')
          and    country_code in (''' ||
                        l_country_code || ''')';
      -- populate pixel score table
      execute immediate '
        create table bt_data.bt_report_score_pixel_' ||
                        p_bt_report_id ||
                        ' compress for all operations tablespace tbl_monthly_process 
                        parallel(degree 3 instances 1) nologging
          as  
          select /*+ PARALLEL (a 3,1) */
                 user_id, bt_category_id, sum(weighted_page_visits) as bt_score
          from   bt_data.fact_bt_category_visits_new a
          where  date_id >= ' ||
                        l_start_date || '
          and    date_id <= ' ||
                        l_end_date || '
          and    user_id in
                 (select /*+ parallel (b 3,1) */
                   distinct user_id
                   from bt_data.bt_report_raw_pixel_' ||
                        p_bt_report_id ||
                        ' b )
          group  by user_id, bt_category_id';
      --copy user_id's into comparision table by comparing with pixel raw table
      execute immediate '
        create table bt_data.bt_report_raw_compare_' ||
                        p_bt_report_id ||
                        ' compress for all operations tablespace tbl_monthly_process 
                        parallel(degree 3 instances 1) nologging
          as
          select /*+ PARALLEL (a 3,1) */
                 date_id, user_id, ad_id, impressions, clicks
          from   bt_data.bt_report_raw_table_' ||
                        p_bt_report_id || ' a
          where  user_id in
                 (select /*+ parallel (b 3,1) */
                   distinct user_id
                   from   bt_data.bt_report_raw_pixel_' ||
                        p_bt_report_id || ' b )';
      --copy user_id's into comparision table by comparing with pixel score table
      execute immediate '
        create table bt_data.bt_report_score_compare_' ||
                        p_bt_report_id ||
                        ' compress for all operations tablespace tbl_monthly_process 
                        parallel(degree 3 instances 1) nologging
          as
          select /*+ PARALLEL (a 3,1) */
                 user_id, bt_category_id, bt_score
          from   bt_data.bt_report_score_table_' ||
                        p_bt_report_id || ' a
          where  user_id in
                 (select /*+ parallel (b 3,1) */
                   distinct user_id
                   from   bt_data.bt_report_score_pixel_' ||
                        p_bt_report_id || ' b )';
    end if;
    if l_report_type in (99) then
        null;
    end if;
    commit;
  end populate_raw_data;

  procedure load_generic_data(p_bt_report_id number) as
    l_raw_table    varchar2(100);
    l_score_table  varchar2(100);
    l_country_code varchar2(10);
    l_start_date   number;
    l_report_type  number;
  begin
    select start_date, bt_report_type, country_code
    into   l_start_date, l_report_type, l_country_code
    from   bt_report_schedule
    where  bt_report_id = p_bt_report_id;
    l_raw_table := case
                     when l_report_type in (1, 3, 4) then
                      'bt_report_raw_table_' || p_bt_report_id
                     when l_report_type = 2 then
                      'bt_report_raw_compare_' ||
                      p_bt_report_id
                   end;
    l_score_table := case
                       when l_report_type in (1, 3, 4) then
                        'bt_report_score_table_' ||
                        p_bt_report_id
                       when l_report_type = 2 then
                        'bt_report_score_compare_' ||
                        p_bt_report_id
                     end;
    -- load agg ic data
    execute immediate 'insert into bt_data.bt_report_agg_ic_table 
            (bt_report_id,date_id,bt_category_id,bt_category,uq_users,impressions,
             clicks,ctr,avg_score,bt_report_type,country_code) 
        select :p_bt_report_id, :l_start_date, a.bt_category_id, 
                b.bt_category, uq_users, impressions, clicks, ctr, 
                avg_score, :l_report_type, :l_country_code
        from   (select /*+ ORDERED PARALLEL( BUSS 3 ) PARALLEL( PUSR 3 ) */
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
                from   bt_data.' ||
                      l_raw_table ||
                      ' pusr,
                       bt_data.' ||
                      l_score_table ||
                      ' buss
                where  buss.user_id = pusr.user_id
                group  by buss.bt_category_id
                order  by buss.bt_category_id) a,
               bt_taxonomy b
        where  a.bt_category_id = b.bt_category_id(+)
        order  by a.bt_category_id'
      using p_bt_report_id, l_start_date, l_report_type, l_country_code;
    commit;
    --if l_report_type in (1, 2, 4) then
      -- load segmentwise data
      for segment_list in (select segmentation,
                                  count(1) as i_cat_count
                           from   bt_data.bt_segmentation_taxonomy
                           where  country = l_country_code
                           group  by segmentation
                           order  by segmentation) loop
        -- UPDATE AGGREGATES
        execute immediate 'insert into bt_data.bt_report_agg_table
          (bt_report_id,segment,uq_users,impressions,clicks,visits,ctr,avg_impressions,
           avg_clicks,avg_visits,avg_score,bt_report_type) 
         with bt_report_score as
           (select /*+ PARALLEL( FBST 3 ) */
             user_id, :segment as segment, sum(bt_score) segment_sum
            from   bt_data.' ||
                          l_score_table ||
                          ' fbst
            where fbst.bt_category_id in
                   (select bt_category_id
                    from   bt_segmentation_taxonomy
                    where  segmentation = :segment)
            group  by user_id),
          final_data as
           (select /*+ ORDERED PARALLEL( BUSS 3 ) PARALLEL( PUSR 3 ) */
             segment, buss.user_id, sum(impressions) as impressions,
             sum(clicks) as clicks, count(distinct(date_id)) as visits,
             sum(segment_sum) bt_score
            from   bt_report_score buss,
                   bt_data.' ||
                          l_raw_table ||
                          ' pusr
            where  buss.user_id = pusr.user_id
            group  by buss.user_id, buss.segment)
          select /*+ PARALLEL( BUSC 3 ) */
            :p_bt_report_id, segment, count(distinct(user_id)) uq_users,
           sum(impressions) impressions, sum(clicks) clicks, sum(visits) visits,
           case when sum(impressions) = 0 then
            0
           else  
            ((sum(clicks) / sum(impressions)) * 100)
           end as ctr,
           avg(impressions) avg_impressions,
           avg(clicks) avg_clicks, avg(visits) avg_visits,
           avg(bt_score) avg_score, :l_report_type
          from   final_data busc
          group  by segment'
          using segment_list.segmentation, segment_list.segmentation, p_bt_report_id, l_report_type;
        commit;
      end loop;
    --end if;
    -- NEW REPORT TYPE ID TO BE DETERMINED FOR BT VIDEO ENGAGEMENT DATA
    if l_report_type = 99 then
      -- TODO: ALL 3 OF THESE ITEMS NEED TO HAVE A BT_REPORT VERSION OF THEIR TABLE CREATED - BB 11/18
      --          POSSIBLY WE SHOULD MAKE SURE THAT THERE IS A DELETE STATEMENT BASED ON BT_REPORT_ID
      --          FOR ALL 3 OF THESE ITEMS AS WELL IN THE EVENT OF SOME REPROCESS
      -- CALCULATE ENGAGEMENT SEGMENTWISE AGGREGATE DATA
      for segment_list in (select segmentation,
                                  count(1) as i_cat_count
                           from   bt_data.bt_segmentation_taxonomy
                           where  upper(country) =
                                  l_country_code
                           and    display_type = 'VIDEO'
                           and    segmentation like 'VD%'
                           group  by segmentation
                           order  by segmentation) loop
        execute immediate '
            insert into BT_DATA.CAMPAIGN_ENG_TMP_SEGMENTS
              (date_id, bt_report_id, country, segment, uq_users, video25, video50, video75, videoend,
               visits, avg_visits, avg_score)
              with segment_score as
               (select /*+ PARALLEL( FBST 2 ) */
                 user_id, :segment as segment, sum(bt_score) segment_sum
                from   BT_DATA.TMP_ENG_SCORE_CAMPAIGN fbst
                where  fbst.bt_category_id in
                           (select bt_category_id
                            from   bt_segmentation_taxonomy
                            where  segmentation = :segment
                            and    country = :l_country_code
                            and    display_type = ''VIDEO'')
                group  by user_id),
              final_data as
               (select /*+ ORDERED PARALLEL( BUSS 2 ) PARALLEL( PUSR 2 ) */
                 buss.segment, buss.user_id,
                 SUM(video25) video25, SUM(video50) video50, SUM(video75) video75, SUM(videoend) videoend,
                 0 as visits,
                 sum(segment_sum) bt_score
                from   segment_score buss,
                       BT_DATA.TMP_ENG_RAW_CAMPAIGN pusr
                where  buss.user_id = pusr.user_id
                group  by buss.user_id, buss.segment
                having sum(videoend) < 10000)
              select /*+ PARALLEL( BUSC 2 ) */
               :l_start_date as date_id, :p_bt_report_id, :l_country_code as country,
               segment, count(distinct(user_id)) uq_users,
               SUM(video25) video25, SUM(video50) video50, SUM(video75) video75, SUM(videoend) videoend,
               0 visits,
               0 avg_visits, avg(bt_score) avg_score
              from   final_data
              group  by segment'
          using segment_list.segmentation, segment_list.segmentation, l_country_code, l_start_date, p_bt_report_id, l_country_code;
        commit;
      end loop;
      -- CALCULATE ENGAGMENT IC AGGREGATE DATA
      execute immediate '
          insert into BT_DATA.CAMPAIGN_ENG_TMP_IC
            (start_date_id, ad_ids, bt_category_id, bt_category, uq_users,
             video25, video50, video75, videoend, avg_score, time_stamp,
             country_code)
            select :l_start_date as start_date_id, :p_bt_report_id as ad_ids, a.bt_category_id,
                   b.bt_category, uq_users, video25, video50, video75, videoend,
                   avg_score, sysdate as time_stamp,
                   :l_country_code as country_code
            from   (select /*+ ORDERED PARALLEL( BUSS 2 ) PARALLEL( PUSR 2 ) */
                      trim(buss.bt_category_id) bt_category_id,
                      count(distinct(pusr.user_id)) uq_users, SUM(video25) video25, SUM(video50) video50, SUM(video75) video75, SUM(videoend) videoend,
                      avg(buss.bt_score) avg_score
                     from   BT_DATA.TMP_ENG_RAW_CAMPAIGN pusr,
                            BT_DATA.TMP_ENG_SCORE_CAMPAIGN buss
                     where  buss.user_id = pusr.user_id
                     group  by trim(buss.bt_category_id)
                     ) a,
                   bt_data.bt_taxonomy b
            where  a.bt_category_id = b.bt_category_id(+)
            order  by a.bt_category_id '
        using l_start_date, p_bt_report_id, l_country_code;
      commit;
      -- CALCULATE ENGAGMENT SUMMARY AGGREGATE DATA
      execute immediate '
            INSERT INTO BT_DATA.CAMPAIGN_ENG_TMP_SUMMARY
                (date_id, ad_ids, unique_users, video25, video50, video75, videoend, time_stamp, country_code)
            SELECT /*+ parallel (pusr 3) */
                :l_start_date, :p_bt_report_id, count(distinct(user_id)) unique_users, SUM(video25), SUM(video50), SUM(video75), SUM(videoend),
                sysdate as time_stamp, :l_country_code
              FROM   BT_DATA.TMP_ENG_RAW_CAMPAIGN pusr'
        using l_start_date, p_bt_report_id, l_country_code;
      commit;
    end if;
    --load report summary data
    if l_report_type <> 3 then
      execute immediate 'insert into bt_data.bt_report_summary
              (bt_report_id,uq_users,impressions,clicks,ctr,bt_report_type)  
              select /*+ parallel( a 3 ) */ 
               :p_bt_report_id, count(distinct(user_id)) uq_users,
               sum(impressions) impressions, sum(clicks) clicks,
               case when sum(impressions) = 0 then
                   0
                 else  
                 ((sum(clicks) / sum(impressions)) * 100)
                 end as ctr, :l_report_type
              from   bt_data.' ||
                        l_raw_table || ' a'
        using p_bt_report_id, l_report_type;
    else
      execute immediate 'insert into bt_data.bt_report_summary
              (bt_report_id,uq_users,impressions,clicks,ctr,bt_report_type,
               order_id, ad_id, ad_size_id)  
              with src as
                 (select /*+ parallel( a 3 ) */
                   count(distinct(user_id)) uq_users, order_id,
                   ad_id, ad_size_id
                  from   bt_data.' ||
                        l_raw_table || ' a
                  group  by order_id, ad_id, ad_size_id)  
              select /*+ parallel( a 3 ) */ 
               :p_bt_report_id, max(uq_users) uq_users,
               sum(total_impressions) impressions, sum(total_clicks) clicks,
               null as ctr, :l_report_type, a.order_id,
               a.ad_id, a.ad_size_id
              from   src a,
                     bt_data.bt_report_imps_' ||
                        p_bt_report_id || ' b
              where a.order_id = b.order_id
              and   a.ad_id = b.ad_id
              group by a.order_id, a.ad_id, a.ad_size_id'
        using p_bt_report_id, l_report_type;
      execute immediate 'insert into bt_data.bt_report_summary
              (bt_report_id,uq_users,impressions,clicks,ctr,bt_report_type,order_id)
              with src as
                 (select /*+ parallel( a 3 ) */
                   count(distinct(user_id)) uq_users, order_id
                  from   bt_data.' ||
                        l_raw_table || ' a
                  group  by order_id)  
              select /*+ parallel( a 3 ) */ 
               :p_bt_report_id, max(uq_users) uq_users,
               sum(total_impressions) impressions, sum(total_clicks) clicks,
               null as ctr, :l_report_type, a.order_id
              from   src a,
                     bt_data.bt_report_imps_' ||
                        p_bt_report_id || ' b
              where a.order_id = b.order_id 
              group by a.order_id'
        using p_bt_report_id, l_report_type;
    end if;
    if l_report_type = 1 then
      execute immediate '
        update bt_report_summary s
        set    (scored_uniques, scored_impressions) =
                (select count(distinct(user_id)) scored_uniques,
                        sum(impressions) socred_impressions
                 from   bt_data.bt_report_raw_table_' ||
                        p_bt_report_id || '
                 where  user_id in
                        (select distinct user_id
                          from   bt_data.bt_report_score_table_' ||
                        p_bt_report_id || '))
        where  s.bt_report_id = :p_bt_report_id'
        using p_bt_report_id;
    elsif l_report_type = 2 then
      execute immediate '    
        update bt_report_summary
        set    pixel_impressions =
                (select sum(impressions)
                 from   bt_report_raw_pixel_' ||
                        p_bt_report_id || '),
               ad_impressions =
                (select sum(impressions)
                 from   bt_report_raw_table_' ||
                        p_bt_report_id || ')
        where  bt_report_id = :p_bt_report_id'
        using p_bt_report_id;
      commit;
    end if;
  end load_generic_data;

  procedure reschedule_report(p_bt_report_id number) is
  begin
    insert into bt_report_schedule
      (bt_report_id, report_name, advertiser_name, ad_ids,
       start_date, end_date, country_code, email_id,
       request_date, report_status, bt_report_type,
       pixel_ids)
      select bt_report_sequence.nextval, report_name,
             advertiser_name, ad_ids, start_date, end_date,
             country_code, email_id, sysdate,
             0 as report_status, bt_report_type, pixel_ids
      from   bt_report_schedule
      where  bt_report_id = p_bt_report_id;
    update bt_report_schedule
    set    report_status = 7
    where  bt_report_id = p_bt_report_id;
    commit;
  end;

  procedure report_2x2(p_bt_report_id number) is
    l_month_start number;
    l_cnt         number;
    l_country     varchar2(100);
  begin
    select to_number(to_char(trunc(to_date(end_date, 'yyyymmdd'), 'mm'), 'yyyymmdd')),
           country_code
    into   l_month_start, l_country
    from   bt_report_schedule
    where  bt_report_id = p_bt_report_id;
    select count(1)
    into   l_cnt
    from   monthly_glam_summary
    where  date_id = l_month_start
    and    country = l_country;
    if l_cnt = 0 then
      l_month_start := to_number(to_char(add_months(to_date(l_month_start, 'yyyymmdd'), -1), 'yyyymmdd'));
    end if;
    insert into bt_report_minmax_agg_index
      (bt_report_id, min_final_index, max_final_index)
      with src as
       (select a.segment, a.ctr, b.ctr as total_ctr
        from   monthly_glam_seg_agg a, monthly_glam_summary b
        where  a.date_id = b.date_id
        and    b.date_id = l_month_start
        and    b.ctr != 0
        and    a.country = l_country
        and    b.country = l_country)
      select p_bt_report_id,
             min(final_index) min_final_index,
             max(final_index) max_final_index
      from   (select a.bt_report_id,
                      (a.ctr /
                       ((b.clicks / b.impressions) * 100)) /
                       (c.ctr / c.total_ctr) final_index
               from   bt_data.bt_report_agg_table a,
                      bt_data.bt_report_summary b, src c
               where  a.bt_report_id = p_bt_report_id
               and    a.bt_report_id = b.bt_report_id
               and    a.segment = c.segment
               and    b.clicks != 0
               and    b.impressions != 0
               and    a.segment in
                      ('Movie Addict', 'Shopaholic', 'Beauty Junkie', 'Decor Diva', 'Fashionista', 'Haute Hostess', 'Living Well', 'Modern Moms', 'Luxe Life', 'TV Obsessed', 'Foodie', 'Quiz Fanatic', 'Health Conscious'));
    insert into bt_report_2x2
      (bt_report_id, segment, uq_users, impressions, clicks,
       ctr, final_index, normalized_index)
      with src as
       (select a.segment, a.ctr, b.ctr as total_ctr
        from   monthly_glam_seg_agg a, monthly_glam_summary b
        where  a.date_id = b.date_id
        and    b.date_id = l_month_start
        and    b.ctr != 0
        and    a.country = l_country
        and    b.country = l_country)
      select a.bt_report_id, a.segment, a.uq_users,
             a.impressions, a.clicks, a.ctr,
             (a.ctr / ((b.clicks / b.impressions) * 100)) /
              (c.ctr / c.total_ctr) * 100 final_index,
             case
               when (a.ctr /
                    ((b.clicks / b.impressions) * 100)) /
                    (c.ctr / c.total_ctr) > 1 then
                (((a.ctr /
                ((b.clicks / b.impressions) * 100)) /
                (c.ctr / c.total_ctr)) - 1) /
                (e.max_final_index - 1) * 400 + 500
               else
                (((a.ctr /
                ((b.clicks / b.impressions) * 100)) /
                (c.ctr / c.total_ctr)) - e.min_final_index) /
                (1 - e.min_final_index) * 400 + 100
             end normalized_index
      from   bt_report_agg_table a, bt_report_summary b,
             src c, bt_report_minmax_agg_index e
      where  a.bt_report_id = p_bt_report_id
      and    a.bt_report_id = b.bt_report_id
      and    b.impressions != 0
      and    b.clicks != 0
      and    a.segment = c.segment
      and    a.bt_report_id = e.bt_report_id;
    commit;
  end;

  function get_country_info(p_country varchar2)
    return varchar2 is
    l_country_id varchar2(55);
  begin
    if upper(p_country) = 'UK' then
      l_country_id := '285,286,287,68';
    elsif upper(p_country) = 'US' then
      l_country_id := '256,257';
    else
      select to_char(country_id)
      into   l_country_id
      from   dart_data.dart_countries@roben
      where  country_abbrev = upper(p_country);
      --and    is_targeted = 1;
    end if;
    return l_country_id;
  end;

  procedure purge_report_data
  (
    p_bt_report_id number,
    p_is_debug     number
  ) as
    l_report_status number;
    l_report_type   number;
  begin
    if p_is_debug = 0 then
      select bt_report_type, report_status
      into   l_report_type, l_report_status
      from   bt_report_schedule
      where  bt_report_id = p_bt_report_id;
      -- if report fail then don't drop raw tables in order to do analysis
      if l_report_status in (2, 6) then
        -- drop the raw tables
        begin
          execute immediate '
          drop table bt_data.bt_report_raw_table_' ||
                            p_bt_report_id || ' purge ';
        exception
          when others then
            null;
        end;
        begin
          execute immediate '
          drop table bt_data.bt_report_score_table_' ||
                            p_bt_report_id || ' purge ';
        exception
          when others then
            null;
        end;
        begin
          execute immediate '
            drop table bt_data.bt_report_imps_' ||
                            p_bt_report_id || ' purge ';
        exception
          when others then
            null;
        end;
        if l_report_type = 2 then
          begin
            execute immediate '
            drop table bt_data.bt_report_raw_pixel_' ||
                              p_bt_report_id || ' purge ';
          exception
            when others then
              null;
          end;
          begin
            execute immediate '
            drop table bt_data.bt_report_score_pixel_' ||
                              p_bt_report_id || ' purge ';
          exception
            when others then
              null;
          end;
          begin
            execute immediate '
            drop table bt_data.bt_report_raw_compare_' ||
                              p_bt_report_id || ' purge ';
          exception
            when others then
              null;
          end;
          begin
            execute immediate '
            drop table bt_data.bt_report_score_compare_' ||
                              p_bt_report_id || ' purge ';
          exception
            when others then
              null;
          end;
        end if;
        if l_report_type = 99 then
          begin
            execute immediate 'DROP TABLE BT_DATA.BT_REPORT_RAW_ENGMT_' ||
                              p_bt_report_id || ' PURGE';
          exception
            when others then
              null;
          end;
          begin
            execute immediate 'DROP TABLE BT_DATA.BT_REPORT_SCORE_ENGMT_' ||
                              p_bt_report_id || ' PURGE';
          exception
            when others then
              null;
          end;
        end if;
        -- if report status is FAIL or CANCEL then purge the data from final tables also
        if l_report_status in (4, 6) then
          delete from bt_report_agg_ic_table
          where  bt_report_id = p_bt_report_id;
          delete from bt_report_agg_table
          where  bt_report_id = p_bt_report_id;
          delete from bt_report_summary
          where  bt_report_id = p_bt_report_id;
          delete from bt_report_2x2
          where  bt_report_id = p_bt_report_id;
          delete from bt_report_minmax_agg_index
          where  bt_report_id = p_bt_report_id;
        end if;
      end if;
      commit;
    end if;
  end;

  procedure cancel_report_request
  (
    p_bt_report_id number,
    p_machine_name varchar2
  ) as
    sqlstmt        varchar2(1000);
    l_report_match varchar2(55);
  begin
    l_report_match := 'select' || p_bt_report_id || ',';
    --dbms_output.put_line(l_report_match);
    update bt_report_schedule
    set    report_status = 6
    where  bt_report_id = p_bt_report_id;
    commit;
    for x in (select s.inst_id inst_id, s.sid sid,
                     s.serial# serial, s.machine, q.sql_text
              from   gv$session s, gv$sqlarea q
              where  s.sql_hash_value = q.hash_value(+)
              and    s.sql_address = q.address(+)
              and    instr(lower(replace(q.sql_text, ' ', '')), l_report_match) > 0
                    --and    instr(lower(q.sql_text), 'fact_media_user_day') > 0
              and    upper(s.username) = 'BT_DATA'
              --and    s.machine = p_machine_name
              ) loop
      sqlstmt := 'ALTER SYSTEM KILL SESSION ' || '''' ||
                 x.sid || ',' || x.serial || ',@' ||
                 x.inst_id || '''';
      --dbms_output.put_line(sqlstmt);
      execute immediate sqlstmt;
    end loop;
    purge_report_data(p_bt_report_id, 1);
  exception
    when others then
      update bt_report_schedule
      set    report_status = 6
      where  bt_report_id = p_bt_report_id;
      purge_report_data(p_bt_report_id, 1);
      commit;
  end cancel_report_request;

end bt_campaign_report;
/
