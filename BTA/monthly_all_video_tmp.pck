create or replace package monthly_all_video_tmp as

  /******************************************************************************
     NAME:       monthly_all_audience
     PURPOSE:
  
     REVISIONS:
     Ver        Date        Author             Description
     ---------  ----------  ---------------   ------------------------------------
     1.0        02/01/2011   Nilesh Mahajan    Created this package.
  ******************************************************************************/
  procedure run_all_video_tmp(p_start_date_id number);


  procedure load_generic_data
  (
    p_start_date_id number,
    p_end_date_id   number,
    p_country       varchar2
  );
  
  procedure load_engagement_data
  (
    p_start_date_id number,
    p_end_date_id   number,
    p_country       varchar2
  );
  
  procedure load_engagement_campaign
  (
    p_start_date_id number,
    p_end_date_id   number,
    p_country       varchar2,
    p_ad_ids        number
  );

end monthly_all_video_tmp; 
/
create or replace package body monthly_all_video_tmp as

  /******************************************************************************
     name:       monthly_all_audience
     purpose:
  
     revisions:
     ver        date        author             description
     ---------  ----------  ---------------   ------------------------------------
     1.0        07/15/2011   bryan beresford            created this package.
  ******************************************************************************/
  procedure run_all_video_tmp(p_start_date_id number) as
    l_month_start number;
    l_month_end   number;
    l_cnt         number := 0;
  begin
    l_month_start := to_number(to_char(trunc(to_date(p_start_date_id, 'yyyymmdd'), 'mm'), 'yyyymmdd'));
    l_month_end   := to_number(to_char(last_day(to_date(p_start_date_id, 'yyyymmdd')), 'yyyymmdd'));
  end;

  procedure load_generic_data
  (
    p_start_date_id number,
    p_end_date_id   number,
    p_country       varchar2
  ) as
  begin
    -- calculate icat aggregate data
    delete from bt_data.all_video_tmp_ic where start_date_id = p_start_date_id and country_code = p_country;
    execute immediate '
      insert into bt_data.all_video_tmp_ic
        (start_date_id, bt_category_id, bt_category, uq_users,
         impressions, clicks, ctr, avg_score, time_stamp,
         country_code)
        select :p_start_date_id as start_date_id, a.bt_category_id,
               b.bt_category, uq_users, impressions, clicks, ctr,
               avg_score, sysdate as time_stamp,
               :p_country as country_code
        from   (select /*+ ORDERED PARALLEL( BUSS 4 4 ) PARALLEL( PUSR 4 4 ) */
                  trim(buss.bt_category_id) bt_category_id,
                  count(distinct(pusr.user_id)) uq_users,
                  sum(pusr.impressions) impressions,
                  0 clicks, 0 as ctr,
                  avg(buss.bt_score) avg_score
                 from   bt_data.TMP_GHOST_RAW_09 pusr,
                        bt_data.TMP_VID0_SCORE_09 buss
                 where  buss.user_id = pusr.user_id
                 group  by trim(buss.bt_category_id)
                 ) a,
               bt_data.bt_taxonomy b
        where  a.bt_category_id = b.bt_category_id(+)
        order  by a.bt_category_id '
      using p_start_date_id, p_country;
    
    COMMIT;
    --calculate segmentwise aggregate data
    for segment_list in (select segmentation,
                                count(1) as i_cat_count
                         from   bt_data.bt_segmentation_taxonomy
                         where  upper(country) = p_country and display_type = 'VIDEO' and segmentation like 'VD%'
                         group  by segmentation
                         order  by segmentation) loop
      delete from bt_data.all_video_tmp_segments
       where date_id = p_start_date_id
         and country = p_country
         and segment = segment_list.segmentation;
      execute immediate '
        insert into bt_data.all_video_tmp_segments
          (date_id, country, segment, uq_users, impressions, clicks,
           visits, ctr, avg_impressions, avg_clicks, avg_visits,
           avg_score)
          with segment_score as
           (select /*+ PARALLEL( FBST 4 4 ) */
             user_id, :segment as segment, sum(bt_score) segment_sum
            from   bt_data.TMP_VID0_SCORE_09 fbst
            where  fbst.bt_category_id in
                   (select bt_category_id
                    from   bt_segmentation_taxonomy
                    where  segmentation = :segment
                    and    country = :l_country
                    and    display_type = ''VIDEO'')
            group  by user_id),
          final_data as
           (select /*+ ORDERED PARALLEL( BUSS 4 4 ) PARALLEL( PUSR 4 4 ) */
             buss.segment, buss.user_id,
             sum(impressions) as impressions, 0 as clicks,
             count(distinct(date_id)) as visits,
             sum(segment_sum) bt_score
            from   segment_score buss,
                   bt_data.TMP_GHOST_RAW_09 pusr
            where  buss.user_id = pusr.user_id
            group  by buss.user_id, buss.segment
            having sum(impressions) < 10000)
          select /*+ PARALLEL( BUSC 4 4 ) */
           :p_start_date_id as date_id, :l_country as country,
           segment, count(distinct(user_id)) uq_users,
           sum(impressions) impressions, sum(clicks) clicks,
           sum(visits) visits,
           ((sum(clicks) / sum(impressions)) * 100) as ctr,
           avg(impressions) avg_impressions, avg(clicks) avg_clicks,
           avg(visits) avg_visits, avg(bt_score) avg_score
          from   final_data
          group  by segment'
        using segment_list.segmentation, segment_list.segmentation, p_country, p_start_date_id, p_country;
      commit;
    end loop;
    -- calculate summary totals
    delete from bt_data.all_video_tmp_summary
     where date_id = p_start_date_id
       and country_code = p_country;
    execute immediate '
      insert into bt_data.all_video_tmp_summary
        (date_id, unique_users, impressions, clicks, time_stamp,
         country_code)
        select /*+ parallel (pusr 4 4) */
         :p_start_date_id, count(distinct(user_id)) unique_users,
         sum(impressions) impressions, 0 clicks,
         sysdate as time_stamp, :p_country
        from   bt_data.TMP_GHOST_RAW_09 pusr'
      using p_start_date_id, p_country;
    commit;
  end load_generic_data;

  procedure load_engagement_data
  (
    p_start_date_id number,
    p_end_date_id   number,
    p_country       varchar2
  ) as
  begin
    -- calculate icat aggregate data
    delete from bt_data.all_eng_tmp_ic where start_date_id = p_start_date_id and country_code = p_country;
    execute immediate '
      insert into bt_data.all_eng_tmp_ic
        (start_date_id, bt_category_id, bt_category, uq_users,
         video25, video50, video75, videoend, avg_score, time_stamp,
         country_code)
        select :p_start_date_id as start_date_id, a.bt_category_id,
               b.bt_category, uq_users, video25, video50, video75, videoend,
               avg_score, sysdate as time_stamp,
               :p_country as country_code
        from   (select /*+ ORDERED PARALLEL( BUSS 2 ) PARALLEL( PUSR 2 ) */
                  trim(buss.bt_category_id) bt_category_id,
                  count(distinct(pusr.user_id)) uq_users, SUM(video25) video25, SUM(video50) video50, SUM(video75) video75, SUM(videoend) videoend,
                  avg(buss.bt_score) avg_score
                 from   bt_data.TMP_ENGAGEMENT_RAW_09 pusr,
                        bt_data.TMP_ENG_SCORE_09 buss
                 where  buss.user_id = pusr.user_id
                 group  by trim(buss.bt_category_id)
                 ) a,
               bt_data.bt_taxonomy b
        where  a.bt_category_id = b.bt_category_id(+)
        order  by a.bt_category_id '
      using p_start_date_id, p_country;
    
    COMMIT;
    --calculate segmentwise aggregate data
    for segment_list in (select segmentation,
                                count(1) as i_cat_count
                         from   bt_data.bt_segmentation_taxonomy
                         where  upper(country) = p_country and display_type = 'VIDEO' and segmentation like 'VD%'
                         group  by segmentation
                         order  by segmentation) loop
      delete from bt_data.all_eng_tmp_segments
       where date_id = p_start_date_id
         and segment = segment_list.segmentation
         and country = p_country; 
      execute immediate '
        insert into bt_data.all_eng_tmp_segments
          (date_id, country, segment, uq_users, video25, video50, video75, videoend,
           visits, avg_visits, avg_score)
          with segment_score as
           (select /*+ PARALLEL( FBST 2 ) */
             user_id, :segment as segment, sum(bt_score) segment_sum
            from   bt_data.TMP_ENG_SCORE_09 fbst
            where  fbst.bt_category_id in
                       (select bt_category_id
                        from   bt_segmentation_taxonomy
                        where  segmentation = :segment
                        and    country = :l_country
                        and    display_type = ''VIDEO'')
              -- and  fbst.user_id in (select user_id from bt_data.TMP_ENGAGEMENT_RAW_08)
            group  by user_id),
          final_data as
           (select /*+ ORDERED PARALLEL( BUSS 2 ) PARALLEL( PUSR 2 ) */
             buss.segment, buss.user_id,
             SUM(video25) video25, SUM(video50) video50, SUM(video75) video75, SUM(videoend) videoend,
             0 as visits,
             sum(segment_sum) bt_score
            from   segment_score buss,
                   bt_data.TMP_ENGAGEMENT_RAW_09 pusr
            where  buss.user_id = pusr.user_id
            group  by buss.user_id, buss.segment
            having sum(videoend) < 10000)
          select /*+ PARALLEL( BUSC 2 ) */
           :p_start_date_id as date_id, :l_country as country,
           segment, count(distinct(user_id)) uq_users,
           SUM(video25) video25, SUM(video50) video50, SUM(video75) video75, SUM(videoend) videoend,
           0 visits,
           0 avg_visits, avg(bt_score) avg_score
          from   final_data
          group  by segment'
        using segment_list.segmentation, segment_list.segmentation, p_country, p_start_date_id, p_country;
      commit;
    end loop;
    -- calculate summary totals: DONE
    delete from bt_data.all_eng_tmp_summary
     where date_id = p_start_date_id
       and country_code = p_country;
    execute immediate '
      insert into bt_data.all_eng_tmp_summary
        (date_id, unique_users, video25, video50, video75, videoend, time_stamp, country_code)
        select /*+ parallel (pusr 4 4) */
         :p_start_date_id, count(distinct(user_id)) unique_users, SUM(video25), SUM(video50), SUM(video75), SUM(videoend),
         sysdate as time_stamp, :p_country
        from   bt_data.TMP_ENGAGEMENT_RAW_09 pusr'
      using p_start_date_id, p_country;
    commit;
  end load_engagement_data;

  procedure load_engagement_campaign
  (
    p_start_date_id number,
    p_end_date_id   number,
    p_country       varchar2,
    p_ad_ids        number
  ) as
  begin
  
    -- LOAD RAW DATA INTO TEMP TABLES FOR THIS CAMPAIGH
    --  TODO: CHANGE THIS TO PARTITIONED "JOB" TABLE FOR BT REPORT PACKAGE
    --        TO SUPPORT MULTIPLE AD IDS RAW QUERY WILL HAVE TO BE CHANGED TO IMMEDIATE
    EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_DATA.TMP_ENG_RAW_CAMPAIGN';

    INSERT INTO BT_DATA.TMP_ENG_RAW_CAMPAIGN
    SELECT /*+ PARALLEL(A 3) */
        USER_ID
        , sum(DECODE(EVENT_CODE,'video25',EVENT_COUNT,0)) video25
        , sum(DECODE(EVENT_CODE,'video50',EVENT_COUNT,0)) video50
        , sum(DECODE(EVENT_CODE,'video75',EVENT_COUNT,0)) video75
        , sum(DECODE(EVENT_CODE,'videoend',EVENT_COUNT,0)) videoend
      FROM BO.ATAKO_ENGAGEMENT@benlink A
     WHERE DATE_ID >= p_start_date_id
       AND DATE_ID <= p_end_date_id
       AND A.AD_ID = p_ad_ids
       AND DEVELOPER_ID IS NOT NULL
     GROUP BY USER_ID;
    COMMIT;
    EXECUTE IMMEDIATE 'ANALYZE TABLE BT_DATA.TMP_ENG_RAW_CAMPAIGN ESTIMATE STATISTICS';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE BT_DATA.TMP_ENG_SCORE_CAMPAIGN';
    INSERT INTO BT_DATA.TMP_ENG_SCORE_CAMPAIGN
      SELECT /*+ parallel( a 3 ) */
            p_start_date_id as date_id, user_id, bt_category_id,
            null as bt_score
       FROM bt_data.fact_bt_category_visits_new a
      WHERE date_id >= p_start_date_id
        AND DATE_ID <= p_end_date_id
        AND USER_ID IN (SELECT USER_ID FROM BT_DATA.TMP_ENG_RAW_CAMPAIGN)
      group  by user_id, bt_category_id;
    COMMIT;
    EXECUTE IMMEDIATE 'ANALYZE TABLE BT_DATA.TMP_ENG_SCORE_CAMPAIGN ESTIMATE STATISTICS';

    -- CALCULATE ICAT AGGREGATE DATA
    DELETE FROM BT_DATA.CAMPAIGN_ENG_TMP_IC
     WHERE start_date_id = p_start_date_id
       AND country_code = p_country
       AND ad_ids = p_ad_ids;
    EXECUTE IMMEDIATE '
      insert into BT_DATA.CAMPAIGN_ENG_TMP_IC
        (start_date_id, ad_ids, bt_category_id, bt_category, uq_users,
         video25, video50, video75, videoend, avg_score, time_stamp,
         country_code)
        select :p_start_date_id as start_date_id, :p_ad_ids as ad_ids, a.bt_category_id,
               b.bt_category, uq_users, video25, video50, video75, videoend,
               avg_score, sysdate as time_stamp,
               :p_country as country_code
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
      USING p_start_date_id, p_ad_ids, p_country;
    COMMIT;
    
    -- CALCULATE SEGMENTWISE AGGREGATE DATA
    FOR segment_list IN (SELECT segmentation,
                                COUNT(1) as i_cat_count
                           FROM   bt_data.bt_segmentation_taxonomy
                          WHERE UPPER(country) = p_country
                            AND display_type = 'VIDEO'
                            AND segmentation LIKE 'VD%'
                          GROUP BY segmentation
                          ORDER BY segmentation) loop
      DELETE FROM BT_DATA.CAMPAIGN_ENG_TMP_SEGMENTS
       WHERE date_id = p_start_date_id
         AND segment = segment_list.segmentation
         AND country = p_country
         AND AD_IDS = p_ad_ids; 
      EXECUTE IMMEDIATE '
        insert into BT_DATA.CAMPAIGN_ENG_TMP_SEGMENTS
          (date_id, ad_ids, country, segment, uq_users, video25, video50, video75, videoend,
           visits, avg_visits, avg_score)
          with segment_score as
           (select /*+ PARALLEL( FBST 2 ) */
             user_id, :segment as segment, sum(bt_score) segment_sum
            from   BT_DATA.TMP_ENG_SCORE_CAMPAIGN fbst
            where  fbst.bt_category_id in
                       (select bt_category_id
                        from   bt_segmentation_taxonomy
                        where  segmentation = :segment
                        and    country = :l_country
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
           :p_start_date_id as date_id, :p_ad_ids, :l_country as country,
           segment, count(distinct(user_id)) uq_users,
           SUM(video25) video25, SUM(video50) video50, SUM(video75) video75, SUM(videoend) videoend,
           0 visits,
           0 avg_visits, avg(bt_score) avg_score
          from   final_data
          group  by segment'
        USING segment_list.segmentation, segment_list.segmentation, p_country, p_start_date_id, p_ad_ids, p_country;
      COMMIT;
    end loop;
    
    -- CALCULATE SUMMARY TOTALS
    DELETE FROM BT_DATA.CAMPAIGN_ENG_TMP_SUMMARY
     WHERE date_id = p_start_date_id
       AND country_code = p_country
         AND AD_IDS = p_ad_ids; 
    EXECUTE IMMEDIATE '
        INSERT INTO BT_DATA.CAMPAIGN_ENG_TMP_SUMMARY
            (date_id, ad_ids, unique_users, video25, video50, video75, videoend, time_stamp, country_code)
        SELECT /*+ parallel (pusr 3) */
            :p_start_date_id, :p_ad_ids, count(distinct(user_id)) unique_users, SUM(video25), SUM(video50), SUM(video75), SUM(videoend),
            sysdate as time_stamp, :p_country
          FROM   BT_DATA.TMP_ENG_RAW_CAMPAIGN pusr'
        using p_start_date_id, p_ad_ids, p_country;
    COMMIT;
  end load_engagement_campaign;
end monthly_all_video_tmp; 
/
