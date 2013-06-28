create or replace package bt_data.pkg_bt_daily as

  procedure run_bt
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure update_metadata_tables
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_monthly_campaign_raw
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_bt_visits_new
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_category_visits
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_bt_score_temp
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_fact_bt_delivery
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure write_memcache_file
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure write_sortdb_file
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body bt_data.pkg_bt_daily as

  procedure run_bt
  (
    p_process_name varchar2,
    p_process_date number
  ) is
    l_run_id        number;
    l_process_date  number;
    l_process_name  varchar2(100);
    l_error_msg     varchar2(4000);
    l_start_process varchar2(100);
    l_mail_msg      varchar2(100);
    l_to            varchar2(1000);
  begin
    l_process_name  := initcap(p_process_name);
    l_start_process := l_process_name || '-run_bt';
    l_mail_msg      := 'Behavioral Targeting Data Load';
    l_to            := 'btimportreport@glam.com';
    if p_process_date is null then
      l_process_date := to_number(to_char(sysdate - 1, 'yyyymmdd'));
    elsif p_process_date is not null then
      l_process_date := p_process_date;
    end if;
    select max(run_id)
    into   l_run_id
    from   daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) like
           upper(p_process_name) || '-RUN_BT%';
    if l_run_id is null then
      l_run_id := seq_daily_bt_run.nextval;
    end if;
    -- make sure we delete the record for the main process name(this has dependency in shell script).
    delete from daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) like
           upper(p_process_name) || '-RUN_BT%'
    and    run_id = l_run_id
    and    status in ('RUNNING', 'FAIL');
    commit;
    -- send intimation mail for starting of process
    pa_send_email.compose_email(l_run_id, 'START', l_process_date, l_mail_msg, l_start_process, l_to);
    -- log starting of the process
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'I');
    -- start
    update_metadata_tables(l_run_id, l_process_date, l_process_name);
    load_monthly_campaign_raw(l_run_id, l_process_date, l_process_name);
    load_fact_bt_visits_new(l_run_id, l_process_date, l_process_name);
    load_fact_category_visits(l_run_id, l_process_date, l_process_name);
    if upper(p_process_name) = 'PROCESS' then
      load_fact_bt_score_temp(l_run_id, l_process_date, l_process_name);
      write_sortdb_file(l_run_id, l_process_date, l_process_name);
    end if;
    -- mark the process as complete in log table
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'UC');
    -- send completion mail
    pa_send_email.compose_email(l_run_id, 'COMPLETE', l_process_date, l_mail_msg, l_start_process, l_to);
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      -- failed steps are handled in compose mail message procedure
      pa_send_email.compose_email(l_run_id, 'FAIL', l_process_date, l_mail_msg, l_start_process, l_to);
  end;

  procedure update_metadata_tables
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
  begin
    l_process_name := p_process_name || '-' ||
                      'update_metadata_tables';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from bt_segmentation_taxonomy;
      commit;
      insert into bt_segmentation_taxonomy
        (bt_category_id, bt_category, segmentation, country,
         display_type)
        select category_abbr as bt_category_id,
               category_name as bt_category,
               segment_name as segmentation, country,
               display_type
        from   admin_data.btn_category@ronap a,
               admin_data.btn_segmentation@ronap b,
               admin_data.btn_taxonomy@ronap c
        where  a.category_id = c.category_id
        and    b.segment_id = c.segment_id;
      commit;
      delete from bt_taxonomy;
      commit;
      insert into bt_taxonomy
        (bt_category_id, bt_category, created_by,
         last_updated, updated_by)
        select bt_category_id, bt_category, created_by,
               last_updated, updated_by
        from   admin_data.bt_taxonomy@ronap;
      commit;
      delete from affiliate_targeting_data;
      commit;
      insert into affiliate_targeting_data
        (id, last_updated, affiliate_id, site_name,
         site_url, match_url, primary_channel, bt_cat_1,
         bt_cat_2, bt_cat_3, bt_cat_4, bt_key_values,
         extra_key_values, dart_site_id, country, bt_cat_5,
         bt_cat_6, bt_cat_7, bt_cat_8, bt_cat_9, bt_cat_10)
        select id, last_updated, affiliate_id, site_name,
               site_url, match_url, primary_channel,
               bt_cat_1, bt_cat_2, bt_cat_3, bt_cat_4,
               bt_key_values, extra_key_values, dart_site_id,
               country, bt_cat_5, bt_cat_6, bt_cat_7,
               bt_cat_8, bt_cat_9, bt_cat_10
        from   admin_data.affiliate_targeting_data@ronap;
      commit;
      delete from dim_bt_category_threshold;
      commit;
      insert into dim_bt_category_threshold
        (bt_category_id, category_threshold, created_by,
         created_date, updated_by, updated_date)
        select bt_category_id, category_threshold,
               created_by, created_date, updated_by,
               updated_date
        from   admin_data.dim_bt_category_threshold@ronap;
      commit;
      update bt_data.affiliate_targeting_data
      set    dart_site_id =
              (select dart_site_id
               from   ap_data.channel@ronap
               where  id = affiliate_id);
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC');
    end if;
    l_process_name := p_process_name || '-' ||
                      'channel_aggregate';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from channel_aggregate;
      insert into channel_aggregate
        select * from ods_metadata.channel_aggregate@roods;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC');
    end if;
    l_process_name := p_process_name || '-' ||
                      'network_ad_extended';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from network_ad_extended;
      insert into network_ad_extended
        select *
        from   ods_metadata.network_ad_extended@roods;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC');
    end if;
  end;

  procedure load_monthly_campaign_raw
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_sql          varchar2(4000);
  begin
    -- monthly_campaign_raw
    l_process_name := p_process_name || '-' ||
                      'monthly_campaign_raw';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      select count(1)
      into   l_cnt
      from   bt_report_schedule
      where  report_status in (0, 1)
      and    end_date = p_process_date;
      if upper(p_process_name) in
         ('PROCESS', 'REPROCESS', 'MANUAL') and l_cnt = 0 then
        execute immediate 'Alter table monthly_campaign_raw_new truncate partition P_' ||
                          p_process_date;
        l_sql := 'insert /*+APPEND */ ';
      else
        delete from monthly_campaign_raw_new
        where  date_id = p_process_date;
        l_sql := 'insert  ';
      end if;
      l_sql := l_sql ||
               'into bt_data.monthly_campaign_raw_new
                (date_id, user_id, country_id, ad_id, impressions,
                 clicks, country_code)
                select date_id, user_id, country_id, ad_id,
                       impressions, clicks, country_code
                from   campaign.fact_bt_monthly_raw@roods
                where  date_id = :p_process_date';
      execute immediate l_sql
        using p_process_date;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table monthly_campaign_raw_new partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_fact_bt_visits_new
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_bt_visits_new';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table fact_bt_visits_new truncate partition P_' ||
                        p_process_date;
      insert /*+APPEND */
      into fact_bt_visits_new
        (date_id, user_id, site_id, page_visits, source_id,
         affiliate_id, ga_os_id)
        select date_id, user_id, site_id, page_visits,
               source_id, affiliate_id, ga_os_id
        from   campaign.fact_bt_visits_new@roods
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_fact_category_visits
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_sql          varchar2(4000);
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_bt_category_visits_new';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      select count(1)
      into   l_cnt
      from   bt_report_schedule
      where  report_status in (0, 1)
      and    end_date = p_process_date;
      if upper(p_process_name) in
         ('PROCESS', 'REPROCESS', 'MANUAL') and l_cnt = 0 then
        execute immediate 'Alter table fact_bt_category_visits_new truncate partition P_' ||
                          p_process_date;
        l_sql := 'insert /*+APPEND */ ';
      else
        delete from fact_bt_category_visits_new
        where  date_id = p_process_date;
        l_sql := 'insert  ';
      end if;
      execute immediate l_sql ||
                        'into bt_data.fact_bt_category_visits_new
                (date_id, user_id, bt_category_id,
                 weighted_page_visits, source_id, ga_os_id)
                select /*+ PARALLEL(a,4,1) */
                 date_id, user_id, name as bt_category_id,
                 sum(cat_score) as weighted_page_visits, 
                 source_id, ga_os_id
                from   (select /*+ PARALLEL(FBV,5,1) NO_USE_NL (FBV AFD) */
                          user_id, site_id, bt_cat_1 as name,
                          page_visits, date_id,
                          page_visits as cat_score, source_id,
                          ga_os_id
                         from   bt_data.fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         union all
                         select /*+ PARALLEL(FBV,5,1) NO_USE_NL (FBV AFD) */
                          user_id, site_id, bt_cat_2 as name,
                          page_visits, date_id,
                          page_visits as cat_score, source_id,
                          ga_os_id
                         from   fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         union all
                         select /*+ PARALLEL(FBV,5,1) NO_USE_NL (FBV AFD) */
                          user_id, site_id, bt_cat_3 as name,
                          page_visits, date_id,
                          page_visits as cat_score, source_id,
                          ga_os_id
                         from   bt_data.fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         union all
                         select /*+ PARALLEL(FBV,5,1) NO_USE_NL (FBV AFD) */
                          user_id, site_id, bt_cat_4 as name,
                          page_visits, date_id,
                          page_visits as cat_score, source_id,
                          ga_os_id
                         from   bt_data.fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         union all -- PARENT CATEGORY
                         select /*+ PARALLEL(FBV,5,1) ORDERED NO_USE_NL (FBV BCR) */
                          user_id, site_id, bcr.parent_name as name,
                          page_visits, date_id,
                          page_visits as cat_score, source_id,
                          ga_os_id
                         from   bt_data.fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd,
                                bt_data.bt_category_relationship bcr
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         and    bcr.name = bt_cat_1
                         union all
                         select /*+ PARALLEL(FBV,5,1) ORDERED NO_USE_NL (FBV BCR) */
                          user_id, site_id, bcr.parent_name as name,
                          page_visits, date_id,
                          page_visits as cat_score, source_id,
                          ga_os_id
                         from   bt_data.fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd,
                                bt_data.bt_category_relationship bcr
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         and    bcr.name = bt_cat_2
                         union all
                         select /*+ PARALLEL(FBV,5,1) ORDERED NO_USE_NL (FBV BCR) */
                          user_id, site_id, bcr.parent_name as name,
                          page_visits, date_id,
                          page_visits as cat_score, source_id,
                          ga_os_id
                         from   bt_data.fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd,
                                bt_data.bt_category_relationship bcr
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         and    bcr.name = bt_cat_3
                         union all
                         select /*+ PARALLEL(FBV,5,1) ORDERED NO_USE_NL (FBV BCR) */
                          user_id, site_id, bcr.parent_name as name,
                          page_visits, date_id,
                          page_visits as cat_score, source_id,
                          ga_os_id
                         from   bt_data.fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd,
                                bt_data.bt_category_relationship bcr
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         and    bcr.name = bt_cat_4
                         union all -- GRANDPARENT CATEGORY
                         select /*+ PARALLEL(FBV,5,1) ORDERED NO_USE_NL (FBV BCR) */
                          user_id, site_id,
                          bcr.grandparent_name as name, page_visits,
                          date_id, page_visits as cat_score, source_id,
                          ga_os_id
                         from   bt_data.fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd,
                                bt_data.bt_category_relationship bcr
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         and    bcr.name = bt_cat_1
                         union all
                         select /*+ PARALLEL(FBV,5,1) ORDERED NO_USE_NL (FBV BCR) */
                          user_id, site_id,
                          bcr.grandparent_name as name, page_visits,
                          date_id, page_visits as cat_score, source_id,
                          ga_os_id
                         from   bt_data.fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd,
                                bt_data.bt_category_relationship bcr
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         and    bcr.name = bt_cat_2
                         union all
                         select /*+ PARALLEL(FBV,5,1) ORDERED NO_USE_NL (FBV BCR) */
                          user_id, site_id,
                          bcr.grandparent_name as name, page_visits,
                          date_id, page_visits as cat_score, source_id,
                          ga_os_id
                         from   bt_data.fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd,
                                bt_data.bt_category_relationship bcr
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         and    bcr.name = bt_cat_3
                         union all
                         select /*+ PARALLEL(FBV,5,1) ORDERED NO_USE_NL (FBV BCR) */
                          user_id, site_id,
                          bcr.grandparent_name as name, page_visits,
                          date_id, page_visits as cat_score, source_id,
                          ga_os_id
                         from   bt_data.fact_bt_visits_new fbv,
                                bt_data.affiliate_targeting_data afd,
                                bt_data.bt_category_relationship bcr
                         where  fbv.site_id = afd.dart_site_id
                         and    fbv.date_id = :p_process_date
                         and    bcr.name = bt_cat_4) a
                where  name is not null
                group  by user_id, date_id, name, source_id,
                          ga_os_id'
        using p_process_date, p_process_date, p_process_date, p_process_date, p_process_date, p_process_date, p_process_date, p_process_date, p_process_date, p_process_date, p_process_date, p_process_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_fact_bt_score_temp
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_bt_score_temp';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      /*begin
        execute immediate 'drop table fact_bt_score_temp purge';
      exception
        when others then
          null;
      end;
      execute immediate '
        create table fact_bt_score_temp
               compress for all operations 
               tablespace tbl_monthly_process 
               parallel(degree 6 instances 2) 
               nologging 
               as
          select \*+ parallel( a 3 4 ) *\
           ' || p_process_date ||
                        ' as date_id, user_id, bt_category_id,
           sum(weighted_page_visits *
                ((10 - (to_date(' ||
                        p_process_date ||
                        ', ''yyyymmdd'') -
                to_date(date_id, ''yyyymmdd''))) / 10)) as bt_score
          from   fact_bt_category_visits_new a
          where  date_id >=
                 to_number(to_char(to_date(' ||
                        p_process_date ||
                        ', ''yyyymmdd'') - 10, ''yyyymmdd''))
            and  to_char(user_id) not like ''22%22''
          group  by user_id, bt_category_id';*/
      execute immediate 'truncate table fact_bt_score_temp';
      insert /*+ append */
      into fact_bt_score_temp
        select /*+ parallel( a 3 4 ) */
         p_process_date as date_id, user_id, bt_category_id,
         sum(weighted_page_visits *
              ((10 - (to_date(p_process_date, 'yyyymmdd') -
              to_date(date_id, 'yyyymmdd'))) / 10)) as bt_score
        from   fact_bt_category_visits_new a
        where  date_id >=
               to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 10, 'yyyymmdd'))
        and    to_char(user_id) not like '22%22'
        group  by user_id, bt_category_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table bt_data.fact_bt_score_temp estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_fact_bt_delivery
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'fact_bt_delivery';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table fact_bt_delivery';
      execute immediate '
        insert /*+APPEND */
         into fact_bt_delivery
          (user_id, category_list)        
          select user_id,
                 user_id || '','' ||
                 concat_all(concat_expr(a.bt_category_id, '',''))
          from   fact_bt_score_temp a, dim_bt_category_threshold ct
          where  a.bt_category_id = ct.bt_category_id
          and    bt_score > ct.category_threshold
          group  by user_id';
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure write_memcache_file
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    v_file         utl_file.file_type;
    v_buffer       varchar2(32767);
    v_name         varchar2(128) := 'bt_memcache' ||
                                    p_process_date ||
                                    '.txt';
    v_lines        pls_integer := 0;
    c_eol     constant varchar2(1) := chr(10);
    c_eollen  constant pls_integer := length(c_eol);
    c_maxline constant pls_integer := 32767;
  begin
    l_process_name := p_process_name || '-' ||
                      'write_memcache_file';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      v_file := utl_file.fopen('BT_OUTPUT', v_name, 'W', 32767);
      /*for r in (select \*+ PARALLEL (a 3 4) *\
       user_id, category_list as csv
      from   fact_bt_delivery a) loop*/
      for r in (select user_id,
                       user_id || ',' ||
                        concat_all(concat_expr(a.bt_category_id, ',')) as csv
                from   fact_bt_score_temp a,
                       dim_bt_category_threshold ct
                where  a.bt_category_id = ct.bt_category_id
                and    bt_score > ct.category_threshold
                group  by user_id) loop
        if length(v_buffer) + c_eollen + length(r.csv) <=
           c_maxline then
          v_buffer := v_buffer || c_eol || r.csv;
        else
          if v_buffer is not null then
            utl_file.put_line(v_file, v_buffer);
          end if;
          v_buffer := r.csv;
        end if;
        v_lines := v_lines + 1;
      end loop;
      utl_file.put_line(v_file, v_buffer);
      utl_file.fclose(v_file);
      l_cnt := v_lines;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure write_sortdb_file
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_run_id       number := p_run_id;
    v_file         utl_file.file_type;
    v_buffer       varchar2(32767);
    v_name         varchar2(128) := 'bt_sortdb' ||
                                    p_process_date ||
                                    '.csv';
    v_lines        pls_integer := 0;
    c_sep     constant varchar2(1) := chr(9);
    c_seplen  constant pls_integer := length(c_sep);
    c_eol     constant varchar2(1) := chr(10);
    c_eollen  constant pls_integer := length(c_eol);
    c_maxline constant pls_integer := 32767;
  begin
    if l_run_id is null then
      select max(run_id)
      into   l_run_id
      from   daily_process_log
      where  process_date = p_process_date
      and    upper(process_name) like '%BT_DAILY%';
      if l_run_id is null then
        l_run_id := seq_daily_bt_run.nextval;
      end if;
    end if;
    l_process_name := p_process_name || '-' ||
                      'write_sortdb_file';
    if pkg_log_process.is_not_complete(l_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(l_run_id, p_process_date, l_process_name, 'I');
      v_file := utl_file.fopen('BT_OUTPUT', v_name, 'W', 32767);
      for r in (select to_char(user_id) userid,
                       concat_all(concat_expr(a.bt_category_id, ',')) bt_scores
                from   fact_bt_score_temp a,
                       dim_bt_category_threshold ct
                where  a.bt_category_id = ct.bt_category_id
                and    bt_score > ct.category_threshold
                group  by user_id
                order  by userid) loop
        if length(v_buffer) + c_eollen + length(r.userid) +
           c_seplen + length(r.bt_scores) <= c_maxline then
          v_buffer := v_buffer || c_eol || r.userid ||
                      c_sep || r.bt_scores;
        else
          if v_buffer is not null then
            utl_file.put_line(v_file, v_buffer);
          end if;
          v_buffer := r.userid || c_sep || r.bt_scores;
        end if;
        v_lines := v_lines + 1;
      end loop;
      utl_file.put_line(v_file, v_buffer);
      utl_file.put_line(v_file, 'updated' || c_sep ||
                         to_char(sysdate, 'yyyymmddhh24miss'));
      utl_file.fclose(v_file);
      l_cnt := v_lines;
      commit;
      pkg_log_process.log_process(l_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

end;
/
