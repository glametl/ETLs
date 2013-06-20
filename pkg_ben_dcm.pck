create or replace package pkg_ben_dcm as

  procedure run_dcm
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_dart_counts_new
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_dart_counts_daily_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_dart_counts_monthly
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_glamcontent_aggregate
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_video_play_data
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_module_aggregate
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure replicate_to_ben
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure replicate_to_www
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_ben_dcm as

  procedure run_dcm
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
    l_start_process := l_process_name || '-run_dcm';
    l_mail_msg      := 'BEN DCM Data';
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
    load_dart_counts_new(l_run_id, l_process_date, l_process_name);
    l_max_date := '';
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
        load_dart_counts_daily_agg(l_run_id, i.date_id, l_process_name);
        l_max_date := i.date_id;
      end loop;
      load_dart_counts_monthly(l_run_id, l_max_date, l_process_name);
    else
      load_dart_counts_daily_agg(l_run_id, l_process_date, l_process_name);
      load_dart_counts_monthly(l_run_id, l_process_date, l_process_name);
    end if;
    load_glamcontent_aggregate(l_run_id, l_process_date, l_process_name);
    load_video_play_data(l_run_id, l_process_date, l_process_name);
    load_module_aggregate(l_run_id, l_process_date, l_process_name);
    replicate_to_ben(l_run_id, l_process_date, l_process_name);
    replicate_to_www(l_run_id, l_process_date, l_process_name);
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

  --879
  procedure load_dart_counts_new
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_dart_counts_new';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_dart_counts_new truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ben_dart_counts_new
        (date_id, process_date_id, count_type, view_date,
         count_number, ad_id, site_id, country_id,
         state_province, dma_id, page_id, city_id,
         ad_size_request, view_month, view_year, data_source,
         agent_type, internal_ip, ad_group_id)
        select /*+ parallel(a,5,1) */
         p_process_date, p_process_date, 0 as count_type,
         to_date(p_process_date, 'yyyymmdd') view_date,
         sum(total_impressions) as count_number, ad_id,
         site_id, country_id, state_province, dma_id,
         page_id, city_id,
         coalesce(b.ad_size_request, 0) as ad_size_request,
         to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'mm')) as view_month,
         to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'yyyy'), 'yyyy')) as view_year,
         coalesce(source_id, 1), agent_type, internal_ip,
         ad_group_id
        from   ben_fact_media_day a, dim_ad_size_request b
        where  date_id = p_process_date
        and    total_impressions > 0
        and    a.ad_size_request = b.request_dimensions(+)
        group  by ad_id, site_id, country_id, state_province,
                  dma_id, page_id, city_id,
                  coalesce(b.ad_size_request, 0), source_id,
                  agent_type, internal_ip, ad_group_id;
      commit;
      insert /*+ append */
      into ben_dart_counts_new
        (date_id, process_date_id, count_type, view_date,
         count_number, ad_id, site_id, country_id,
         state_province, dma_id, page_id, city_id,
         ad_size_request, view_month, view_year, data_source,
         agent_type, internal_ip, ad_group_id)
        select /*+ parallel(a,5,1) */
         p_process_date, p_process_date, 1 as count_type,
         to_date(p_process_date, 'yyyymmdd') view_date,
         sum(total_clicks) as count_number, ad_id, site_id,
         country_id, state_province, dma_id, page_id,
         city_id,
         coalesce(b.ad_size_request, 0) as ad_size_request,
         to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'mm')) as view_month,
         to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'yyyy'), 'yyyy')) as view_year,
         coalesce(source_id, 1), agent_type, internal_ip,
         ad_group_id
        from   ben_fact_media_day a, dim_ad_size_request b
        where  date_id = p_process_date
        and    total_clicks > 0
        and    a.ad_size_request = b.request_dimensions(+)
        group  by ad_id, site_id, country_id, state_province,
                  dma_id, page_id, city_id,
                  coalesce(b.ad_size_request, 0), source_id,
                  agent_type, internal_ip, ad_group_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_dart_counts_new partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  -- 1108
  procedure load_dart_counts_daily_agg
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
                      'ben_dart_counts_daily_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if to_char(to_date(p_process_date, 'yyyymmdd'), 'dd') = '01' then
        l_previous_dt := '';
      else
        l_previous_dt := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd'));
      end if;
      execute immediate 'Alter table ben_dart_counts_daily_agg truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ben_dart_counts_daily_agg
        (count_type, view_date, count_number, ad_id,
         site_id, country_id, state_province, dma_id,
         page_id, city_id, ad_size_request, view_month,
         view_year, view_date_id, days_counted, data_source,
         agent_type, internal_ip, ad_group_id, date_id)
        select count_type,
               trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'),
               sum(count_number) as count_number, ad_id,
               site_id, country_id, state_province, dma_id,
               page_id, city_id, ad_size_request, view_month,
               view_year, p_process_date as view_date_id,
               to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'dd'), 'dd')) as days_counted,
               data_source, agent_type, internal_ip,
               ad_group_id, p_process_date
        from   (select /*+ parallel(nrd,4,1) */
                  count_type, count_number, ad_id, site_id,
                  country_id, state_province, dma_id, page_id,
                  city_id, ad_size_request, view_month,
                  view_year, data_source, agent_type,
                  internal_ip, ad_group_id
                 from   ben_dart_counts_daily_agg nrd
                 where  date_id = l_previous_dt
                 union all
                 select /*+ parallel(nrd,4,1) */
                  count_type, count_number, ad_id, site_id,
                  country_id, state_province, dma_id, page_id,
                  city_id, ad_size_request, view_month,
                  view_year, data_source, agent_type,
                  internal_ip, ad_group_id
                 from   ben_dart_counts_new nrd
                 where  date_id = p_process_date)
        group  by count_type, ad_id, site_id, country_id,
                  state_province, dma_id, page_id, city_id,
                  ad_size_request, view_month, view_year,
                  data_source, agent_type, internal_ip,
                  ad_group_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_dart_counts_daily_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  -- 1108
  procedure load_dart_counts_monthly
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_dcm_current';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table ben_dcm_current';
      insert /*+ append */
      into ben_dcm_current
        (count_type, view_date, count_number, ad_id,
         site_id, country_id, state_province, dma_id,
         page_id, city_id, ad_size_request, view_month,
         view_year, view_date_id, days_counted, data_source,
         agent_type, internal_ip, ad_group_id)
        select /*+ parallel(a,4,1) */
         count_type, view_date, count_number, ad_id, site_id,
         country_id, state_province, dma_id, page_id,
         city_id, ad_size_request, view_month, view_year,
         to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd')) as view_date_id,
         days_counted, data_source, agent_type, internal_ip,
         ad_group_id
        from   ben_dart_counts_daily_agg a
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_dcm_current estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  --1772
  procedure load_glamcontent_aggregate
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_glamcontent_aggregate';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_glamcontent_aggregate truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ben_glamcontent_aggregate
        (view_date, cid, did, mid, affiliate_id,
         video_views, adjacent_ads, content_type, date_id,
         process_date_id, created_date, data_source)
        select /*+ parallel(a,4,1) */
         to_date(p_process_date, 'yyyymmdd'), cid, did, mid,
         cag.id, sum(total_impressions) as video_views,
         sum(total_impressions) as adjacent_ads,
         'A' as content_type, date_id, p_process_date,
         sysdate, 1 data_source
        from   ben_fact_media_inventory a,
               ods_metadata.channel cag,
               ods_metadata.adm_ads nad
        where  date_id = p_process_date
        and    a.did > 0
        and    a.site_id = cag.dart_site_id
        and    a.ad_id = nad.ad_id
        and    nad.ad_size_id in
               ('160x600', '300x250', '300x600', '728x90', '800x150', '630x150', '270x150', '800x50', '160x160', '650x35', '984x258', '990x26', '728x91', '120x600')
        group  by date_id, cid, did, mid, cag.id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_glamcontent_aggregate partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  --1953
  procedure load_video_play_data
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_video_play_data';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_video_play_data truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ben_video_play_data
        (view_date, cid, did, video_views, adjacent_ads,
         pre_roll, over, post_roll, date_id, process_date_id,
         created_date, data_source)
        select g.view_date, g.cid, g.did,
               sum(g.video_views) as video_views,
               sum(g.adjacent_ads) as adjacent_ads,
               sum(vds.pre) as pre_roll,
               sum(vds.over) as over,
               sum(vds.post) as post_roll, p_process_date,
               p_process_date, sysdate as created_date,
               coalesce(g.data_source, 1)
        from   (select /*+ parallel(a,4,1) */
                  view_date, cid, did,
                  sum(video_views) as video_views,
                  sum(adjacent_ads) as adjacent_ads,
                  data_source
                 from   ben_glamcontent_aggregate a
                 where  (mid in
                        (select distinct mid
                          from   insider_data.insider_glamtv_groups@etlwww) or
                        mid = 404136226)
                 and    date_id = p_process_date
                 group  by view_date, cid, did, data_source) g,
               (select to_char((vds.creation_date + 3 / 24), 'yyyy-mm-dd') as creation_date,
                        to_number(trim(did)) did,
                        sum(case
                              when ad_type = 'pre' then
                               1
                              else
                               0
                            end) as pre,
                        sum(case
                              when ad_type = 'over' then
                               1
                              else
                               0
                            end) as over,
                        sum(case
                              when ad_type = 'post' then
                               1
                              else
                               0
                            end) as post
                 from   glamspace_data.video_ad_traffic@etlwww vds
                 where  did is not null
                 and    to_date(to_char((vds.creation_date +
                                        3 / 24), 'yyyy-mm-dd'), 'yyyy-mm-dd') =
                        to_date(p_process_date, 'yyyymmdd')
                 group  by to_char((vds.creation_date +
                                    3 / 24), 'yyyy-mm-dd'),
                           to_number(trim(did))) vds
        where  g.did = vds.did(+)
        and    to_char(g.view_date, 'yyyy-mm-dd') =
               vds.creation_date(+)
        group  by g.view_date, g.cid, g.data_source, g.did
        union all
        select /*+ parallel(a,4,1) */
         view_date, content_partner_id, data_id,
         null as video_views,
         sum(a.impressions_delivered) as adjacent_ads,
         null as pre_roll, null as over, null as post_roll,
         p_process_date, p_process_date,
         sysdate as created_date, 1 as data_source
        from   ben_network_revenue_daily a,
               ods_metadata.adm_ads b
        where  date_id = p_process_date
        and    country = 'WW'
        and    content_partner_id in
               (select id
                 from   ods_metadata.channel
                 where  affiliate_type = 'Content Partner')
        and    a.ad_id = b.ad_id
        and    b.category not in ('Ad Server', 'Internal')
        and    b.ad_size_id in
               ('300x250', '160x600', '110x90', '555x2', '728x90', '300x600')
        and    a.data_id > 0
        group  by view_date, content_partner_id, data_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_video_play_data partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  --2123
  procedure load_module_aggregate
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_module_aggregate';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_module_aggregate truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ben_module_aggregate
        (view_date, mid, ad_name, affiliate_id, country_id,
         impressions, clicks, date_id, process_date_id,
         created_date, data_source)
        select /*+ parallel(dc,4,1) */
         to_date(p_process_date, 'yyyymmdd'),
         dc.ad_id as mid, nav.name, dc.affiliate_id,
         dc.country_id, sum(total_impressions) impressions,
         sum(total_clicks) clicks, p_process_date,
         p_process_date, sysdate, 2 as data_source
        from   ben_fact_media_inventory dc,
               ods_metadata.adm_ads nav
        where  dc.date_id = p_process_date
        and    dc.ad_id = nav.ad_id
        and    nav.ad_size_id in ('888x30', '888x10')
        and    dc.ad_id != 203108986
        group  by dc.ad_id, nav.name, dc.affiliate_id,
                  dc.country_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_module_aggregate partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure replicate_to_ben
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
    -- dart_counts_new
    l_process_name := p_process_name || '-' ||
                      'dart_counts_new@etlben';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from bo.dart_counts_new where date_id = ' ||
                 p_process_date;
        --bo.pkg_generic_ben.sql_from_ods@etlben(l_sql);
        bo.pkg_generic_ddl.sql_from_ods@etlben(l_sql);
      end if;
      insert into bo.dart_counts_new@etlben
        (date_id, process_date_id, count_type, view_date,
         count_number, ad_id, site_id, country_id,
         state_province, dma_id, page_id, city_id,
         ad_size_request, view_month, view_year, data_source,
         agent_type, internal_ip, ad_group_id)
        select date_id, process_date_id, count_type,
               view_date, count_number, ad_id, site_id,
               country_id, state_province, dma_id, page_id,
               city_id, ad_size_request, view_month,
               view_year, data_source, agent_type,
               internal_ip, ad_group_id
        from   ben_dart_counts_new
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      bo.pkg_generic_ddl.analyze_partition@etlben('bo', 'dart_counts_new', 'P_' ||
                                                   p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dcm_current
    if upper(p_process_name) = 'PROCESS' then
      l_process_name := p_process_name || '-' ||
                        'dcm_current@etlben';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        /*bo.pkg_generic_ben.truncate_table@ben('BO', 'dcm_current');
        insert \*+ append *\
        into bo.dcm_current@ben
          (count_type, view_date, count_number, ad_id,
           site_id, country_id, state_province, dma_id,
           page_id, city_id, ad_size_request, view_month,
           view_year, view_date_id, days_counted,
           data_source, agent_type, internal_ip, ad_group_id)
          select count_type, view_date, count_number, ad_id,
                 site_id, country_id, state_province, dma_id,
                 page_id, city_id, ad_size_request,
                 view_month, view_year, view_date_id,
                 days_counted, data_source, agent_type,
                 internal_ip, ad_group_id
          from   ben_dcm_current;*/
        bo.pkg_generic_ben.load_dcm_current@etlben;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
    end if;
    -- fact_atac_atp_day
    l_process_name := p_process_name || '-' ||
                      'fact_atac_atp_day@etlben';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from dart_data.fact_atac_atp_day where view_date = to_date(' ||
                 p_process_date || ', ''yyyymmdd'')';
        --bo.pkg_generic_ben.sql_from_ods@etlben(l_sql);
        bo.pkg_generic_ddl.sql_from_ods@etlben(l_sql);
      end if;
      insert into dart_data.fact_atac_atp_day@etlben
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
      bo.pkg_generic_ddl.analyze_partition@etlben('dart_data', 'fact_atac_atp_day', 'P_' ||
                                                   p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- network_revenue_daily
    l_process_name := p_process_name || '-' ||
                      'network_revenue_daily@etlben';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from bo.network_revenue_daily where date_id = ' ||
                 p_process_date;
        --bo.pkg_generic_ben.sql_from_ods@etlben(l_sql);
        bo.pkg_generic_ddl.sql_from_ods@etlben(l_sql);
      end if;
      insert into bo.network_revenue_daily@etlben
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
      bo.pkg_generic_ddl.analyze_partition@etlben('bo', 'network_revenue_daily', 'P_' ||
                                                   p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- network_revenue_monthly
    l_process_name := p_process_name || '-' ||
                      'network_revenue_monthly@etlben';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      l_month_start := to_char(to_date(p_process_date, 'yyyymmdd'), 'yyyymm') || '01';
      l_sql         := 'delete from bo.network_revenue_monthly where date_id =' ||
                       l_month_start;
      --bo.pkg_generic_ben.sql_from_ods@etlben(l_sql);
      bo.pkg_generic_ddl.sql_from_ods@etlben(l_sql);
      insert into bo.network_revenue_monthly@etlben
        (view_date, date_id, process_date_id, max_date_id,
         impressions_delivered, clicks, ad_id, country,
         impression_type, affiliate_id, site_id, zone_id,
         content_partner_id, developer_id, module_id,
         data_id, data_source, agent_type, internal_ip,
         flags, atf_value, ad_group_id, total_inventory,
         total_debug)
        select /*+ PARALLEL (a,8,2) */
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
      bo.pkg_generic_ddl.analyze_partition@etlben('bo', 'network_revenue_monthly', 'P_' ||
                                                   p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    if upper(p_process_name) in ('PROCESS') then
      delete from bo.date_stamp@etlben
      where  view_date =
             to_date(p_process_date, 'yyyymmdd');
      insert into bo.date_stamp@etlben
        (view_date_id, view_date)
      values
        (p_run_id, to_date(p_process_date, 'yyyymmdd'));
      commit;
    end if;
  end;

  procedure replicate_to_www
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_sql          varchar2(30000);
  begin
    --- This needs to be removed when new insider is fully functional
    -- Insider Traffic: Add DATA_FOR_TRAFFIC_REPORTS@WWW (stop-gap for yield reporting)
    l_process_name := p_process_name || '-' ||
                      'data_for_traffic_reports@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        delete from insider_data.data_for_traffic_reports@etlwww
        where  date_id = p_process_date;
      end if;
      insert into insider_data.data_for_traffic_reports@etlwww
        (affiliate_id, date_id, impressions_delivered,
         clicks, view_date, marketplace_zone, ad_category,
         ad_dimension, ad_id, ad_rate, impression_type)
        select /*+ parallel(nrd,4,1) */
         nrd.affiliate_id, nrd.date_id,
         sum(nrd.impressions_delivered) impressions_delivered,
         sum(nrd.clicks) clicks, nrd.view_date, null,
         nae.category, nae.ad_size_id, nae.ad_id, nae.rate,
         nrd.impression_type
        from   ben_network_revenue_daily nrd,
               ods_metadata.adm_ads nae
        where  date_id = p_process_date
        and    country = 'WW'
        and    nrd.ad_id = nae.ad_id
        group  by nrd.affiliate_id, nrd.date_id,
                  nrd.view_date, nae.category,
                  nae.ad_size_id, nae.ad_id, nae.rate,
                  nrd.impression_type;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_partition@etlwww('insider_data', 'data_for_traffic_reports', 'P_' ||
                                                             p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- glamcontent_aggregate
    l_process_name := p_process_name || '-' ||
                      'glamcontent_aggregate@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from insider_data.glamcontent_aggregate@etlwww
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
      end if;
      insert into insider_data.glamcontent_aggregate@etlwww
        (view_date, cid, did, mid, affiliate_id,
         video_views, adjacent_ads, content_type, date_id,
         process_date_id, created_date)
        select view_date, cid, did, mid, affiliate_id,
               video_views, adjacent_ads, content_type,
               date_id, process_date_id, created_date
        from   ben_glamcontent_aggregate
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_partition@etlwww('insider_data', 'glamcontent_aggregate', 'P_' ||
                                                             p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- video_play_data
    l_process_name := p_process_name || '-' ||
                      'video_play_data@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from insider_data.video_play_data@etlwww
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
      end if;
      insert into insider_data.video_play_data@etlwww
        (view_date, cid, did, video_views, adjacent_ads,
         pre_roll, over, post_roll, date_id, process_date_id,
         created_date, data_source)
        select view_date, cid, did, video_views,
               adjacent_ads, pre_roll, over, post_roll,
               date_id, process_date_id, created_date,
               data_source
        from   ben_video_play_data
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_partition@etlwww('insider_data', 'video_play_data', 'P_' ||
                                                             p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- module_aggregate
    l_process_name := p_process_name || '-' ||
                      'module_aggregate@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if upper(p_process_name) in ('REPROCESS', 'MANUAL') then
        l_sql := 'delete from insider_data.module_aggregate@etlwww
                  where  date_id = :p_process_date';
        execute immediate l_sql
          using p_process_date;
      end if;
      insert into insider_data.module_aggregate@etlwww
        (view_date, mid, ad_name, affiliate_id, country_id,
         impressions, clicks, date_id, process_date_id,
         created_date)
        select view_date, mid, ad_name, affiliate_id,
               country_id, impressions, clicks, date_id,
               process_date_id, created_date
        from   ben_module_aggregate
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_partition@etlwww('insider_data', 'module_aggregate', 'P_' ||
                                                             p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

end;
/
