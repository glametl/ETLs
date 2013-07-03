create or replace package pkg_ignite_data as

  procedure run_ignite
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_ignite_metadata
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ignite_ad_aff_daily_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ignite_ad_aff_monthly
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ignite_total_imps
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_social_metrics
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ignite_total_metrics
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ignite_30day_imps_metrics
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_social_engagement
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure replicate_to_nap
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_ignite_data as

  procedure run_ignite
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
    l_start_process := l_process_name || '-run_ignite';
    l_mail_msg      := 'Ignite Process Data';
    l_to            := 'nileshm@glam.com,sahilt@glam.com';
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
    load_ignite_metadata(l_run_id, l_process_date, l_process_name);
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
        load_ignite_ad_aff_daily_agg(l_run_id, i.date_id, l_process_name);
        l_max_date := i.date_id;
      end loop;
      load_ignite_ad_aff_monthly(l_run_id, l_max_date, l_process_name);
    else
      load_ignite_ad_aff_daily_agg(l_run_id, l_process_date, l_process_name);
      load_ignite_ad_aff_monthly(l_run_id, l_process_date, l_process_name);
    end if;
    load_ignite_total_imps(l_run_id, l_process_date, l_process_name);
    load_social_metrics(l_run_id, l_process_date, l_process_name);
    load_ignite_total_metrics(l_run_id, l_process_date, l_process_name);
    load_ignite_30day_imps_metrics(l_run_id, l_process_date, l_process_name);
    if upper(p_process_name) = 'PROCESS' then
      replicate_to_nap(l_run_id, l_process_date, l_process_name);
    end if;
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

  procedure load_ignite_metadata
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ignite_metadata';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table ignite_metadata';
      insert /*+ append */
      into ignite_metadata
        (imps_type, ignite_campaign_id, advertiser_id,
         ad_id, author_id, posts_id, start_date, end_date,
         start_date_last_day, start_month,
         end_date_first_day, end_month, posts_group_id)
      /*with advertiser as
       (select 'advertiser_imps' as imps_type,
               advertiser_id,
               min(campaign_live_date - 5) start_date_id,
               max(least(coalesce(campaign_close_date + 30, sysdate - 1), sysdate - 1)) end_date_id
        from   ignite_data.gi_campaign@etlnap
        group  by advertiser_id),
      campaign as
       (select 'campaign_imps' as imps_type,
               ignite_campaign_id,
               min(creation_date) as min_creation_date
        from   ignite_data.gi_posts@etlnap
        group  by ignite_campaign_id),
      post as
       (select 'post_imps' as imps_type, ignite_campaign_id,
               (campaign_live_date - 5) start_date_id,
               least(coalesce(campaign_close_date + 30, sysdate - 1), sysdate - 1) end_date_id
        from   ignite_data.gi_campaign@etlnap)
      select imps_type, b.ignite_campaign_id,
             a.advertiser_id, ad_id, author_id, -11,
             to_number(to_char(start_date_id, 'yyyymmdd')) as start_date,
             to_number(to_char(end_date_id, 'yyyymmdd')) as end_date,
             to_number(to_char(last_day(start_date_id), 'yyyymmdd')) as start_date_last_day,
             to_number(to_char(start_date_id, 'yyyymm')) as start_month,
             to_number(to_char(trunc(end_date_id, 'mm'), 'yyyymmdd')) as end_date_first_day,
             to_number(to_char(end_date_id, 'yyyymm')) as end_month,
             -11
      from   advertiser a,
             ignite_data.gi_campaign@etlnap b,
             ignite_data.gi_posts@etlnap c,
             ignite_data.gi_posts_group@etlnap d
      where  a.advertiser_id = b.advertiser_id
      and    b.ignite_campaign_id = c.ignite_campaign_id
      and    b.ignite_campaign_id = d.ignite_campaign_id
      and    c.posts_group_id = d.posts_group_id
      union all
      select imps_type, a.ignite_campaign_id, -11, ad_id,
             author_id, -11,
             to_number(to_char(min_creation_date, 'yyyymmdd')) as start_date,
             -11 as end_date,
             to_number(to_char(last_day(min_creation_date), 'yyyymmdd')) as start_date_last_day,
             to_number(to_char(min_creation_date, 'yyyymm')) as start_month,
             -11 as end_date_first_day, -11 as end_month,
             b.posts_group_id
      from   campaign a, ignite_data.gi_posts@etlnap b,
             ignite_data.gi_posts_group@etlnap c
      where  a.ignite_campaign_id = b.ignite_campaign_id
      and    a.ignite_campaign_id = c.ignite_campaign_id
      and    b.posts_group_id = c.posts_group_id
      union all
      select imps_type, a.ignite_campaign_id, -11, ad_id,
             author_id, posts_id,
             to_number(to_char(start_date_id, 'yyyymmdd')) as start_date,
             to_number(to_char(end_date_id, 'yyyymmdd')) as end_date,
             to_number(to_char(last_day(start_date_id), 'yyyymmdd')) as start_date_last_day,
             to_number(to_char(start_date_id, 'yyyymm')) as start_month,
             to_number(to_char(trunc(end_date_id, 'mm'), 'yyyymmdd')) as end_date_first_day,
             to_number(to_char(end_date_id, 'yyyymm')) as end_month,
             b.posts_group_id
      from   post a, ignite_data.gi_posts@etlnap b,
             ignite_data.gi_posts_group@etlnap c
      where  a.ignite_campaign_id = b.ignite_campaign_id
      and    a.ignite_campaign_id = c.ignite_campaign_id
      and    b.posts_group_id = c.posts_group_id;*/
        with max_run_ignite as
         (select to_date(max(process_date), 'yyyymmdd') + 10 as max_date
          from   daily_process_log
          where  process_name like '%run_ignite%'),
        advertiser as
         (select 'advertiser_imps' as imps_type,
                 advertiser_id,
                 min(campaign_live_date - 5) start_date_id,
                 max(least(coalesce(campaign_close_date + 30, sysdate - 1), sysdate - 1)) end_date_id
          from   ignite_data.gi_campaign@etlnap
          --where  advertiser_id = 50002527
          group  by advertiser_id),
        campaign as
         (select 'campaign_imps' as imps_type,
                 ignite_campaign_id,
                 min(creation_date) as min_creation_date
          from   ignite_data.gi_posts@etlnap
          group  by ignite_campaign_id),
        post as
         (select 'post_imps' as imps_type, ignite_campaign_id,
                 (campaign_live_date - 5) start_date_id,
                 least(coalesce(campaign_close_date + 30, sysdate - 1), sysdate - 1) end_date_id
          from   ignite_data.gi_campaign@etlnap),
        advertiser_new as
         (select imps_type, advertiser_id,
                 to_number(to_char(start_date_id, 'yyyymmdd')) as start_date,
                 case
                   when trunc(end_date_id, 'dd') -
                        trunc(start_date_id, 'dd') < 61 then
                    to_number(to_char(max_date, 'yyyymmdd'))
                   else
                    to_number(to_char(end_date_id, 'yyyymmdd'))
                 end as end_date,
                 case
                   when trunc(end_date_id, 'dd') -
                        trunc(start_date_id, 'dd') < 61 then
                    to_number(to_char(end_date_id, 'yyyymmdd'))
                   else
                    to_number(to_char(last_day(start_date_id), 'yyyymmdd'))
                 end as start_date_last_day,
                 case
                   when trunc(end_date_id, 'dd') -
                        trunc(start_date_id, 'dd') < 61 then
                    to_number(to_char(max_date, 'yyyymm'))
                   else
                    to_number(to_char(start_date_id, 'yyyymm'))
                 end as start_month,
                 case
                   when trunc(end_date_id, 'dd') -
                        trunc(start_date_id, 'dd') < 61 then
                    to_number(to_char(trunc(max_date, 'mm'), 'yyyymmdd'))
                   else
                    to_number(to_char(trunc(end_date_id, 'mm'), 'yyyymmdd'))
                 end as end_date_first_day,
                 case
                   when trunc(end_date_id, 'dd') -
                        trunc(start_date_id, 'dd') < 61 then
                    to_number(to_char(max_date, 'yyyymm'))
                   else
                    to_number(to_char(end_date_id, 'yyyymm'))
                 end as end_month
          from   advertiser, max_run_ignite),
        campaign_new as
         (select imps_type, ignite_campaign_id,
                 to_number(to_char(min_creation_date, 'yyyymmdd')) as start_date,
                 -11 as end_date,
                 to_number(to_char(last_day(min_creation_date), 'yyyymmdd')) as start_date_last_day,
                 to_number(to_char(min_creation_date, 'yyyymm')) as start_month,
                 -11 as end_date_first_day, -11 as end_month
          from   campaign),
        post_new as
         (select imps_type, ignite_campaign_id,
                 to_number(to_char(start_date_id, 'yyyymmdd')) as start_date,
                 case
                   when trunc(end_date_id, 'dd') -
                        trunc(start_date_id, 'dd') < 61 then
                    to_number(to_char(max_date, 'yyyymmdd'))
                   else
                    to_number(to_char(end_date_id, 'yyyymmdd'))
                 end as end_date,
                 case
                   when trunc(end_date_id, 'dd') -
                        trunc(start_date_id, 'dd') < 61 then
                    to_number(to_char(end_date_id, 'yyyymmdd'))
                   else
                    to_number(to_char(last_day(start_date_id), 'yyyymmdd'))
                 end as start_date_last_day,
                 case
                   when trunc(end_date_id, 'dd') -
                        trunc(start_date_id, 'dd') < 61 then
                    to_number(to_char(max_date, 'yyyymm'))
                   else
                    to_number(to_char(start_date_id, 'yyyymm'))
                 end as start_month,
                 case
                   when trunc(end_date_id, 'dd') -
                        trunc(start_date_id, 'dd') < 61 then
                    to_number(to_char(trunc(max_date, 'mm'), 'yyyymmdd'))
                   else
                    to_number(to_char(trunc(end_date_id, 'mm'), 'yyyymmdd'))
                 end as end_date_first_day,
                 case
                   when trunc(end_date_id, 'dd') -
                        trunc(start_date_id, 'dd') < 61 then
                    to_number(to_char(max_date, 'yyyymm'))
                   else
                    to_number(to_char(end_date_id, 'yyyymm'))
                 end as end_month
          from   post, max_run_ignite)
        select distinct imps_type, b.ignite_campaign_id,
                        a.advertiser_id, ad_id, author_id,
                        -11, start_date, end_date,
                        start_date_last_day, start_month,
                        end_date_first_day, end_month, -11
        from   advertiser_new a,
               ignite_data.gi_campaign@etlnap b,
               ignite_data.gi_posts@etlnap c,
               ignite_data.gi_posts_group@etlnap d
        where  a.advertiser_id = b.advertiser_id
        and    b.ignite_campaign_id = c.ignite_campaign_id
        and    b.ignite_campaign_id = d.ignite_campaign_id
        and    c.posts_group_id = d.posts_group_id
        union all
        select imps_type, a.ignite_campaign_id, -11, ad_id,
               author_id, -11, start_date, end_date,
               start_date_last_day, start_month,
               end_date_first_day, end_month,
               b.posts_group_id
        from   campaign_new a, ignite_data.gi_posts@etlnap b,
               ignite_data.gi_posts_group@etlnap c
        where  a.ignite_campaign_id = b.ignite_campaign_id
        and    a.ignite_campaign_id = c.ignite_campaign_id
        and    b.posts_group_id = c.posts_group_id
        union all
        select imps_type, a.ignite_campaign_id, -11, ad_id,
               author_id, posts_id, start_date, end_date,
               start_date_last_day, start_month,
               end_date_first_day, end_month,
               b.posts_group_id
        from   post_new a, ignite_data.gi_posts@etlnap b,
               ignite_data.gi_posts_group@etlnap c
        where  a.ignite_campaign_id = b.ignite_campaign_id
        and    a.ignite_campaign_id = c.ignite_campaign_id
        and    b.posts_group_id = c.posts_group_id
        -- this is temparory fix for social metrics as we are not doing with the imps approach
        union all
        select distinct 'advertiser_metrics' as imps_type,
                        b.ignite_campaign_id,
                        a.advertiser_id, null as ad_id,
                        null as author_id,
                        --posts_id as post_id,
                        null as post_id,
                        to_number(to_char(start_date_id, 'yyyymmdd')) as start_date,
                        to_number(to_char(end_date_id, 'yyyymmdd')) as end_date,
                        null as start_date_last_day,
                        null as start_month,
                        null as end_date_first_day,
                        null as end_month, -11
                        --c.posts_group_id
        from   advertiser a,
               ignite_data.gi_campaign@etlnap b,
               ignite_data.gi_posts@etlnap c,
               ignite_data.gi_posts_group@etlnap d
        where  a.advertiser_id = b.advertiser_id
        and    b.ignite_campaign_id = c.ignite_campaign_id
        and    b.ignite_campaign_id = d.ignite_campaign_id
        and    c.posts_group_id = d.posts_group_id
        union all
        select 'post_metrics' imps_type,
               a.ignite_campaign_id, null as advertiser_id,
               null as ad_id, author_id, posts_id,
               to_number(to_char(start_date_id, 'yyyymmdd')) as start_date,
               to_number(to_char(end_date_id, 'yyyymmdd')) as end_date,
               null as start_date_last_day,
               null as start_month,
               null as end_date_first_day, null as end_month,
               b.posts_group_id
        from   post a, ignite_data.gi_posts@etlnap b,
               ignite_data.gi_posts_group@etlnap c
        where  a.ignite_campaign_id = b.ignite_campaign_id
        and    a.ignite_campaign_id = c.ignite_campaign_id
        and    b.posts_group_id = c.posts_group_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ignite_metadata estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --
    l_process_name := p_process_name || '-' ||
                      'ignite_metadata_30';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table ignite_metadata_30';
      insert /*+ append */
      into ignite_metadata_30
        (advertiser_id, ignite_campaign_id, ad_id,
         posts_group_id, close_30_start_date_id,
         close_30_end_date_id, first_30_start_date_id,
         first_30_end_date_id, current_30_start_date_id,
         current_30_end_date_id, author_id, posts_id,
         imps_type)
        select advertiser_id, a.ignite_campaign_id, ad_id,
               posts_group_id,
               to_number(to_char(greatest(campaign_live_date - 5, least(coalesce(least(coalesce(campaign_close_date, sysdate - 1), sysdate - 1) - 30, campaign_close_date - 30))), 'yyyymmdd')) close_30_start_date_id,
               to_number(to_char(least(coalesce(campaign_close_date, sysdate - 1), sysdate - 1), 'yyyymmdd')) close_30_end_date_id,
               to_number(to_char(campaign_live_date, 'yyyymmdd')) first_30_start_date_id,
               to_number(to_char(campaign_live_date + 30, 'yyyymmdd')) first_30_end_date_id,
               to_number(to_char(add_months(sysdate, -1), 'yyyymmdd')) current_30_start_date_id,
               to_number(to_char(sysdate, 'yyyymmdd')) current_30_start_date_id,
               '', '', ''
        from   ignite_data.gi_campaign@etlnap a,
               ignite_data.gi_posts_group@etlnap b
        where  a.ignite_campaign_id = b.ignite_campaign_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ignite_metadata_30 estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ignite_ad_aff_daily_agg
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
                      'ignite_ad_aff_daily_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if to_char(to_date(p_process_date, 'yyyymmdd'), 'dd') = '01' then
        l_previous_dt := '';
      else
        l_previous_dt := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd'));
      end if;
      execute immediate 'alter table ignite_ad_aff_daily_agg truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ignite_ad_aff_daily_agg
        (date_id, month_id, ad_id, affiliate_id,
         advertiser_id, order_id, total_impressions,
         total_clicks)
        select p_process_date, month_id, ad_id, affiliate_id,
               advertiser_id, order_id,
               sum(total_impressions), sum(total_clicks)
        from   (select /*+ parallel(a,4,1) */
                  month_id, ad_id, affiliate_id, advertiser_id,
                  order_id, total_impressions, total_clicks
                 from   ignite_ad_aff_daily_agg a
                 where  date_id = l_previous_dt
                 union all
                 select /*+ parallel(b,4,1) */
                  to_number(substr(date_id, 1, 6)) as month_id,
                  ad_id, affiliate_id, advertiser_id, order_id,
                  total_impressions, total_clicks
                 from   fact_adq_ad_aff_agg b
                 where  date_id = p_process_date)
        group  by month_id, ad_id, affiliate_id,
                  advertiser_id, order_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ignite_ad_aff_daily_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ignite_ad_aff_monthly
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ignite_ad_aff_monthly';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'alter table ignite_ad_aff_monthly truncate partition P_' ||
                        substr(p_process_date, 1, 6);
      insert /*+ append */
      into ignite_ad_aff_monthly
        (month_id, ad_id, affiliate_id, advertiser_id,
         order_id, total_impressions, total_clicks)
        select month_id, ad_id, affiliate_id, advertiser_id,
               order_id, total_impressions, total_clicks
        from   ignite_ad_aff_daily_agg a
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ignite_ad_aff_monthly partition(P_' ||
                        substr(p_process_date, 1, 6) ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ignite_total_imps
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) as
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ignite_total_impressions';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      l_process_name := p_process_name || '-' ||
                        'advertiser_impressions';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        execute immediate 'truncate table ignite_total_impressions';
        insert /*+ append */
        into ignite_total_impressions
          (date_id, advertiser_id, start_date, end_date,
           total_impressions, total_clicks, imps_type)
          with src as
           (select a.advertiser_id, start_date, end_date,
                   sum(total_impressions) total_impressions,
                   sum(total_clicks) total_clicks, imps_type
            from   fact_adq_ad_aff_agg a, ignite_metadata b
            where  date_id >= start_date
            and    date_id <= start_date_last_day
            and    a.advertiser_id = b.advertiser_id
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'advertiser_imps'
            group  by a.advertiser_id, start_date, end_date,
                      imps_type
            union all
            select a.advertiser_id, start_date, end_date,
                   sum(total_impressions), sum(total_clicks),
                   imps_type
            from   ignite_ad_aff_monthly a, ignite_metadata b
            where  month_id > start_month
            and    month_id < end_month
            and    a.advertiser_id = b.advertiser_id
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'advertiser_imps'
            group  by a.advertiser_id, start_date, end_date,
                      imps_type
            union all
            select a.advertiser_id, start_date, end_date,
                   sum(total_impressions), sum(total_clicks),
                   imps_type
            from   fact_adq_ad_aff_agg a, ignite_metadata b
            where  date_id >= end_date_first_day
            and    date_id <= end_date
            and    a.advertiser_id = b.advertiser_id
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'advertiser_imps'
            group  by a.advertiser_id, start_date, end_date,
                      imps_type)
          select p_process_date, advertiser_id, start_date,
                 end_date, sum(total_impressions),
                 sum(total_clicks), imps_type
          from   src
          group  by advertiser_id, start_date, end_date,
                    imps_type;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- 
      l_process_name := p_process_name || '-' ||
                        'campaign_impressions';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        -- no 'truncate table ignite_total_impressions' because it is used in previous step;
        insert /*+ append */
        into ignite_total_impressions
          (date_id, ignite_campaign_id, total_impressions,
           total_clicks, imps_type)
          with src as
           (select ignite_campaign_id,
                   sum(total_impressions) total_impressions,
                   sum(total_clicks) total_clicks, imps_type
            from   fact_adq_ad_aff_agg a, ignite_metadata b
            where  date_id >= start_date
            and    date_id <= start_date_last_day
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'campaign_imps'
            group  by ignite_campaign_id, imps_type
            union all
            select ignite_campaign_id, sum(total_impressions),
                   sum(total_clicks), imps_type
            from   ignite_ad_aff_monthly a, ignite_metadata b
            where  month_id > start_month
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'campaign_imps'
            group  by ignite_campaign_id, imps_type)
          select p_process_date, ignite_campaign_id,
                 sum(total_impressions), sum(total_clicks),
                 imps_type
          from   src
          group  by ignite_campaign_id, imps_type;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      --
      l_process_name := p_process_name || '-' ||
                        'post_impressions';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        -- no 'truncate table ignite_total_impressions' because it is used in previous step;
        insert /*+ append */
        into ignite_total_impressions
          (date_id, ignite_campaign_id, posts_id,
           total_impressions, total_clicks, imps_type)
          with src as
           (select ignite_campaign_id, posts_id,
                   sum(total_impressions) total_impressions,
                   sum(total_clicks) total_clicks, imps_type
            from   fact_adq_ad_aff_agg a, ignite_metadata b
            where  date_id >= start_date
            and    date_id <= start_date_last_day
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'post_imps'
            group  by ignite_campaign_id, posts_id, imps_type
            union all
            select ignite_campaign_id, posts_id,
                   sum(total_impressions), sum(total_clicks),
                   imps_type
            from   ignite_ad_aff_monthly a, ignite_metadata b
            where  month_id > start_month
                  --and    month_id < end_month
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'post_imps'
            group  by ignite_campaign_id, posts_id, imps_type
            /*union all
            select ignite_campaign_id, posts_id,
                   sum(total_impressions), sum(total_clicks),
                   imps_type
            from   fact_adq_ad_aff_agg a,
                   ignite_metadata b
            where  date_id >= end_date_first_day
            and    date_id <= end_date
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'post_imps'
            group  by ignite_campaign_id, posts_id, imps_type*/
            )
          select p_process_date, ignite_campaign_id,
                 posts_id, sum(total_impressions),
                 sum(total_clicks), imps_type
          from   src
          group  by ignite_campaign_id, posts_id, imps_type;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      --
      l_process_name := p_process_name || '-' ||
                        'affiliate_impressions';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        -- no 'truncate table ignite_total_impressions' because it is used in previous step;
        /*insert \*+ append *\
        into ignite_total_impressions
          (date_id, ignite_campaign_id, advertiser_id,
           affiliate_id, total_impressions, total_clicks,
           imps_type)
          select p_process_date, ignite_campaign_id,
                 a.advertiser_id, affiliate_id,
                 sum(total_impressions) total_impressions,
                 sum(total_clicks) total_clicks,
                 'affiliate_imps' imps_type
          from   fact_adq_ad_aff_agg a, ignite_metadata b
          where  date_id >=
                 to_char(add_months(sysdate, -1), 'yyyymmdd')
          and    a.advertiser_id = b.advertiser_id
          and    a.affiliate_id = b.author_id
          and    a.ad_id = b.ad_id
          and    imps_type = 'advertiser_imps'
          group  by ignite_campaign_id, a.advertiser_id,
                    affiliate_id;*/
        insert /*+ append */
        into ignite_total_impressions
          (date_id, ignite_campaign_id, affiliate_id,
           advertiser_id, total_impressions, total_clicks,
           imps_type)
          with src as
           (select ignite_campaign_id, affiliate_id,
                   a.advertiser_id,
                   sum(total_impressions) total_impressions,
                   sum(total_clicks) total_clicks
            from   fact_adq_ad_aff_agg a, ignite_metadata b
            where  date_id >= start_date
            and    date_id <= start_date_last_day
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'post_imps'
            group  by ignite_campaign_id, affiliate_id,
                      a.advertiser_id
            union all
            select ignite_campaign_id, affiliate_id,
                   a.advertiser_id, sum(total_impressions),
                   sum(total_clicks)
            from   ignite_ad_aff_monthly a, ignite_metadata b
            where  month_id > start_month
            and    month_id < end_month
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'post_imps'
            group  by ignite_campaign_id, affiliate_id,
                      a.advertiser_id
            union all
            select ignite_campaign_id, affiliate_id,
                   a.advertiser_id, sum(total_impressions),
                   sum(total_clicks)
            from   fact_adq_ad_aff_agg a, ignite_metadata b
            where  date_id >= end_date_first_day
            and    date_id <= end_date
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'post_imps'
            group  by ignite_campaign_id, affiliate_id,
                      a.advertiser_id)
          select p_process_date, ignite_campaign_id,
                 affiliate_id, advertiser_id,
                 sum(total_impressions), sum(total_clicks),
                 'affiliate_imps' as imps_type
          from   src
          group  by ignite_campaign_id, affiliate_id,
                    advertiser_id;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- 
      l_process_name := p_process_name || '-' ||
                        'post_group_impressions';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        -- no 'truncate table ignite_total_impressions' because it is used in previous step;
        /*insert \*+ append *\
        into ignite_total_impressions
          (date_id, ignite_campaign_id, posts_group_id,
           total_impressions, total_clicks, imps_type)
          with src as
           (select ignite_campaign_id, posts_group_id,
                   sum(total_impressions) total_impressions,
                   sum(total_clicks) total_clicks
            from   fact_adq_ad_aff_agg a, ignite_metadata b
            where  date_id >= start_date
            and    date_id <= start_date_last_day
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'campaign_imps'
            group  by ignite_campaign_id, posts_group_id,
                      imps_type
            union all
            select ignite_campaign_id, posts_group_id,
                   sum(total_impressions), sum(total_clicks)
            from   ignite_ad_aff_monthly a, ignite_metadata b
            where  month_id > start_month
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'campaign_imps'
            group  by ignite_campaign_id, posts_group_id,
                      imps_type)
          select p_process_date, ignite_campaign_id,
                 posts_group_id, sum(total_impressions),
                 sum(total_clicks),
                 'post_groups_imps' as imps_type
          from   src
          group  by ignite_campaign_id, posts_group_id;*/
        insert /*+ append */
        into ignite_total_impressions
          (date_id, ignite_campaign_id, posts_group_id,
           posts_id, total_impressions, total_clicks,
           imps_type)
          with src as
           (select ignite_campaign_id, posts_group_id,
                   posts_id,
                   sum(total_impressions) total_impressions,
                   sum(total_clicks) total_clicks
            from   fact_adq_ad_aff_agg a, ignite_metadata b
            where  date_id >= start_date
            and    date_id <= start_date_last_day
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'post_imps'
            group  by ignite_campaign_id, posts_group_id,
                      posts_id, imps_type
            union all
            select ignite_campaign_id, posts_group_id,
                   posts_id, sum(total_impressions),
                   sum(total_clicks)
            from   ignite_ad_aff_monthly a, ignite_metadata b
            where  month_id > start_month
            and    a.affiliate_id = b.author_id
            and    a.ad_id = b.ad_id
            and    imps_type = 'post_imps'
            group  by ignite_campaign_id, posts_group_id,
                      posts_id, imps_type)
          select p_process_date, ignite_campaign_id,
                 posts_group_id, posts_id,
                 sum(total_impressions), sum(total_clicks),
                 'post_groups_imps' as imps_type
          from   src
          group  by ignite_campaign_id, posts_group_id,
                    posts_id;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      l_process_name := p_process_name || '-' ||
                        'ignite_total_impressions';
      execute immediate 'analyze table ignite_total_impressions estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    if upper(p_process_name) = 'PROCESS' then
      execute immediate 'truncate table ignite_total_imps_verify';
      insert /*+ append */
      into ignite_total_imps_verify
        (date_id, ignite_campaign_id, start_date, end_date,
         ad_id, affiliate_id, advertiser_id,
         total_impressions, total_clicks, posts_id,
         posts_group_id, imps_type)
        select date_id, ignite_campaign_id, start_date,
               end_date, ad_id, affiliate_id, advertiser_id,
               total_impressions, total_clicks, posts_id,
               posts_group_id, imps_type
        from   ignite_total_impressions;
      commit;
    end if;
  end;

  procedure load_social_metrics
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) as
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ignite_social_metrics_daily';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      for i in (select table_name, partition_name
                from   user_tab_partitions
                where  table_name =
                       'IGNITE_SOCIAL_METRICS_DAILY'
                and    replace(partition_name, 'P_', '') >=
                       to_char(to_date(p_process_date, 'yyyymmdd') - 30, 'yyyymmdd')
                and    replace(partition_name, 'P_', '') <=
                       to_char(p_process_date)
                order  by partition_position) loop
        execute immediate 'Alter table ignite_social_metrics_daily truncate partition ' ||
                          i.partition_name;
      end loop;
      insert /*+ append */
      into ignite_social_metrics_daily
        (date_id, campaign_id, post_group_id, post_id,
         author_id, url, type, source, value, reach)
        select date_id, campaign_id, post_group_id, post_id,
               author_id, url, type, source,
               sum(coalesce(value, 0)),
               sum(coalesce(reach, 0))
        from   ignite_stats.social_metrics_aggr@etladq
        --  from   ignite_stats.tmp_social_metrics_aggr@etladq
        where  date_id >=
               to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 30, 'yyyymmdd'))
        and    date_id <= p_process_date
        group  by date_id, campaign_id, post_group_id,
                  post_id, author_id, url, type, source;
      l_cnt := sql%rowcount;
      commit;
      for i in (select table_name, partition_name
                from   user_tab_partitions
                where  table_name =
                       'IGNITE_SOCIAL_METRICS_DAILY'
                and    replace(partition_name, 'P_', '') >=
                       to_char(to_date(p_process_date, 'yyyymmdd') - 30, 'yyyymmdd')
                and    replace(partition_name, 'P_', '') <=
                       to_char(p_process_date)
                order  by partition_position) loop
        execute immediate 'analyze table ignite_social_metrics_daily partition(' ||
                          i.partition_name ||
                          ') estimate statistics';
      end loop;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ignite_total_metrics
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) as
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ignite_total_metrics';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      l_process_name := p_process_name || '-' ||
                        'ignite_advertiser_metrics';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        execute immediate 'truncate table ignite_total_metrics ';
        /*insert \*+ append *\
        into ignite_total_metrics
          (date_id, advertiser_id, start_date, end_date,
           type, val, ct_reach, imps_type)
          select p_process_date, advertiser_id, start_date,
                 end_date, type, sum(value) val,
                 sum(coalesce(reach, 0)) ct_reach,
                 'advertiser_metrics' as imps_type
          from   ignite_social_metrics_daily a,
                 (select distinct advertiser_id,
                                   ignite_campaign_id,
                                   start_date, end_date,
                                   imps_type
                   from   ignite_metadata
                   where  imps_type = 'advertiser_imps') b
          where  date_id >= start_date
          and    date_id <= end_date
          and    campaign_id = ignite_campaign_id
          and    imps_type = 'advertiser_imps'
          group  by advertiser_id, start_date, end_date,
                    type;*/
        insert /*+ append */
        into ignite_total_metrics
          (date_id, advertiser_id, /*ignite_campaign_id,*/
           start_date, end_date, type, val, ct_reach,
           imps_type)
          select p_process_date, advertiser_id,
                 /*ignite_campaign_id,*/ start_date, end_date,
                 type, sum(value) val,
                 sum(coalesce(reach, 0)) ct_reach,
                 'advertiser_metrics' as imps_type
          from   ignite_social_metrics_daily a,
                 ignite_metadata b
          where  date_id >= start_date
          and    date_id <= end_date
          and    campaign_id = ignite_campaign_id
          --and    post_id = posts_id
          and    imps_type = 'advertiser_metrics'
          group  by advertiser_id, /*ignite_campaign_id,*/
                    start_date, end_date, type;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      ---
      l_process_name := p_process_name || '-' ||
                        'ignite_url_metrics';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        -- no 'truncate table ignite_total_metrics' because it is used in previous step;
        /*insert \*+ append *\
        into ignite_total_metrics
          (date_id, ignite_campaign_id, start_date,
           end_date, posts_group_id, posts_id, author_id,
           url, type, val, ct_reach, imps_type)
          select p_process_date, ignite_campaign_id,
                 start_date, end_date, a.post_group_id,
                 a.post_id, a.author_id, url, type,
                 sum(value) val,
                 sum(coalesce(reach, 0)) ct_reach,
                 'url_metrics' as imps_type
          from   ignite_social_metrics_daily a,
                 ignite_metadata b
          where  date_id >= start_date
          and    date_id <= end_date
          and    a.campaign_id = ignite_campaign_id
          and    a.post_id = b.posts_id
          and    a.post_group_id = b.posts_group_id
          and    a.author_id = b.author_id
          and    imps_type = 'post_imps'
          group  by ignite_campaign_id, start_date, end_date,
                    a.post_group_id, a.post_id, a.author_id,
                    url, type;*/
        insert /*+ append */
        into ignite_total_metrics
          (date_id, ignite_campaign_id, start_date,
           end_date, posts_group_id, posts_id, author_id,
           url, type, val, ct_reach, imps_type)
          select p_process_date, ignite_campaign_id,
                 start_date, end_date, a.post_group_id,
                 a.post_id, a.author_id, url, type,
                 sum(value) val,
                 sum(coalesce(reach, 0)) ct_reach,
                 'url_metrics' as imps_type
          from   ignite_social_metrics_daily a,
                 ignite_metadata b
          where  date_id >= start_date
          and    date_id <= end_date
          and    a.campaign_id = ignite_campaign_id
          and    a.post_id = b.posts_id
          and    a.post_group_id = b.posts_group_id
          and    a.author_id = b.author_id
          and    imps_type = 'post_metrics'
          group  by ignite_campaign_id, start_date, end_date,
                    a.post_group_id, a.post_id, a.author_id,
                    url, type;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      ---special provision for twitter_hashtag as there is no real post_id and author_id
      -- so we need to ignore that from joins
      l_process_name := p_process_name || '-' ||
                        'ignite_url_metrics_twitter_hashtag';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        -- no 'truncate table ignite_total_metrics' because it is used in previous step;
        insert /*+ append */
        into ignite_total_metrics
          (date_id, ignite_campaign_id, start_date,
           end_date, posts_group_id, posts_id, author_id,
           url, type, val, ct_reach, imps_type)
          select p_process_date, ignite_campaign_id,
                 start_date, end_date, a.post_group_id,
                 a.post_id, a.author_id, url, type,
                 sum(value) val,
                 sum(coalesce(reach, 0)) ct_reach,
                 'url_metrics' as imps_type
          from   ignite_social_metrics_daily a,
                 (select distinct start_date, end_date,
                                   ignite_campaign_id,
                                   posts_group_id, imps_type
                   from   ignite_metadata
                   where  imps_type = 'post_metrics') b
          where  date_id >= start_date
          and    date_id <= end_date
          and    a.campaign_id = ignite_campaign_id
                --and    a.post_id = b.posts_id
          and    a.post_group_id = b.posts_group_id
                --and    a.author_id = b.author_id
          and    imps_type = 'post_metrics'
                --- only select type = twitter_hashtag records as others are already inserted
          and    type = 'twitter_hashtag'
          group  by ignite_campaign_id, start_date, end_date,
                    a.post_group_id, a.post_id, a.author_id,
                    url, type;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      ---
      l_process_name := p_process_name || '-' ||
                        'ignite_campaign_metrics';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        -- no 'truncate table ignite_total_metrics' because it is used in previous step;
        insert /*+ append */
        into ignite_total_metrics
          (date_id, ignite_campaign_id, start_date,
           end_date, type, val, ct_reach, imps_type)
          select date_id, ignite_campaign_id, start_date,
                 end_date, type, sum(val) val,
                 sum(ct_reach) ct_reach,
                 'campaign_metrics' as imps_type
          from   ignite_total_metrics
          where  imps_type = 'url_metrics'
          group  by date_id, ignite_campaign_id, start_date,
                    end_date, type;
        l_cnt := sql%rowcount;
        commit;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      l_process_name := p_process_name || '-' ||
                        'ignite_total_metrics';
      execute immediate 'analyze table ignite_total_metrics estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', 0);
    end if;
  end;

  procedure load_ignite_30day_imps_metrics
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) as
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ignite_total_last_30_imps';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table ignite_total_30day_imps ';
      insert /*+ append */
      into ignite_total_30day_imps
        (date_id, advertiser_id, ignite_campaign_id,
         affiliate_id, posts_group_id, total_impressions,
         total_clicks, imps_type, process_date)
        select date_id, a.advertiser_id, ignite_campaign_id,
               affiliate_id, posts_group_id,
               sum(total_impressions) total_impressions,
               sum(total_clicks) total_clicks,
               'last_30_imps' as imps_type, p_process_date
        from   fact_adq_ad_aff_agg a, ignite_metadata_30 b
        where  date_id >= close_30_start_date_id
        and    date_id <= close_30_end_date_id
        and    a.advertiser_id = b.advertiser_id
        and    a.ad_id = b.ad_id
        group  by date_id, a.advertiser_id,
                  ignite_campaign_id, affiliate_id,
                  posts_group_id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ---
    l_process_name := p_process_name || '-' ||
                      'ignite_total_first_30_imps';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      -- no 'truncate table ignite_total_30day_imps' because it is used in previous step;
      insert /*+ append */
      into ignite_total_30day_imps
        (date_id, advertiser_id, ignite_campaign_id,
         affiliate_id, posts_group_id, total_impressions,
         total_clicks, imps_type, process_date)
        select date_id, a.advertiser_id, ignite_campaign_id,
               affiliate_id, posts_group_id,
               sum(total_impressions) total_impressions,
               sum(total_clicks) total_clicks,
               'first_30_imps' as imps_type, p_process_date
        from   fact_adq_ad_aff_agg a, ignite_metadata_30 b
        where  date_id >= first_30_start_date_id
        and    date_id <= first_30_end_date_id
        and    a.advertiser_id = b.advertiser_id
        and    a.ad_id = b.ad_id
        group  by date_id, a.advertiser_id,
                  ignite_campaign_id, affiliate_id,
                  posts_group_id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    l_process_name := p_process_name || '-' ||
                      'ignite_total_current_30_imps';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      -- no 'truncate table ignite_total_30day_imps' because it is used in previous step;
      insert /*+ append */
      into ignite_total_30day_imps
        (date_id, advertiser_id, ignite_campaign_id,
         affiliate_id, posts_group_id, total_impressions,
         total_clicks, imps_type, process_date)
        select date_id, a.advertiser_id, ignite_campaign_id,
               affiliate_id, posts_group_id,
               sum(total_impressions) total_impressions,
               sum(total_clicks) total_clicks,
               'current_30_imps' as imps_type,
               p_process_date
        from   fact_adq_ad_aff_agg a, ignite_metadata_30 b
        where  date_id >= current_30_start_date_id
        and    date_id <= current_30_end_date_id
        and    a.advertiser_id = b.advertiser_id
        and    a.ad_id = b.ad_id
        group  by date_id, a.advertiser_id,
                  ignite_campaign_id, affiliate_id,
                  posts_group_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ignite_total_30day_imps estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ----metrics agg
    l_process_name := p_process_name || '-' ||
                      'ignite_total_last_30_metrics';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table IGNITE_total_30day_metrics ';
      insert /*+ append */
      into ignite_total_30day_metrics
        (date_id, ignite_campaign_id, affiliate_id,
         posts_group_id, post_id, engage, reach, imps_type,
         process_date, advertiser_id)
        select date_id, ignite_campaign_id, a.author_id,
               posts_group_id, post_id, sum(value) engage,
               sum(reach) reach,
               'last_30_metrics' as imps_type,
               p_process_date, advertiser_id
        from   ignite_social_metrics_daily a,
               ignite_metadata_30 b
        where  date_id >= close_30_start_date_id
        and    date_id <= close_30_end_date_id
        and    a.campaign_id = b.ignite_campaign_id
        and    a.post_group_id = b.posts_group_id
        and    type not in ('facebook_total_count')
        group  by date_id, ignite_campaign_id, a.author_id,
                  posts_group_id, post_id, advertiser_id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ---
    l_process_name := p_process_name || '-' ||
                      'ignite_total_first_30_metrics';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      -- no 'truncate table ignite_total_30day_imps' because it is used in previous step;
      insert /*+ append */
      into ignite_total_30day_metrics
        (date_id, ignite_campaign_id, affiliate_id,
         posts_group_id, post_id, engage, reach, imps_type,
         process_date, advertiser_id)
        select date_id, ignite_campaign_id, a.author_id,
               posts_group_id, post_id, sum(value) engage,
               sum(reach) reach,
               'first_30_metrics' as imps_type,
               p_process_date, advertiser_id
        from   ignite_social_metrics_daily a,
               ignite_metadata_30 b
        where  date_id >= first_30_start_date_id
        and    date_id <= first_30_end_date_id
        and    a.campaign_id = b.ignite_campaign_id
        and    a.post_group_id = b.posts_group_id
        and    type not in ('facebook_total_count')
        group  by date_id, ignite_campaign_id, a.author_id,
                  posts_group_id, post_id, advertiser_id;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    l_process_name := p_process_name || '-' ||
                      'ignite_total_current_30_metrics';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      -- no 'truncate table ignite_total_30day_imps' because it is used in previous step;
      insert /*+ append */
      into ignite_total_30day_metrics
        (date_id, ignite_campaign_id, affiliate_id,
         posts_group_id, post_id, engage, reach, imps_type,
         process_date, advertiser_id)
        select date_id, ignite_campaign_id, a.author_id,
               posts_group_id, post_id, sum(value) engage,
               sum(reach) reach,
               'current_30_metrics' as imps_type,
               p_process_date, advertiser_id
        from   ignite_social_metrics_daily a,
               ignite_metadata_30 b
        where  date_id >= current_30_start_date_id
        and    date_id <= current_30_end_date_id
        and    a.campaign_id = b.ignite_campaign_id
        and    a.post_group_id = b.posts_group_id
        and    type not in ('facebook_total_count')
        group  by date_id, ignite_campaign_id, a.author_id,
                  posts_group_id, post_id, advertiser_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ignite_total_30day_imps estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_social_engagement
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) as
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ignite_gi_social_engagement';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ignite_gi_social_engagement truncate partition P_' ||
                        p_process_date;
      /*insert \*+ append*\
      into ignite_gi_social_engagement
        (date_id, campaign_id, postgroup_id, affiliate_id,
         type, author, posts_id, type_author_id,
         num_followers)
        select \*+ parallel(a,4) *\
         p_process_date, campaign_id, postgroup_id,
         affiliate_id, type, author, posts_id,
         type_author_id, sum(num_followers)
        from   (select \*+ parallel(a,4) *\
                  campaign_id, postgroup_id, affiliate_id,
                  type, author, posts_id, type_author_id,
                  num_followers
                 from   ignite_gi_social_engagement a
                 where  date_id = l_previous_dt
                 union all
                 select \*+ parallel(b,4) *\
                  campaign_id, postgroup_id, affiliate_id,
                  type, author, posts_id, type_author_id,
                  num_followers
                 from   ignite_stats.gi_social_engagement@adq b
                 where  date_id = p_process_date)
        group  by campaign_id, postgroup_id, affiliate_id,
                  type, author, posts_id, type_author_id;*/
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure replicate_to_nap
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    ---- ignite_total_impressions@nap
    l_process_name := p_process_name || '-' ||
                      'ignite_total_impressions@etlnap';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      ap_data.pkg_generic_ddl.truncate_table@etlnap('ignite_data', 'ignite_total_impressions');
      insert /*+ append */
      into ignite_data.ignite_total_impressions@etlnap
        (date_id, ignite_campaign_id, start_date, end_date,
         ad_id, affiliate_id, advertiser_id,
         total_impressions, total_clicks, posts_id,
         posts_group_id, imps_type)
        select date_id, ignite_campaign_id, start_date,
               end_date, ad_id, affiliate_id, advertiser_id,
               total_impressions, total_clicks, posts_id,
               posts_group_id, imps_type
        from   ignite_total_impressions;
      l_cnt := sql%rowcount;
      commit;
      ap_data.pkg_generic_ddl.analyze_table@etlnap('ignite_data', 'ignite_total_impressions');
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ---- ignite_total_metrics@nap
    l_process_name := p_process_name || '-' ||
                      'ignite_total_metrics@etlnap';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      ap_data.pkg_generic_ddl.truncate_table@etlnap('ignite_data', 'ignite_total_metrics');
      insert /*+ append */
      into ignite_data.ignite_total_metrics@etlnap
        (date_id, advertiser_id, ignite_campaign_id,
         start_date, end_date, posts_id, posts_group_id,
         author_id, url, type, val, ct_reach, imps_type)
        select date_id, advertiser_id, ignite_campaign_id,
               start_date, end_date, posts_id,
               posts_group_id, author_id, url, type, val,
               ct_reach, imps_type
        from   ignite_total_metrics;
      l_cnt := sql%rowcount;
      commit;
      ap_data.pkg_generic_ddl.analyze_table@etlnap('ignite_data', 'ignite_total_metrics');
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --
    ---- ignite_total_30day_imps@nap
    l_process_name := p_process_name || '-' ||
                      'ignite_total_30day_imps@etlnap';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      ap_data.pkg_generic_ddl.truncate_table@etlnap('ignite_data', 'ignite_total_30day_imps');
      insert /*+ append */
      into ignite_data.ignite_total_30day_imps@etlnap
        (date_id, advertiser_id, ignite_campaign_id,
         affiliate_id, posts_group_id, total_impressions,
         total_clicks, imps_type, process_date)
        select date_id, advertiser_id, ignite_campaign_id,
               affiliate_id, posts_group_id,
               total_impressions, total_clicks, imps_type,
               process_date
        from   ignite_total_30day_imps;
      l_cnt := sql%rowcount;
      commit;
      ap_data.pkg_generic_ddl.analyze_table@etlnap('ignite_data', 'ignite_total_30day_imps');
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ---- ignite_total_30day_metrics@nap
    l_process_name := p_process_name || '-' ||
                      'ignite_total_30day_metrics@etlnap';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      ap_data.pkg_generic_ddl.truncate_table@etlnap('ignite_data', 'ignite_total_30day_metrics');
      insert /*+ append */
      into ignite_data.ignite_total_30day_metrics@etlnap
        (date_id, ignite_campaign_id, affiliate_id,
         posts_group_id, post_id, engage, reach, imps_type,
         process_date, advertiser_id)
        select date_id, ignite_campaign_id, affiliate_id,
               posts_group_id, post_id, engage, reach,
               imps_type, process_date, advertiser_id
        from   ignite_total_30day_metrics;
      l_cnt := sql%rowcount;
      commit;
      ap_data.pkg_generic_ddl.analyze_table@etlnap('ignite_data', 'ignite_total_30day_metrics');
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

end;
/
