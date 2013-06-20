create or replace package pkg_inview as

  procedure run_inview
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_inview_daily_by_ad
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_inview_ad_daily
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_inview_ad_daily_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_inview_daily_by_url
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_inview_url_daily
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_inview_url_daily_agg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_inview_url_mtd
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_inview_url_last_30_days
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
create or replace package body pkg_inview as

  procedure run_inview
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
    l_start_process := l_process_name || '-run_inview';
    l_mail_msg      := 'Insider Inview Data';
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
    load_inview_daily_by_ad(l_run_id, l_process_date, l_process_name);
    load_inview_daily_by_url(l_run_id, l_process_date, l_process_name);
    load_inview_ad_daily(l_run_id, l_process_date, l_process_name);
    load_inview_url_daily(l_run_id, l_process_date, l_process_name);
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
        load_inview_ad_daily_agg(l_run_id, i.date_id, l_process_name);
        load_inview_url_daily_agg(l_run_id, l_process_date, l_process_name);
        l_max_date := i.date_id;
      end loop;
    else
      load_inview_ad_daily_agg(l_run_id, l_process_date, l_process_name);
      load_inview_url_daily_agg(l_run_id, l_process_date, l_process_name);
    end if;
    load_inview_url_mtd(l_run_id, l_process_date, l_process_name);
    load_inview_url_last_30_days(l_run_id, l_process_date, l_process_name);
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

  procedure load_inview_daily_by_ad
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'inview_daily_by_ad';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table inview_daily_by_ad truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into inview_daily_by_ad
        (date_id, sel_order_id, sel_ad_id, sel_creative_id,
         affiliate_id, total_measuredimpressions,
         total_initialviews, total_grossimpressions)
        select date_id, sel_order_id, sel_ad_id,
               sel_creative_id, affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions
        from   analytics_data.inview_daily_by_ad@etladq
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_daily_by_ad partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_inview_ad_daily
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'inview_ad_daily';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table inview_ad_daily truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into inview_ad_daily
        (date_id, ad_size_id, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions)
        select date_id, ad_size_id, affiliate_id,
               sum(total_measuredimpressions) as total_measuredimpressions,
               sum(total_initialviews) as total_initialviews,
               sum(total_grossimpressions) as total_grossimpressions
        from   inview_daily_by_ad a, ods_metadata.adm_ads b
        where  date_id = p_process_date
        and    a.sel_ad_id = b.ad_id
        and    ad_size_id in
               ('300x250', '728x90', '160x600')
        group  by date_id, ad_size_id, affiliate_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_ad_daily partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_inview_ad_daily_agg
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
                      'inview_ad_daily_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if to_char(to_date(p_process_date, 'yyyymmdd'), 'dd') = '01' then
        l_previous_dt := '';
      else
        l_previous_dt := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd'));
      end if;
      execute immediate 'Alter table inview_ad_daily_agg truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into inview_ad_daily_agg
        (date_id, ad_size_id, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions)
        select p_process_date, ad_size_id, affiliate_id,
               sum(total_measuredimpressions) as total_measuredimpressions,
               sum(total_initialviews) as total_initialviews,
               sum(total_grossimpressions) as total_grossimpressions
        from   (select /*+ parallel(a,4,1) */
                  ad_size_id, affiliate_id,
                  total_measuredimpressions,
                  total_initialviews, total_grossimpressions
                 from   inview_ad_daily_agg a
                 where  date_id = l_previous_dt
                 union all
                 select /*+ parallel(b,4,1) */
                  ad_size_id, affiliate_id,
                  total_measuredimpressions,
                  total_initialviews, total_grossimpressions
                 from   inview_ad_daily
                 where  date_id = p_process_date)
        group  by ad_size_id, affiliate_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_ad_daily_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_inview_daily_by_url
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'inview_daily_by_url';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table inview_daily_by_url truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into inview_daily_by_url
        (date_id, url, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions, rank_no)
        select date_id, url, affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions, 1
        from   analytics_data.inview_daily_by_url@etladq
        where  date_id = p_process_date
        and    total_grossimpressions > 0;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_daily_by_url partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_inview_url_daily
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    /*l_process_name := p_process_name || '-' ||
                      'inview_url_90_percent_daily';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table inview_url_90_percent';
      insert \*+ append *\
      into inview_url_90_percent
        (date_id, affiliate_id, grand_total,
         gross_90_percent, table_type)
        select p_process_date, affiliate_id,
               sum(total_grossimpressions) as grand_total,
               0.9 * sum(total_grossimpressions) as gross_90_percent,
               'daily' as table_type
        from   inview_daily_by_url
        where  date_id = p_process_date
        group  by affiliate_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_url_90_percent estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;*/
    l_process_name := p_process_name || '-' ||
                      'inview_url_daily';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table inview_url_daily truncate partition P_' ||
                        p_process_date;
      /*insert \*+ append *\
      into inview_url_daily
        (date_id, url, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions, rank_no)
        select date_id, url, affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions, rk
        from   (select date_id, affiliate_id, url,
                        sum(total_grossimpressions) total_grossimpressions,
                        sum(total_measuredimpressions) total_measuredimpressions,
                        sum(total_initialviews) total_initialviews,
                        rank() over(partition by affiliate_id order by sum(total_grossimpressions) desc) rk
                 from   inview_daily_by_url
                 where  date_id = p_process_date
                 and    total_grossimpressions > 0
                 group  by date_id, affiliate_id, url)
        where  rk <= 100
        order  by affiliate_id, rk;*/
      insert /*+ append */
      into inview_url_daily
        (date_id, url, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions, running_total, rank_no,
         gross_90_percent, grand_total)
        with url_90_percent as
         (select affiliate_id,
                 sum(total_grossimpressions) as grand_total,
                 0.9 * sum(total_grossimpressions) as gross_90_percent
          from   inview_daily_by_url
          where  date_id = p_process_date
          group  by affiliate_id),
        src as
         (select date_id, affiliate_id, url,
                 total_grossimpressions,
                 total_measuredimpressions,
                 total_initialviews,
                 sum(total_grossimpressions) over(partition by affiliate_id order by total_grossimpressions desc rows between unbounded preceding and current row) as running_total,
                 sum(rank_no) over(partition by affiliate_id order by total_grossimpressions desc rows between unbounded preceding and current row) as rank_no
          from   inview_daily_by_url
          where  date_id = p_process_date)
        select date_id, url, a.affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions, running_total,
               rank_no, gross_90_percent, grand_total
        from   src a, url_90_percent b
        where  a.affiliate_id = b.affiliate_id
        and    rank_no < 250;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_url_daily partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_inview_url_daily_agg
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
                      'inview_url_daily_agg';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      if to_char(to_date(p_process_date, 'yyyymmdd'), 'dd') = '01' then
        l_previous_dt := '';
      else
        l_previous_dt := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd'));
      end if;
      execute immediate 'Alter table inview_url_daily_agg truncate partition P_' ||
                        p_process_date;
      /*insert \*+ append *\
      into inview_url_daily_agg
        (date_id, url, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions, rank_no)
        select p_process_date, url, affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions, rk
        from   (select url, affiliate_id,
                        sum(total_measuredimpressions) as total_measuredimpressions,
                        sum(total_initialviews) as total_initialviews,
                        sum(total_grossimpressions) as total_grossimpressions,
                        rank() over(partition by affiliate_id order by sum(total_grossimpressions) desc) rk
                 from   (select \*+ parallel(a,4,1) *\
                           url, affiliate_id,
                           total_measuredimpressions,
                           total_initialviews,
                           total_grossimpressions
                          from   inview_url_daily_agg a
                          where  date_id = l_previous_dt
                          union all
                          select \*+ parallel(b,4,1) *\
                           url, affiliate_id,
                           total_measuredimpressions,
                           total_initialviews,
                           total_grossimpressions
                          from   inview_url_daily b
                          where  date_id = p_process_date)
                 group  by url, affiliate_id)
        where  rk <= 100
        order  by affiliate_id, rk;*/
      insert /*+ append */
      into inview_url_daily_agg
        (date_id, url, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions, rank_no)
        select p_process_date, url, affiliate_id,
               sum(total_measuredimpressions) as total_measuredimpressions,
               sum(total_initialviews) as total_initialviews,
               sum(total_grossimpressions) as total_grossimpressions,
               1
        from   (select /*+ parallel(a,4,1) */
                  url, affiliate_id, total_measuredimpressions,
                  total_initialviews, total_grossimpressions
                 from   inview_url_daily_agg a
                 where  date_id = l_previous_dt
                 union all
                 select /*+ parallel(b,4,1) */
                  url, affiliate_id, total_measuredimpressions,
                  total_initialviews, total_grossimpressions
                 from   inview_url_daily b
                 where  date_id = p_process_date
                 and    rank_no <= 150)
        group  by url, affiliate_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_url_daily_agg partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    /*l_process_name := p_process_name || '-' ||
                      'inview_url_90_percent_mtd';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      --no truncate because we just need to append the records into this table
      insert \*+ append *\
      into inview_url_90_percent
        (date_id, affiliate_id, grand_total,
         gross_90_percent, table_type)
        select p_process_date, affiliate_id,
               sum(total_grossimpressions) as grand_total,
               0.9 * sum(total_grossimpressions) as gross_90_percent,
               'mtd' as table_type
        from   inview_url_daily_agg
        where  date_id = p_process_date
        group  by affiliate_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_url_90_percent estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;*/
  end;

  procedure load_inview_url_mtd
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'inview_url_mtd';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table inview_url_mtd';
      insert /*+ append */
      into inview_url_mtd
        (date_id, url, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions, running_total, rank_no,
         gross_90_percent, grand_total)
        with url_90_percent as
         (select affiliate_id,
                 sum(total_grossimpressions) as grand_total,
                 0.9 * sum(total_grossimpressions) as gross_90_percent
          from   inview_url_daily_agg
          where  date_id = p_process_date
          group  by affiliate_id),
        src as
         (select date_id, affiliate_id, url,
                 total_grossimpressions,
                 total_measuredimpressions,
                 total_initialviews,
                 sum(total_grossimpressions) over(partition by affiliate_id order by total_grossimpressions desc rows between unbounded preceding and current row) as running_total,
                 sum(rank_no) over(partition by affiliate_id order by total_grossimpressions desc rows between unbounded preceding and current row) as rank_no
          from   inview_url_daily_agg
          where  date_id = p_process_date)
        select date_id, url, a.affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions, running_total,
               rank_no, gross_90_percent, grand_total
        from   src a, url_90_percent b
        where  a.affiliate_id = b.affiliate_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_url_mtd estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_inview_url_last_30_days
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_30_day_date  number;
  begin
    l_process_name := p_process_name || '-' ||
                      'inview_url_last_30_days';
    l_30_day_date  := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 30, 'yyyymmdd'));
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      /*execute immediate 'truncate table inview_url_last_30_days ';
      insert \*+ append *\
      into inview_url_last_30_days
        (date_id, url, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions, rank_no)
        select p_process_date, url, affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions, rk
        from   (select affiliate_id, url,
                        sum(total_grossimpressions) total_grossimpressions,
                        sum(total_measuredimpressions) total_measuredimpressions,
                        sum(total_initialviews) total_initialviews,
                        rank() over(partition by affiliate_id order by sum(total_grossimpressions) desc) rk
                 from   inview_url_daily
                 where  date_id >= l_30_day_date
                 and    date_id <= p_process_date
                 group  by affiliate_id, url)
        where  rk <= 100
        order  by affiliate_id, rk;*/
      execute immediate 'truncate table inview_url_last_30_days ';
      insert /*+ append */
      into inview_url_last_30_days
        (date_id, url, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions, running_total, rank_no,
         gross_90_percent, grand_total)
        with url_30_day_agg as
         (select affiliate_id, url,
                 sum(total_grossimpressions) total_grossimpressions,
                 sum(total_measuredimpressions) total_measuredimpressions,
                 sum(total_initialviews) total_initialviews,
                 1 as rank_no
          from   inview_url_daily
          where  date_id >= l_30_day_date
          and    date_id <= p_process_date
          and    rank_no <= 150
          group  by affiliate_id, url),
        url_90_percent as
         (select affiliate_id,
                 sum(total_grossimpressions) as grand_total,
                 0.9 * sum(total_grossimpressions) as gross_90_percent
          from   url_30_day_agg
          group  by affiliate_id),
        src as
         (select affiliate_id, url, total_grossimpressions,
                 total_measuredimpressions,
                 total_initialviews,
                 sum(total_grossimpressions) over(partition by affiliate_id order by total_grossimpressions desc rows between unbounded preceding and current row) as running_total,
                 sum(rank_no) over(partition by affiliate_id order by total_grossimpressions desc rows between unbounded preceding and current row) as rank_no
          from   url_30_day_agg)
        select p_process_date, url, a.affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions, running_total,
               rank_no, gross_90_percent, grand_total
        from   src a, url_90_percent b
        where  a.affiliate_id = b.affiliate_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_url_last_30_days estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
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
  begin
    -- inview_ad_daily
    l_process_name := p_process_name || '-' ||
                      'inview_ad_daily@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      insider_data.pkg_generic_ddl.truncate_partition@etlwww('insider_data', 'inview_ad_daily', 'P_' ||
                                                              p_process_date);
      insert /*+ append */
      into insider_data.inview_ad_daily@etlwww
        (date_id, ad_size_id, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions)
        select date_id, ad_size_id, affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions
        from   inview_ad_daily
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_partition@etlwww('insider_data', 'inview_ad_daily', 'P_' ||
                                                             p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- inview_url_daily
    l_process_name := p_process_name || '-' ||
                      'inview_url_daily@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      insider_data.pkg_generic_ddl.truncate_partition@etlwww('insider_data', 'inview_url_daily', 'P_' ||
                                                              p_process_date);
      insert /*+ append */
      into insider_data.inview_url_daily@etlwww
        (date_id, url, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions, rank_no)
        select date_id, url, affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions, rank_no
        from   inview_url_daily
        where  date_id = p_process_date
        and    rank_no <= 100;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_partition@etlwww('insider_data', 'inview_url_daily', 'P_' ||
                                                             p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- inview_ad_mtd
    l_process_name := p_process_name || '-' ||
                      'inview_ad_mtd@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      insider_data.pkg_generic_ddl.truncate_table@etlwww('insider_data', 'inview_ad_mtd');
      insert /*+ append */
      into insider_data.inview_ad_mtd@etlwww
        (date_id, ad_size_id, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions)
        select date_id, ad_size_id, affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions
        from   inview_ad_daily_agg
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_table@etlwww('insider_data', 'inview_ad_mtd');
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- inview_ad_last_30_days
    l_process_name := p_process_name || '-' ||
                      'inview_ad_last_30_days@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      insider_data.pkg_generic_ddl.truncate_table@etlwww('insider_data', 'inview_ad_last_30_days');
      insert /*+ append */
      into insider_data.inview_ad_last_30_days@etlwww
        (date_id, ad_size_id, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions)
        select p_process_date, ad_size_id, affiliate_id,
               sum(total_measuredimpressions) as total_measuredimpressions,
               sum(total_initialviews) as total_initialviews,
               sum(total_grossimpressions) as total_grossimpressions
        from   inview_ad_daily
        where  date_id >=
               to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 30, 'yyyymmdd'))
        and    date_id <= p_process_date
        group  by ad_size_id, affiliate_id;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_table@etlwww('insider_data', 'inview_ad_last_30_days');
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- inview_url_mtd
    l_process_name := p_process_name || '-' ||
                      'inview_url_mtd@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      insider_data.pkg_generic_ddl.truncate_table@etlwww('insider_data', 'inview_url_mtd');
      insert /*+ append */
      into insider_data.inview_url_mtd@etlwww
        (date_id, url, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions, rank_no)
        select date_id, url, affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions, rank_no
        from   inview_url_mtd
        where  date_id = p_process_date
        and    rank_no <= 100;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_table@etlwww('insider_data', 'inview_url_mtd');
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- inview_url_last_30_days
    l_process_name := p_process_name || '-' ||
                      'inview_url_last_30_days@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      insider_data.pkg_generic_ddl.truncate_table@etlwww('insider_data', 'inview_url_last_30_days');
      insert /*+ append */
      into insider_data.inview_url_last_30_days@etlwww
        (date_id, url, affiliate_id,
         total_measuredimpressions, total_initialviews,
         total_grossimpressions, rank_no)
        select date_id, url, affiliate_id,
               total_measuredimpressions, total_initialviews,
               total_grossimpressions, rank_no
        from   inview_url_last_30_days
        where  date_id = p_process_date
        and    rank_no <= 100;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_table@etlwww('insider_data', 'inview_url_last_30_days');
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure run_manual_inview
  (
    p_process_name varchar2,
    p_process_date number
  ) is
    l_run_id       number := 9;
    l_process_date number;
    l_process_name varchar2(10);
    l_error_msg    varchar2(4000);
    l_cnt          number := 0;
  begin
    select max(run_id)
    into   l_run_id
    from   daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) like
           upper(p_process_name) || '-RUN_INVIEW%';
    if l_run_id is null then
      l_run_id := seq_daily_campaign_run.nextval;
    end if;
    -- make sure we delete the record for the main process name(this has dependency in shell script).
    delete from daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) like
           upper(p_process_name) || '-RUN_INVIEW%'
    and    run_id = l_run_id
    and    status in ('RUNNING', 'FAIL');
    commit;
    l_process_name := initcap(p_process_name);
    -- send intimation mail for starting of process
    --compose_sendemailmessage(l_run_id, 'START', l_process_date, 'Insider Inview Data', l_process_name);
    -- log starting of the process
    pkg_log_process.log_process(l_run_id, l_process_date, l_process_name ||
                                 '-run_inview', 'I');
    if upper(l_process_name) = 'MANUAL_AD' then
      load_inview_daily_by_ad(l_run_id, l_process_date, l_process_name);
      load_inview_ad_daily(l_run_id, l_process_date, l_process_name);
      if p_process_date <>
         to_number(to_char(sysdate - 1, 'yyyymmdd')) then
        for i in (select date_id
                  from   dim_date
                  where  date_id >= l_process_date
                  and    date_id <=
                         to_number(to_char(sysdate - 1, 'yyyymmdd'))
                  order  by date_id) loop
          load_inview_ad_daily_agg(l_run_id, i.date_id, l_process_name);
        end loop;
      end if;
      -- inview_ad_daily
      l_process_name := p_process_name || '-' ||
                        'inview_ad_daily@etlwww';
      if pkg_log_process.is_not_complete(l_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(l_run_id, p_process_date, l_process_name, 'I');
        insider_data.pkg_generic_ddl.truncate_partition@etlwww('insider_data', 'inview_ad_daily', 'P_' ||
                                                                p_process_date);
        insert /*+ append */
        into insider_data.inview_ad_daily@etlwww
          (date_id, ad_size_id, affiliate_id,
           total_measuredimpressions, total_initialviews,
           total_grossimpressions)
          select date_id, ad_size_id, affiliate_id,
                 total_measuredimpressions,
                 total_initialviews, total_grossimpressions
          from   inview_ad_daily
          where  date_id = p_process_date;
        l_cnt := sql%rowcount;
        commit;
        insider_data.pkg_generic_ddl.analyze_partition@etlwww('insider_data', 'inview_ad_daily', 'P_' ||
                                                               p_process_date);
        pkg_log_process.log_process(l_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
    else
      load_inview_daily_by_url(l_run_id, l_process_date, l_process_name);
      load_inview_url_daily(l_run_id, l_process_date, l_process_name);
      if p_process_date <>
         to_number(to_char(sysdate - 1, 'yyyymmdd')) then
        for i in (select date_id
                  from   dim_date
                  where  date_id >= l_process_date
                  and    date_id <=
                         to_number(to_char(sysdate - 1, 'yyyymmdd'))
                  order  by date_id) loop
          load_inview_url_daily_agg(l_run_id, l_process_date, l_process_name);
        end loop;
      end if;
      -- inview_url_daily
      l_process_name := p_process_name || '-' ||
                        'inview_url_daily@etlwww';
      if pkg_log_process.is_not_complete(l_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(l_run_id, p_process_date, l_process_name, 'I');
        insider_data.pkg_generic_ddl.truncate_partition@etlwww('insider_data', 'inview_url_daily', 'P_' ||
                                                                p_process_date);
        insert /*+ append */
        into insider_data.inview_url_daily@etlwww
          (date_id, url, affiliate_id,
           total_measuredimpressions, total_initialviews,
           total_grossimpressions, rank_no)
          select date_id, url, affiliate_id,
                 total_measuredimpressions,
                 total_initialviews, total_grossimpressions,
                 rank_no
          from   inview_url_daily
          where  date_id = p_process_date;
        l_cnt := sql%rowcount;
        commit;
        insider_data.pkg_generic_ddl.analyze_partition@etlwww('insider_data', 'inview_url_daily', 'P_' ||
                                                               p_process_date);
        pkg_log_process.log_process(l_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
    end if;
    -- mark the process as complete in log table
    pkg_log_process.log_process(l_run_id, l_process_date, l_process_name ||
                                 '-run_inview', 'UC');
    -- send completion mail
    --compose_sendemailmessage(l_run_id, 'COMPLETE', l_process_date, 'Insider Inview Data', l_process_name);
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      -- failed steps are handled in compose mail message procedure
    --compose_sendemailmessage(l_run_id, 'FAIL', l_process_date, 'Insider Inview Data', l_process_name);
  end;

end;
/
