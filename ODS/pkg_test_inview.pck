create or replace package pkg_test_inview as

  procedure run_test_inview
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_inview_ad_cumulative
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_inview_ad_to_date
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_test_inview as

  procedure run_test_inview
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
    l_start_process := l_process_name || '-run_inview_test';
    l_mail_msg      := 'Insider Inview Test Agg';
    -- l_to            := 'nileshm@glam.com,sahilt@glam.com';
    if p_process_date is null then
      l_process_date := to_number(to_char(sysdate - 1, 'yyyymmdd'));
    elsif p_process_date is not null then
      l_process_date := p_process_date;
    end if;
    -----------------
    select max(run_id)
    into   l_run_id
    from   daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) = upper(l_start_process);
    if l_run_id is null then
      l_run_id := seq_daily_campaign_run.nextval;
    end if;
    ------------------
    delete from daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) = upper(l_start_process)
    and    run_id = l_run_id
    and    status in ('RUNNING', 'FAIL');
    commit;
    --------
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'I');
    --pa_send_email1.compose_email(l_run_id, 'START', l_process_date, l_mail_msg, l_start_process, l_to);
    if upper(p_process_name) = 'MANUAL' then
      for i in (select date_id
                from   dim_date
                where  date_id >= l_process_date
                and    date_id <=
                       to_number(to_char(sysdate - 1, 'yyyymmdd'))
                order  by date_id) loop
        load_inview_ad_cumulative(l_run_id, p_process_date, p_process_name);
      end loop;
      load_inview_ad_to_date(l_run_id, p_process_date, p_process_name);
    else
      load_inview_ad_cumulative(l_run_id, p_process_date, p_process_name);
      load_inview_ad_to_date(l_run_id, p_process_date, p_process_name);
    end if;
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'UC');
    --  pa_send_email1.compose_email(l_run_id, 'COMPLETE', l_process_date, l_mail_msg, l_start_process, l_to);
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      --pa_send_email1.compose_email(l_run_id, 'FAIL', l_process_date, l_mail_msg, l_start_process, l_to);
  end;

  procedure load_inview_ad_cumulative
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
                      'inview_ad_cumulative';
    l_previous_dt  := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd'));
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table inview_ad_cumulative truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into inview_ad_cumulative
        (date_id, max_date_id, sel_ad_id,
         total_grossimpressions, total_measuredimpressions,
         total_initialviews)
        select /*+ parallel(c,4,1) */
         p_process_date, max(max_date_id), sel_ad_id,
         sum(total_grossimpressions) as total_grossimpressions,
         sum(total_measuredimpressions) as total_measuredimpressions,
         sum(total_initialviews) as total_initialviews
        from   (select /*+ parallel(a,4,1) */
                  max(max_date_id) as max_date_id, sel_ad_id,
                  sum(total_grossimpressions) as total_grossimpressions,
                  sum(total_measuredimpressions) as total_measuredimpressions,
                  sum(total_initialviews) as total_initialviews
                 from   inview_ad_cumulative a
                 where  date_id = l_previous_dt
                 group  by sel_ad_id
                 union all
                 select /*+ parallel(b,4,1) */
                  max(date_id) as max_date_id, sel_ad_id,
                  coalesce(sum(total_grossimpressions), 0) as total_grossimpressions,
                  coalesce(sum(total_measuredimpressions), 0) as total_measuredimpressions,
                  coalesce(sum(total_initialviews), 0) as total_initialviews
                 from   inview_daily_by_ad b
                 where  date_id = p_process_date
                 group  by sel_ad_id) a
        group  by sel_ad_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_ad_cumulative partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    --load_inview_ad_to_date(p_run_id, p_process_date, p_process_name);
  end;

  procedure load_inview_ad_to_date
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
                      'inview_ad_to_date ';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table inview_ad_to_date';
      insert /*+ append */
      into inview_ad_to_date
        (process_date, max_date_id, sel_ad_id,
         total_grossimpressions, total_measuredimpressions,
         total_initialviews)
        select /*+ parallel(a,4,1) */
         p_process_date, max_date_id, sel_ad_id,
         total_grossimpressions, total_measuredimpressions,
         total_initialviews
        from   inview_ad_cumulative a
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table inview_ad_to_date estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    l_process_name := p_process_name || '-' ||
                      'inview_ad_to_date@nap';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      ap_data.pkg_generic_ddl.truncate_table@etlnap('bi_team', 'inview_ad_to_date');
      insert /*+ append */
      into bi_team.inview_ad_to_date@etlnap
        (process_date, max_date_id, sel_ad_id,
         total_grossimpressions, total_measuredimpressions,
         total_initialviews)
        select process_date, max_date_id, sel_ad_id,
               total_grossimpressions,
               total_measuredimpressions, total_initialviews
        from   inview_ad_to_date;
      l_cnt := sql%rowcount;
      commit;
      ap_data.pkg_generic_ddl.analyze_table@etlnap('bi_team', 'inview_ad_to_date');
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

end;
/
