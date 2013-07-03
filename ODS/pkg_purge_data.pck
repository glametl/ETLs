create or replace package pkg_purge_data as

  procedure run_purge
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure purge_data
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_purge_data as

  procedure run_purge
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
    l_start_process := l_process_name || '-run_purge';
    l_mail_msg      := 'Purge Data';
    l_to            := 'nileshm@glam.com,sahilt@glam.com';
    if p_process_date is null then
      l_process_date := to_number(to_char(sysdate - 1, 'yyyymmdd'));
    elsif p_process_date is not null then
      l_process_date := p_process_date;
    end if;
    select max(run_id)
    into   l_run_id
    from   daily_process_log
    where  substr(process_date, 1, 6) =
           substr(l_process_date, 1, 6)
    and    upper(process_name) = upper(l_start_process);
    if l_run_id is null then
      l_run_id := seq_daily_campaign_run.nextval;
      -- log starting of the process
      pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'I');
      pa_send_email1.compose_email(l_run_id, 'START', l_process_date, l_mail_msg, l_start_process, l_to);
    else
      -- set the main process status as Running sothat there are no multiple entries for the main process
      update daily_process_log
      set    process_date = l_process_date,
             start_time = sysdate, status = 'RUNNING',
             error_reason = '', end_time = ''
      where  run_id = l_run_id
      and    upper(process_name) = upper(l_start_process);
      commit;
    end if;
    purge_data(l_run_id, l_process_date, l_process_name);
    -- mark the process as complete in log table
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'UC');
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      -- failed steps are handled in compose mail message procedure
      pa_send_email1.compose_email(l_run_id, 'FAIL', l_process_date, l_mail_msg, l_start_process, l_to);
  end;

  procedure purge_data
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
  begin
    for i in (select a.table_name, partition_name
              from   user_tab_partitions a, data_retention b
              where  a.table_name = upper(b.table_name)
              and    replace(partition_name, 'P_', '') <=
                     to_char(to_date(p_process_date, 'yyyymmdd') -
                              no_of_days, 'yyyymmdd')
              order  by a.table_name, partition_name) loop
      l_process_name := p_process_name || '-' ||
                        i.table_name || '-' ||
                        i.partition_name;
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        execute immediate ' alter table ' || i.table_name ||
                          ' drop partition ' ||
                          i.partition_name;
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC');
      end if;
    end loop;
  end;

end;
/
