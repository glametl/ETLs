create or replace package pkg_log_process as

  function is_not_complete
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) return boolean;

  procedure log_process
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2,
    p_flag         varchar2,
    p_count        number default 0,
    p_err_msg      varchar2 default null
  );

end;
/
create or replace package body pkg_log_process as

  function is_not_complete
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) return boolean as
    l_cnt number;
  begin
    select count(1)
    into   l_cnt
    from   daily_process_log
    where  run_id = p_run_id
    and    process_date = p_process_date
    and    upper(process_name) = upper(p_process_name)
    and    upper(status) = 'COMPLETE';
    if l_cnt = 0 then
      return true;
    else
      return false;
    end if;
  end;

  procedure log_process
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2,
    p_flag         varchar2,
    p_count        number default 0,
    p_err_msg      varchar2 default null
  ) as
    pragma autonomous_transaction;
  begin
    if p_flag = 'I' then
      delete from daily_process_log
      where  process_date = p_process_date
      and    process_name = p_process_name
      and    run_id = p_run_id
      and    status = 'RUNNING';
      insert into daily_process_log
        (run_id, process_date, process_name, start_time,
         status)
      values
        (p_run_id, p_process_date, p_process_name, sysdate,
         'RUNNING');
      commit;
    elsif p_flag = 'UC' then
      update daily_process_log
      set    end_time = sysdate, status = 'COMPLETE',
             time_hrs = trunc(86400 * (sysdate - start_time) / 3600) -
                         24 * (trunc(86400 *
                                     (sysdate - start_time) /
                                     86400)),
             time_minutes = trunc(86400 *
                                   (sysdate - start_time) / 60) -
                             60 *
                             (trunc(86400 *
                                    (sysdate - start_time) / 3600)),
             time_seconds = trunc(86400 *
                                   (sysdate - start_time)) -
                             60 *
                             (trunc(86400 *
                                    (sysdate - start_time) / 60)),
             records_inserted = p_count
      where  run_id = p_run_id
      and    process_date = p_process_date
      and    process_name = p_process_name
      and    status = 'RUNNING';
      commit;
      -- this is because we don't have process_name when it is fail in exception
    elsif p_flag = 'UF' then
      update daily_process_log
      set    end_time = sysdate, status = 'FAIL',
             time_hrs = trunc(86400 * (sysdate - start_time) / 3600) -
                         24 * (trunc(86400 *
                                     (sysdate - start_time) /
                                     86400)),
             time_minutes = trunc(86400 *
                                   (sysdate - start_time) / 60) -
                             60 *
                             (trunc(86400 *
                                    (sysdate - start_time) / 3600)),
             time_seconds = trunc(86400 *
                                   (sysdate - start_time)) -
                             60 *
                             (trunc(86400 *
                                    (sysdate - start_time) / 60)),
             error_reason = p_err_msg
      where  run_id = p_run_id
      and    process_date = p_process_date
      and    status = 'RUNNING';
      commit;
    end if;
  end;

end;
/
